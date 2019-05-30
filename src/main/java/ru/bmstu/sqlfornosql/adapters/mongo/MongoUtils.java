package ru.bmstu.sqlfornosql.adapters.mongo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.bson.Document;
import ru.bmstu.sqlfornosql.adapters.sql.SqlUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MongoUtils {
    private MongoUtils() {
        //utility class
    }

    public static Document createProjectionsFromSelectItems(List<SelectItem> selectItems, MongoHolder mongoHolder) {
        Document document = new Document();
        if (selectItems == null || selectItems.isEmpty()) {
            return document;
        }

        List<SelectItem> functionItems = Lists.newArrayList(selectItems.stream().filter(selectItem -> {
            try {
                if (selectItem instanceof SelectExpressionItem
                        && ((SelectExpressionItem) selectItem).getExpression() instanceof Function) {
                    return true;
                }
            } catch (NullPointerException e) {
                return false;
            }
            return false;
        }).collect(Collectors.toList()));

        List<SelectItem> nonFunctionItems = Lists.newArrayList(Collections2.filter(selectItems, selectItem -> !functionItems.contains(selectItem)));

        //Preconditions.checkArgument(functionItems.size() > 0, "there must be at least one group by function specified in the select clause");
        //Preconditions.checkArgument(nonFunctionItems.size() > 0, "there must be at least one non-function column specified");

        Document idDocument = new Document();
        for (SelectItem selectItem : nonFunctionItems) {
            Column column = (Column) ((SelectExpressionItem) selectItem).getExpression();
            String columnName = SqlUtils.getStringValue(column);
            if (columnName.contains(".")) {
                columnName = columnName.substring(columnName.lastIndexOf('.') + 1);
            }
            idDocument.put(columnName,"$" + columnName);
        }

        for (String groupBy : mongoHolder.getGroupBys()) {
            if (groupBy.contains(".")) {
                groupBy = groupBy.substring(groupBy.lastIndexOf('.') + 1);
            }
            idDocument.put(groupBy, "$" + groupBy);
        }

        document.append("_id", idDocument.size() == 1 ? Iterables.get(idDocument.values(),0) : idDocument);

        for (SelectItem selectItem : functionItems) {
            Function function = (Function) ((SelectExpressionItem)selectItem).getExpression();
            parseFunctionForAggregation(function, document, mongoHolder);
        }

        return document;
    }

    private static void parseFunctionForAggregation(Function function, Document document, MongoHolder mongoHolder) {
        List<String> parameters = function.getParameters() ==  null ? Collections.emptyList() :
                function
                        .getParameters()
                        .getExpressions()
                        .stream()
                        .map(param -> SqlUtils.getStringValue(param, true))
                        .collect(Collectors.toList());

        if (parameters.size() > 1) {
            throw new IllegalStateException(function.getName()+" function can only have one parameter");
        }

        String field = !parameters.isEmpty() ? Iterables.get(parameters, 0).replaceAll("\\.","_") : null;

        if (field != null && field.contains(".")) {
            field = field.substring(field.lastIndexOf('.') + 1);
        }

        mongoHolder.setHasAggregateFunctions(true);

        switch (function.getName().toLowerCase()) {
            case "sum":
                createFunction("sum", field, document, "$" + field);
                break;
            case "avg":
                createFunction("avg", field, document, "$" + field);
                break;
            case "count":
                if (field != null){
                    document.put("count" + "(" + field + ")", new Document("$sum", 1));
                } else {
                    document.put("count", new Document("$sum", 1));
                }
                break;
            case "min":
                createFunction("min", field, document,"$" + field);
                break;
            case "max":
                createFunction("max", field, document,"$" + field);
                break;
            default:
                throw new IllegalStateException("could not understand function:" + function.getName());
        }
    }

    public static String makeMongoColName(String sqlName) {
        if (sqlName.startsWith("sum(")
                || sqlName.startsWith("avg(")
                || sqlName.startsWith("min(")
                || sqlName.startsWith("max(")) {
            return sqlName.substring(0, 3) + "_" + getNonQualifiedName(sqlName.substring(4, sqlName.length() - 1)) + "";
        } else if (sqlName.startsWith("count(")) {
            if (sqlName.contains("*")){
                return "count";
            } else {
                return sqlName.substring(0, 5) + "(" + getNonQualifiedName(sqlName.substring(6, sqlName.length() - 1)) + ")";
            }
        } else {
            return getNonQualifiedName(sqlName);
        }
    }

    private static void createFunction(String functionName, String field, Document document, Object value) {
        Preconditions.checkArgument(field != null,"function "+ functionName + " must contain a single field to run on");
        document.put(functionName + "_" + field + "", new Document("$" + functionName, value));
    }

    public static Document createSortInfoFromOrderByElements(List<OrderByElement> orderByElements, MongoHolder mongoHolder) {
        Document document = new Document();
        if (orderByElements == null || orderByElements.isEmpty()) {
            return document;
        }

        List<OrderByElement> functionItems = Lists.newArrayList(orderByElements.stream().filter(orderByElement -> {
            try {
                if (orderByElement.getExpression() instanceof Function) {
                    return true;
                }
            } catch (NullPointerException e) {
                return false;
            }
            return false;
        }).collect(Collectors.toList()));

        final List<OrderByElement> nonFunctionItems = Lists.newArrayList(
                Collections2.filter(orderByElements, orderByElement -> !functionItems.contains(orderByElement))
        );

        Document sortItems = new Document();
        for (OrderByElement orderByElement : orderByElements) {
            if (nonFunctionItems.contains(orderByElement)) {
                sortItems.put(getNonQualifiedName(SqlUtils.getStringValue(orderByElement.getExpression())), orderByElement.isAsc() ? 1 : -1);
            } else {
                Function function = (Function) orderByElement.getExpression();
                Document parseFunctionDocument = new Document();
                boolean hasAggregationFn = mongoHolder.hasAggregateFunctions();
                parseFunctionForAggregation(function, parseFunctionDocument, mongoHolder);
                //возвращаем как было
                if (!hasAggregationFn) {
                    mongoHolder.setHasAggregateFunctions(false);
                }
                sortItems.put(getNonQualifiedName(Iterables.get(parseFunctionDocument.keySet(),0)), orderByElement.isAsc() ? 1 : -1);
            }
        }

        return sortItems;
    }

    public static String getNonQualifiedName(String qualifiedName) {
        if (qualifiedName.contains(".")) {
            return qualifiedName.substring(qualifiedName.lastIndexOf('.') + 1);
        } else {
            return qualifiedName;
        }
    }

    public static String normalizeColumnName(String mongoColumnName) {
        return mongoColumnName.charAt(0) == '$' ? mongoColumnName.substring(1) : mongoColumnName;
    }
}
