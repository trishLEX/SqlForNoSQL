package ru.bmstu.sqlfornosql.executor;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mongodb.client.MongoDatabase;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.*;
import org.bson.BsonDocument;
import org.medfoster.sqljep.ParseException;
import org.medfoster.sqljep.RowJEP;
import ru.bmstu.sqlfornosql.adapters.mongo.MongoAdapter;
import ru.bmstu.sqlfornosql.adapters.mongo.MongoClient;
import ru.bmstu.sqlfornosql.adapters.postgres.PostgresClient;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.adapters.sql.SqlUtils;
import ru.bmstu.sqlfornosql.model.Row;
import ru.bmstu.sqlfornosql.model.Table;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ru.bmstu.sqlfornosql.executor.ExecutorUtils.*;

public class Executor {
    //TODO вводится правило, что работаем только с lowerCase'ми
    static final Pattern IDENT_REGEXP = Pattern.compile("([a-zA-Z]+[0-9a-zA-Z.]*)");

    static final Set<String> FORBIDDEN_STRINGS = ImmutableSet.of(
            "SELECT",
            "AND",
            "IN",
            "BETWEEN",
            "NOT",
            "IS",
            "DATE",
            "TIME",
            "TIMESTAMP",
            "LIKE",
            "TRUE",
            "FALSE",
            "NULL"
    );

    public Table execute(String sql) {
        SqlHolder sqlHolder = SqlUtils.fillSqlMeta(sql);
        if (sqlHolder.getJoins().isEmpty()) {
            return simpleSelect(sqlHolder);
        } else {
            return selectWithJoins(sqlHolder);
        }
    }

    /**
     * Выполняет SELECT запрос без JOIN'ов и с одной таблицей
     * @param sqlHolder - холдер, содержащий запрос
     * @return таблицу с результатом выполнения запроса
     */
    //TODO здесь fromItem может быть подзапросом - это нужно обработать
    private Table simpleSelect(SqlHolder sqlHolder) {
        if (sqlHolder.getFromItem() instanceof net.sf.jsqlparser.schema.Table) {
            switch (sqlHolder.getDatabase().getDbType()) {
                case POSTGRES:
                    try (PostgresClient client = new PostgresClient("localhost", 5432, "postgres", "0212", "postgres")) {
                        return client.executeQuery(sqlHolder);
                    }
                case MONGODB:
                    try (com.mongodb.MongoClient client = new com.mongodb.MongoClient()) {
                        MongoDatabase database = client.getDatabase(sqlHolder.getDatabase().getDatabaseName());
                        MongoAdapter adapter = new MongoAdapter();
                        MongoClient<BsonDocument> mongoClient = new MongoClient<>(
                                database.getCollection(sqlHolder.getDatabase().getTable(), BsonDocument.class)
                        );
                        return mongoClient.executeQuery(adapter.translate(sqlHolder));
                    }
                default:
                    throw new IllegalArgumentException("Unknown database type");
            }
        } else if (sqlHolder.getFromItem() instanceof SubSelect) {
            SubSelect subSelect = ((SubSelect) sqlHolder.getFromItem());
            String subSelectStr = subSelect.toString();
            Table subSelectResult;
            if (subSelect.isUseBrackets()) {
                subSelectResult = execute(subSelectStr.substring(1, subSelectStr.length() - 1));
            } else {
                subSelectResult = execute(subSelectStr);
            }

//            for (String col : sqlHolder.getSelectItemsStrings()) {
//                if (col.startsWith("avg(")) {
//                    sqlHolder.addAdditionalItemString("count" + col.substring(3));
//                }
//            }

            Table result = new Table();
            for (Row subSelectRow : subSelectResult.getRows()) {
                Row row = new Row(result);
                for (String column : sqlHolder.getSelectItemsStrings()) {
                    String ident = SqlUtils.getIdentFromSelectItem(column.toLowerCase());
                    row.add(ident, subSelectRow.getObject(ident));
                    result.setType(ident, subSelectResult.getType(ident));
                }

                if (sqlHolder.getWhereClause() != null) {
                    HashMap<String, Integer> colMapping = getIdentMapping(sqlHolder.getWhereClause().toString().toLowerCase());
                    RowJEP sqljep = prepareSqlJEP(sqlHolder.getWhereClause(), colMapping);
                    Comparable[] values = new Comparable[colMapping.size()];

                    for (Map.Entry<String, Integer> colMappingEntry : colMapping.entrySet()) {
                        values[colMappingEntry.getValue()] = getValueKeyIgnoreCase(row, colMappingEntry.getKey());
                    }

                    try {
                        Boolean expressionValue = (Boolean) sqljep.getValue(values);
                        if (expressionValue) {
                            result.add(row);
                        }
                    } catch (ParseException e) {
                        throw new IllegalStateException("Can't execute expression: " + sqlHolder.getWhereClause(), e);
                    }
                } else {
                    result.add(row);
                }
            }

            if (!sqlHolder.getGroupBys().isEmpty()) {
                result = Grouper.group(result, sqlHolder.getGroupBys(), sqlHolder.getSelectItemsStrings(), sqlHolder.getHavingClause());
            }

            if (!sqlHolder.getOrderByElements().isEmpty()) {
                LinkedHashMap<String, Boolean> orderByMap = new LinkedHashMap<>();
                for (OrderByElement element : sqlHolder.getOrderByElements()) {
                    //TODO order by можно выполянть по expression'у
                    orderByMap.put(element.toString(), element.isAsc());
                }
                result.sort(orderByMap);
            }

            return result;
        } else {
            throw new IllegalStateException("Can't determine type of FROM argument");
        }
    }

    /**
     * Выполяняет SELECT запрос из нескольких таблиц и/или с JOIN
     * @param sqlHolder - холдер, содержащий запрос
     * @return таблицу с результато выполнения запроса
     */
    private Table selectWithJoins(SqlHolder sqlHolder) {
        Map<FromItem, Table> resultParts = new LinkedHashMap<>();
        List<String> queries = new ArrayList<>();

        Set<String> additionalSelectItems = new HashSet<>();
        for (Map.Entry<FromItem, List<SelectItem>> fromItemListEntry : sqlHolder.getSelectItemMap().entrySet()) {
            SqlHolder holder = new SqlHolder.SqlHolderBuilder()
                    .withSelectItems(fromItemListEntry.getValue())
                    .withFromItem(fromItemListEntry.getKey())
                    .build();

            Set<String> selectItemsStr = Sets.newHashSet(holder.getSelectIdents());

            if (!fromItemListEntry.getKey().equals(sqlHolder.getFromItem())) {
                holder.addAllAdditionalItemStrings(getAdditionalSelectItemsStrings(fromItemListEntry.getKey(), sqlHolder.getJoins(), selectItemsStr));
            }

            if (sqlHolder.getWhereClause() != null) {
                holder.addAllAdditionalItemStrings(getIdentsFromExpression(fromItemListEntry.getKey(), sqlHolder.getWhereClause(), selectItemsStr));
            }

            selectItemsStr.addAll(holder.getAdditionalSelectItemsStrings());
            additionalSelectItems.addAll(holder.getAdditionalSelectItemsStrings());

            if (selectItemsStr.isEmpty()) {
                resultParts.put(fromItemListEntry.getKey(), new Table());
                continue;
            }

            String query = holder.toString();

            List<String> queryOrParts = new ArrayList<>();

            if (sqlHolder.getWhereClause() != null) {
                fillIdents(selectItemsStr, queryOrParts, sqlHolder.getWhereClause());

                if (!queryOrParts.isEmpty()) {
                    query += " WHERE " + String.join(" OR ", queryOrParts);
                }
            }

            if (!sqlHolder.getGroupBys().isEmpty()) {
                List<String> groupBys = new ArrayList<>();
                for (String groupByItem : sqlHolder.getGroupBys()) {
                    if (selectItemsStr.contains(groupByItem)) {
                        groupBys.add(groupByItem);
                    }
                }

                if (!groupBys.isEmpty()) {
                    query += " GROUP BY " + String.join(" ,", groupBys);
                }

                if (sqlHolder.getHavingClause() != null) {
                    List<String> havingOrParts = new ArrayList<>();
                    fillIdents(selectItemsStr, havingOrParts, sqlHolder.getHavingClause());

                    if (!havingOrParts.isEmpty()) {
                        query += " HAVING " + String.join(" OR ", havingOrParts);
                    }
                }
            }

            if (!sqlHolder.getOrderByElements().isEmpty()) {
                List<String> orderBys = new ArrayList<>();
                for (OrderByElement orderByItem : sqlHolder.getOrderByElements()) {
                    fillIdents(selectItemsStr, orderBys, orderByItem.toString());
                }

                if (!orderBys.isEmpty()) {
                    query += " ORDER BY " + String.join(" ,", orderBys);
                }
            }

            queries.add(query);

            Table result = execute(query);
            resultParts.put(fromItemListEntry.getKey(), result);
        }

        System.out.println(queries);

        Table from = resultParts.remove(sqlHolder.getFromItem());
        if (from == null) {
            throw new IllegalStateException("FromItem can not be null");
        }

        Table result = Joiner.join(from, Lists.newArrayList(resultParts.values()), sqlHolder.getJoins(), sqlHolder.getWhereClause());
        result.remove(additionalSelectItems);
        return result;
    }

    //TODO не поддерживатся USING
    private List<String> getAdditionalSelectItemsStrings(FromItem from, List<Join> joins, Set<String> existingSelectItems) {
        int i = joins.stream().map(Join::getRightItem).collect(Collectors.toList()).indexOf(from);
        if (i < 0) {
            throw new IllegalStateException("From must be found in joins");
        }

        Join src = joins.get(i);
        if (src.getOnExpression() == null) {
            return Collections.emptyList();
        }

        return getIdentsFromExpression(from, src.getOnExpression(), existingSelectItems);
    }

    private List<String> getIdentsFromExpression(FromItem fromItem, Expression expression, Set<String> existingSelectItems) {
        String stringExpr = expression.toString();
        Matcher matcher = IDENT_REGEXP.matcher(stringExpr.replaceAll("'.*'", ""));
        List<String> idents = new ArrayList<>();
        while (matcher.find()) {
            String potentialIdent = matcher.group(1);
            String suffix = potentialIdent.contains(".") ? potentialIdent.substring(0, potentialIdent.lastIndexOf('.')) : potentialIdent;
            if (!FORBIDDEN_STRINGS.contains(matcher.group(1).toUpperCase())
                    && fromItem.toString().endsWith(suffix)
                    && !existingSelectItems.contains(potentialIdent))
            {
                idents.add(potentialIdent);
            }
        }

        return idents;
    }

    private void fillIdents(Set<String> selectItemsStr, List<String> destination, String str) {
        Matcher matcher = IDENT_REGEXP.matcher(str.replaceAll("'.*'", ""));
        List<String> idents = new ArrayList<>();
        while (matcher.find()) {
            if (!FORBIDDEN_STRINGS.contains(matcher.group(1).toUpperCase())) {
                idents.add(matcher.group(1));
            }
        }

        if (selectItemsStr.containsAll(idents)) {
            destination.add(str);
        }
    }

    private void fillIdents(Set<String> selectItemsStr, List<String> whereOrParts, Expression whereClause) {
        String whereExpression = whereClause.toString();
        String[] orParts = whereExpression.split("\\sOR\\s");
        for (String part : orParts) {
            fillIdents(selectItemsStr, whereOrParts, part);
        }
    }
}
