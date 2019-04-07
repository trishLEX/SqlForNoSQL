package ru.bmstu.sqlfornosql.adapters.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.bson.Document;
import ru.bmstu.sqlfornosql.adapters.mongo.DateFunction;
import ru.bmstu.sqlfornosql.adapters.mongo.ObjectIdFunction;
import ru.bmstu.sqlfornosql.adapters.mongo.WhereClauseParser;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectField;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectFieldExpression;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SqlFunction;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SqlUtils {
    private static final Pattern SURROUNDED_IN_QUOTES = Pattern.compile("^\"(.+)*\"$");
    private static final Pattern LIKE_RANGE_REGEX = Pattern.compile("(\\[.+?\\])");

    private SqlUtils() {
        //utility class
    }

    public static boolean isCountAll(List<SelectItem> selectItems) {
        if (selectItems != null && selectItems.size() == 1) {
            SelectItem firstItem = selectItems.get(0);
            if (firstItem instanceof SelectExpressionItem
                    && ((SelectExpressionItem) firstItem).getExpression() instanceof Function) {
                Function function = (Function) ((SelectExpressionItem) firstItem).getExpression();

                return "count(*)".equals(function.toString());
            }
        }

        return false;
    }

    public static boolean isSelectAll(List<SelectItem> selectItems) {
        if (selectItems != null && selectItems.size() == 1) {
            SelectItem firstItem = selectItems.get(0);
            return firstItem instanceof AllColumns;
        } else {
            return false;
        }
    }

    public static long getLimit(Limit limit) {
        if (limit != null) {
            String rowCountString = getStringValue(limit.getRowCount());
            BigInteger bigInt = new BigInteger(rowCountString);
            Preconditions.checkArgument(bigInt.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) <= 0, rowCountString + ": value is too large");
            return bigInt.longValue();
        }

        return -1;
    }

    public static long getOffset(Offset offset) {
        if (offset != null) {
            Preconditions.checkArgument(offset.getOffset() <= Integer.MAX_VALUE, offset.getOffset() + ": value is too large");
            return offset.getOffset();
        }

        return -1;
    }

    public static String getStringValue(Expression expression, boolean getNonQualifiedName) {
        if (expression instanceof StringValue) {
            return ((StringValue)expression).getValue();
        } else if (expression instanceof Column) {
            String columnName = expression.toString();

            if (getNonQualifiedName && columnName.contains(".")) {
                columnName = columnName.substring(columnName.lastIndexOf('.') + 1);
            }

            Matcher matcher = SURROUNDED_IN_QUOTES.matcher(columnName);
            if (matcher.matches()) {
                return matcher.group(1);
            }
            return columnName;
        }
        return expression.toString();
    }

    public static String getStringValue(Expression expression) {
        return getStringValue(expression, false);
    }

    public static SelectField getSelectFieldFromString(String field) {
        if (field.equals("*")) {
            return ru.bmstu.sqlfornosql.adapters.sql.selectfield.AllColumns.ALL_COLUMNS;
        } else if (field.startsWith("sum(")) {
            return new SelectFieldExpression(
                    SqlFunction.SUM,
                    field.substring(4, field.length() - 1)
            );
        } else if (field.startsWith("avg(")) {
            return new SelectFieldExpression(
                    SqlFunction.AVG,
                    field.substring(4, field.length() - 1)
            );
        } else if (field.startsWith("min(")) {
            return new SelectFieldExpression(
                    SqlFunction.MIN,
                    field.substring(4, field.length() - 1)
            );
        } else if (field.startsWith("max(")) {
            return new SelectFieldExpression(
                    SqlFunction.MAX,
                    field.substring(4, field.length() - 1)
            );
        } else if (field.startsWith("count(")) {
            return new SelectFieldExpression(
                    SqlFunction.COUNT,
                    field.substring(6, field.length() - 1)
            );
        } else {
            return new ru.bmstu.sqlfornosql.adapters.sql.selectfield.Column(field);
        }
    }

    public static List<String> getGroupBy(List<Expression> groupByColumnReferences) {
        if (groupByColumnReferences == null || groupByColumnReferences.isEmpty()) {
            return Collections.emptyList();
        }

        return groupByColumnReferences.stream().map(SqlUtils::getStringValue).collect(Collectors.toList());
    }

    public static DateFunction isDateFunction(Expression incomingExpression) {
        if (incomingExpression instanceof ComparisonOperator) {
            ComparisonOperator comparisonOperator = (ComparisonOperator) incomingExpression;
            String rightExpression = getStringValue(comparisonOperator.getRightExpression());

            if (comparisonOperator.getLeftExpression() instanceof Function) {
                Function function = ((Function)comparisonOperator.getLeftExpression());

                if ("to_date".equalsIgnoreCase(function.getName())
                        && (function.getParameters().getExpressions().size() == 2)
                        && function.getParameters().getExpressions().get(1) instanceof StringValue) {

                    String column = getStringValue(function.getParameters().getExpressions().get(0));
                    DateFunction dateFunction = new DateFunction(
                            ((StringValue)(function.getParameters().getExpressions().get(1))).getValue(),
                            rightExpression,
                            column
                    );

                    dateFunction.setComparisonFunction(comparisonOperator);

                    return dateFunction;
                }

            }
        }

        return null;
    }

    public static ObjectIdFunction isObjectIdFunction(Expression incomingExpression) {
        if (incomingExpression instanceof ComparisonOperator) {
            ComparisonOperator comparisonOperator = (ComparisonOperator)incomingExpression;
            String rightExpression = getStringValue(comparisonOperator.getRightExpression());

            if (comparisonOperator.getLeftExpression() instanceof Function) {
                Function function = ((Function) comparisonOperator.getLeftExpression());
                if ("objectid".equalsIgnoreCase(function.getName())
                        && (function.getParameters().getExpressions().size() == 1)
                        && function.getParameters().getExpressions().get(0) instanceof StringValue) {
                    String column = getStringValue(function.getParameters().getExpressions().get(0));
                    return new ObjectIdFunction(column, rightExpression, comparisonOperator);
                }
            }
        } else if (incomingExpression instanceof InExpression) {
            InExpression inExpression = (InExpression)incomingExpression;
            Expression leftExpression = ((InExpression) incomingExpression).getLeftExpression();

            if (inExpression.getLeftExpression() instanceof Function) {
                Function function = ((Function) inExpression.getLeftExpression());
                if ("objectid".equalsIgnoreCase(function.getName())
                        && (function.getParameters().getExpressions().size() == 1)
                        && function.getParameters().getExpressions().get(0) instanceof StringValue) {
                    String column = getStringValue(function.getParameters().getExpressions().get(0));
                    List<Object> rightExpression = ((ExpressionList) inExpression.getRightItemsList()).getExpressions()
                            .stream()
                            .map(expression -> WhereClauseParser.parseExpression(new Document(), expression, leftExpression))
                            .collect(Collectors.toList());

                    return new ObjectIdFunction(column, rightExpression, inExpression);
                }
            }
        }
        return null;
    }

    public static String replaceRegexCharacters(String value) {
        String newValue = value.replaceAll("%",".*")
                .replaceAll("_",".{1}");

        Matcher m = LIKE_RANGE_REGEX.matcher(newValue);
        StringBuffer sb = new StringBuffer();
        while(m.find())  {
            m.appendReplacement(sb, m.group(1) + "{1}");
        }
        m.appendTail(sb);

        return sb.toString();
    }

    public static Object parseFunctionArguments(ExpressionList parameters, boolean getNonQualifiedName) {
        if (parameters == null) {
            return null;
        } else if (parameters.getExpressions().size() == 1) {
            return getStringValue(parameters.getExpressions().get(0), getNonQualifiedName);
        } else {
            return Lists.newArrayList(parameters.getExpressions().stream().map(expression -> {
                try {
                    return getValue(expression, null);
                } catch (IllegalStateException e) {
                    return getStringValue(expression, getNonQualifiedName);
                }
            }).collect(Collectors.toList()));
        }
    }

    public static Object parseFunctionArguments(ExpressionList parameters) {
        return parseFunctionArguments(parameters, false);
    }

    public static Object getValue(Expression incomingExpression, Expression otherSide, boolean getNonQualifiedName) {
        if (incomingExpression instanceof LongValue) {
            return normalizeValue((((LongValue)incomingExpression).getValue()));
        } else if (incomingExpression instanceof SignedExpression) {
            return normalizeValue((((SignedExpression)incomingExpression).toString()));
        } else if (incomingExpression instanceof StringValue) {
            return normalizeValue((((StringValue)incomingExpression).getValue()));
        } else if (incomingExpression instanceof Column) {
            return normalizeValue(getStringValue(incomingExpression, getNonQualifiedName));
        } else {
            throw new IllegalStateException("can not parseNaturalLanguageDate: " + incomingExpression.toString());
        }
    }

    public static Object getValue(Expression incomingExpression, Expression otherSide) {
        return getValue(incomingExpression, otherSide, false);
    }

    public static Object normalizeValue(Object value) {
        Object bool = forceBool(value);
        return (bool != null) ? bool : value;
    }

    public static Object forceBool(Object value) {
        if (value.toString().equalsIgnoreCase("true") || value.toString().equalsIgnoreCase("false")) {
            return Boolean.valueOf(value.toString());
        }
        return null;
    }

    public static SqlHolder fillSqlMeta(String sql) {
        boolean isDistinct;
        boolean isCountAll;
        FromItem fromItem;
        long limit;
        long offset;
        Expression whereClause;
        List<SelectItem> selectItems;
        List<Join> joins;
        List<String> groupBys;
        Expression havingClause;
        List<OrderByElement> orderByElements;

        try {
            Statement statement = CCJSqlParserUtil.parse(sql);

            if (Select.class.isAssignableFrom(statement.getClass())) {
                PlainSelect plainSelect = (PlainSelect)(((Select)statement).getSelectBody());
                Preconditions.checkArgument(plainSelect != null, "Can't parse statement");

                isDistinct = plainSelect.getDistinct() != null;
                isCountAll = SqlUtils.isCountAll(plainSelect.getSelectItems());
                fromItem = plainSelect.getFromItem();
                limit = SqlUtils.getLimit(plainSelect.getLimit());
                offset = SqlUtils.getOffset(plainSelect.getOffset());
                whereClause = plainSelect.getWhere();
                selectItems = plainSelect.getSelectItems();
                joins = plainSelect.getJoins();
                groupBys = SqlUtils.getGroupBy(plainSelect.getGroupByColumnReferences());
                havingClause = plainSelect.getHaving();
                orderByElements = plainSelect.getOrderByElements();

                return new SqlHolder.SqlHolderBuilder()
                        .withDistinct(isDistinct)
                        .withCountAll(isCountAll)
                        .withFromItem(fromItem)
                        .withLimit(limit)
                        .withOffset(offset)
                        .withWhere(whereClause)
                        .withSelectItems(selectItems)
                        .withJoins(joins)
                        .withGroupBy(groupBys)
                        .withHaving(havingClause)
                        .withOrderBy(orderByElements)
                        .build();
            } else {
                throw new IllegalArgumentException("Only select statements are supported");
            }
        } catch (JSQLParserException e) {
            throw new IllegalArgumentException("Can't parse statement: " + sql, e);
        }
    }

    public static String getIdentFromSelectItem(String item) {
        if (item.startsWith("sum(")
                || item.startsWith("avg(")
                || item.startsWith("min(")
                || item.startsWith("max(")) {
            return item.substring(4, item.length() - 1);
        } else if (item.startsWith("count(")) {
            if (item.contains("*")){
                return "count";
            } else {
                return item.substring(6, item.length() - 1);
            }
        } else {
            return item;
        }
    }
}
