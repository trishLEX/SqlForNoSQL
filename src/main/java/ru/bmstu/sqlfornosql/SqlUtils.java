package ru.bmstu.sqlfornosql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.bson.Document;
import ru.bmstu.sqlfornosql.adapters.mongo.DateFunction;
import ru.bmstu.sqlfornosql.adapters.mongo.ObjectIdFunction;
import ru.bmstu.sqlfornosql.adapters.mongo.WhereClauseParser;

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

    public static String getStringValue(Expression expression) {
        if (expression instanceof StringValue) {
            return ((StringValue)expression).getValue();
        } else if (expression instanceof Column) {
            String columnName = expression.toString();
            Matcher matcher = SURROUNDED_IN_QUOTES.matcher(columnName);
            if (matcher.matches()) {
                return matcher.group(1);
            }
            return columnName;
        }
        return expression.toString();
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

                if ("date".equalsIgnoreCase(function.getName())
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

    public static Object parseFunctionArguments(ExpressionList parameters)
    {
        if (parameters == null) {
            return null;
        } else if (parameters.getExpressions().size()==1) {
            return getStringValue(parameters.getExpressions().get(0));
        } else {
            return Lists.newArrayList(parameters.getExpressions().stream().map(expression -> {
                try {
                    return getValue(expression, null);
                } catch (IllegalStateException e) {
                    return getStringValue(expression);
                }
            }).collect(Collectors.toList()));
        }
    }

    public static Object getValue(Expression incomingExpression, Expression otherSide) {
        if (incomingExpression instanceof LongValue) {
            return normalizeValue((((LongValue)incomingExpression).getValue()));
        } else if (incomingExpression instanceof SignedExpression) {
            return normalizeValue((((SignedExpression)incomingExpression).toString()));
        } else if (incomingExpression instanceof StringValue) {
            return normalizeValue((((StringValue)incomingExpression).getValue()));
        } else if (incomingExpression instanceof Column) {
            return normalizeValue(getStringValue(incomingExpression));
        } else {
            throw new IllegalStateException("can not parseNaturalLanguageDate: " + incomingExpression.toString());
        }
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
}
