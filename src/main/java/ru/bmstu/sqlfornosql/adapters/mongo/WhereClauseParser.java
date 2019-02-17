package ru.bmstu.sqlfornosql.adapters.mongo;

import com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import org.bson.Document;
import ru.bmstu.sqlfornosql.SqlUtils;

import java.util.*;
import java.util.stream.Collectors;

public class WhereClauseParser {
    private WhereClauseParser() {
        //utility class
    }

    public static Object parseExpression(Document query, Expression incomingExpression, Expression otherSide) {
        if (incomingExpression instanceof ComparisonOperator) {
            DateFunction dateFunction = SqlUtils.isDateFunction(incomingExpression);
            ObjectIdFunction objectIdFunction = SqlUtils.isObjectIdFunction(incomingExpression);
            if (dateFunction != null) {
                query.put(
                        dateFunction.getColumn(),
                        new Document(dateFunction.getComparisonExpression(), dateFunction.getDate())
                );
            } else if (objectIdFunction != null) {
                query.put(objectIdFunction.getColumn(), objectIdFunction.toDocument());
            } else if (incomingExpression instanceof EqualsTo) {
                final Expression leftExpression = ((EqualsTo) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((EqualsTo) incomingExpression).getRightExpression();
                if (leftExpression instanceof Function) {
                    query.put("$eq",
                            new Document(
                                    "arg1",
                                    parseExpression(new Document(), leftExpression, rightExpression)
                            ).append(
                                    "arg2",
                                    parseExpression(new Document(), rightExpression, leftExpression))
                    );
                } else {
                    query.put(parseExpression(new Document(), leftExpression, rightExpression).toString(),
                            parseExpression(new Document(), rightExpression, leftExpression));
                }
            } else if (incomingExpression instanceof NotEqualsTo) {
                final Expression leftExpression = ((NotEqualsTo) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((NotEqualsTo) incomingExpression).getRightExpression();

                if (leftExpression instanceof Function) {
                    query.put(
                            "$ne",
                            new Document(
                                    "arg1",
                                    parseExpression(new Document(), leftExpression, rightExpression)
                            ).append(
                                    "arg2",
                                    parseExpression(new Document(), rightExpression, leftExpression))
                    );
                } else {
                    query.put(
                            SqlUtils.getStringValue(leftExpression),
                            new Document("$ne", parseExpression(new Document(), rightExpression, leftExpression))
                    );
                }

            } else if (incomingExpression instanceof GreaterThan) {
                final Expression leftExpression = ((GreaterThan) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((GreaterThan) incomingExpression).getRightExpression();
                query.put(leftExpression.toString(), new Document("$gt",parseExpression(new Document(), rightExpression, leftExpression)));
            } else if (incomingExpression instanceof MinorThan) {
                final Expression leftExpression = ((MinorThan) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((MinorThan) incomingExpression).getRightExpression();
                query.put(leftExpression.toString(),new Document("$lt", parseExpression(new Document(), rightExpression, leftExpression)));
            } else if (incomingExpression instanceof GreaterThanEquals) {
                final Expression leftExpression = ((GreaterThanEquals) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((GreaterThanEquals) incomingExpression).getRightExpression();
                query.put(leftExpression.toString(),new Document("$gte",parseExpression(new Document(), rightExpression, leftExpression)));
            } else if (incomingExpression instanceof MinorThanEquals) {
                final Expression leftExpression = ((MinorThanEquals) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((MinorThanEquals) incomingExpression).getRightExpression();
                query.put(leftExpression.toString(),new Document("$lte", parseExpression(new Document(), rightExpression, leftExpression)));
            }
        } else if(incomingExpression instanceof LikeExpression
                && ((LikeExpression) incomingExpression).getLeftExpression() instanceof Column
                && (((LikeExpression) incomingExpression).getRightExpression() instanceof StringValue
                || ((LikeExpression) incomingExpression).getRightExpression() instanceof Column))
        {
            LikeExpression likeExpression = (LikeExpression) incomingExpression;
            String stringValueLeftSide = SqlUtils.getStringValue(likeExpression.getLeftExpression());
            String stringValueRightSide = SqlUtils.getStringValue(likeExpression.getRightExpression());

            Document document = new Document("$regex", "^" + SqlUtils.replaceRegexCharacters(stringValueRightSide) + "$");
            if (likeExpression.isNot()) {
                throw new IllegalStateException("NOT LIKE queries not supported");
            } else {
                document = new Document(stringValueLeftSide,document);
            }
            query.putAll(document);
        } else if(incomingExpression instanceof IsNullExpression) {
            IsNullExpression isNullExpression = (IsNullExpression) incomingExpression;
            query.put(SqlUtils.getStringValue(isNullExpression.getLeftExpression()), new Document("$exists", isNullExpression.isNot()));
        } else if(incomingExpression instanceof InExpression) {
            InExpression inExpression = (InExpression) incomingExpression;
            Expression leftExpression = ((InExpression) incomingExpression).getLeftExpression();
            String leftExpressionAsString = SqlUtils.getStringValue(leftExpression);
            ObjectIdFunction objectIdFunction = SqlUtils.isObjectIdFunction(incomingExpression);

            if (objectIdFunction != null) {
                query.put(objectIdFunction.getColumn(), objectIdFunction.toDocument());
            } else {
                List<Object> objectList = ((ExpressionList) inExpression.getRightItemsList())
                        .getExpressions()
                        .stream()
                        .map(expression -> parseExpression(new Document(), expression, leftExpression))
                        .collect(Collectors.toList());

                if (leftExpression instanceof Function) {
                    String mongoInFunction = inExpression.isNot() ? "$fnin" : "$fin";
                    query.put(
                            mongoInFunction,
                            new Document(
                                    "function",
                                    parseExpression(new Document(), leftExpression, otherSide)
                            ).append("list", objectList)
                    );
                } else {
                    String mongoInFunction = inExpression.isNot() ? "$nin" : "$in";
                    query.put(leftExpressionAsString, new Document(mongoInFunction, objectList));
                }
            }
        } else if(incomingExpression instanceof AndExpression) {
            handleAndOr("$and", (BinaryExpression)incomingExpression, query);
        } else if(incomingExpression instanceof OrExpression) {
            handleAndOr("$or", (BinaryExpression)incomingExpression, query);
        } else if(incomingExpression instanceof Parenthesis) {
            Parenthesis parenthesis = (Parenthesis) incomingExpression;
            Object expression = parseExpression(new Document(), parenthesis.getExpression(), null);
            if (parenthesis.isNot()) {
                return new Document("$nor", Collections.singletonList(expression));
            }
            return expression;
        } else if (incomingExpression instanceof NotExpression && otherSide == null) {
            Expression expression = ((NotExpression)incomingExpression).getExpression();
            return new Document(SqlUtils.getStringValue(expression), new Document("$ne", true));
        } else if (incomingExpression instanceof Function) {
            Function function = ((Function)incomingExpression);
            query.put("$" + function.getName(), SqlUtils.parseFunctionArguments(function.getParameters()));
        } else if (otherSide == null) {
            return new Document(SqlUtils.getStringValue(incomingExpression), true);
        } else {
            return SqlUtils.getValue(incomingExpression,otherSide);
        }

        return query;
    }

    private static void handleAndOr(String key, BinaryExpression incomingExpression, Document query) {
        final Expression leftExpression = incomingExpression.getLeftExpression();
        final Expression rightExpression = incomingExpression.getRightExpression();

        List<Object> result = flattenOrsOrAnds(new ArrayList<>(), leftExpression, leftExpression, rightExpression);

        if (result != null) {
            query.put(key, Lists.reverse(result));
        } else {
            query.put(
                    key,
                    Arrays.asList(
                            parseExpression(new Document(), leftExpression, rightExpression),
                            parseExpression(new Document(), rightExpression, leftExpression)
                    )
            );
        }
    }

    private static List<Object> flattenOrsOrAnds(List<Object> arrayList, Expression firstExpression, Expression leftExpression, Expression rightExpression) {
        if (firstExpression.getClass().isInstance(leftExpression)
                && isOrAndExpression(leftExpression)
                && !isOrAndExpression(rightExpression))
        {
            Expression left = ((BinaryExpression)leftExpression).getLeftExpression();
            Expression right = ((BinaryExpression)leftExpression).getRightExpression();
            arrayList.add(parseExpression(new Document(), rightExpression, null));
            List<Object> result = flattenOrsOrAnds(arrayList, firstExpression, left, right);
            if (result != null) {
                return arrayList;
            }
        } else if (isOrAndExpression(firstExpression) && !isOrAndExpression(leftExpression) && !isOrAndExpression(rightExpression)) {
            arrayList.add(parseExpression(new Document(), rightExpression, null));
            arrayList.add(parseExpression(new Document(), leftExpression, null));
            return arrayList;
        } else {
            return null;
        }
        return null;
    }

    private static boolean isOrAndExpression(Expression expression) {
        return expression instanceof OrExpression || expression instanceof AndExpression;
    }
}
