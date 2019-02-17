package ru.bmstu.sqlfornosql.adapters.mongo;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.stream.Collectors;

public class ObjectIdFunction {
    private final Object value;
    private final String column;
    private final Expression comparisonExpression;

    public ObjectIdFunction(String column, Object value, Expression expression) {
        this.column = column;
        this.value = value;
        this.comparisonExpression = expression;
    }

    public String getColumn() {
        return column;
    }

    @SuppressWarnings("unchecked")
    public Object toDocument() {
        if (comparisonExpression instanceof EqualsTo) {
            return new ObjectId(value.toString());
        } else if (comparisonExpression instanceof NotEqualsTo) {
            return new Document("$ne", new ObjectId(value.toString()));
        } else if (comparisonExpression instanceof InExpression) {
            InExpression inExpression = (InExpression) comparisonExpression;
            List<String> stringList = (List<String>) value;
            return new Document(inExpression.isNot() ? "$nin" : "$in", stringList.stream().map(ObjectId::new).collect(Collectors.toList()));
        }

        throw new IllegalStateException("Count not convert ObjectId function into document");
    }
}