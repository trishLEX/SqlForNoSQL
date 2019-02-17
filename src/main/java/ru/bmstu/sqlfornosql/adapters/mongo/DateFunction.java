package ru.bmstu.sqlfornosql.adapters.mongo;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import net.sf.jsqlparser.expression.operators.relational.*;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

public class DateFunction {
    private final LocalDateTime date;
    private final String column;
    private String comparisonExpression = "$eq";

    public DateFunction(String format,String value, String column) {
        if ("natural".equals(format)) {
            this.date = parseNaturalLanguageDate(value);
        } else {
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(format);
            this.date = LocalDateTime.parse(value, dateTimeFormatter);
        }
        this.column = column;
    }


    public LocalDateTime getDate() {
        return date;
    }

    public String getColumn() {
        return column;
    }

    public void setComparisonFunction(ComparisonOperator comparisonFunction) {
        if (comparisonFunction instanceof GreaterThanEquals) {
            this.comparisonExpression = "$gte";
        } else if (comparisonFunction instanceof GreaterThan) {
            this.comparisonExpression = "$gt";
        } else if (comparisonFunction instanceof MinorThanEquals) {
            this.comparisonExpression = "$lte";
        } else if (comparisonFunction instanceof MinorThan) {
            this.comparisonExpression = "$lt";
        } else {
            throw new IllegalStateException("could not parseNaturalLanguageDate string expression: " + comparisonFunction.getStringExpression());
        }
    }

    public String getComparisonExpression() {
        return comparisonExpression;
    }

    private LocalDateTime parseNaturalLanguageDate(String text) {
        Parser parser = new Parser();
        List<DateGroup> groups = parser.parse(text);
        for (DateGroup group : groups) {
            List<Date> dates = group.getDates();
            if (dates.size() > 0) {
                return LocalDateTime.ofInstant(dates.get(0).toInstant(), ZoneId.systemDefault());
            }
        }
        throw new IllegalArgumentException("could not natural language date: "+ text);
    }
}
