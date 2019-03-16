package ru.bmstu.sqlfornosql.executor;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Join;
import org.medfoster.sqljep.ParseException;
import org.medfoster.sqljep.RowJEP;
import ru.bmstu.sqlfornosql.model.Row;
import ru.bmstu.sqlfornosql.model.Table;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import static ru.bmstu.sqlfornosql.executor.Executor.FORBIDDEN_STRINGS;
import static ru.bmstu.sqlfornosql.executor.Executor.IDENT_REGEXP;

@ParametersAreNonnullByDefault
public class Joiner {
    //TODO whereExpression can be null

    public static Table join(Table from, List<Table> joinTables, List<Join> joins, @Nullable Expression where) {
        Table leftTable = from;
        for (int i = 0; i < joins.size(); i++) {
            Table rightTable = joinTables.get(i);
            Join join = joins.get(i);

            if (join.getOnExpression() != null) {
                leftTable = join(leftTable, rightTable, join.getOnExpression(), where);
            } else {
                leftTable = join(leftTable, rightTable, where);
            }
        }

        return leftTable;
    }

    //TODO join работает сейчас только по полям, которые есть в selectItems!!!
    //TODO WHERE!!!
    private static Table join(Table leftTable, Table rightTable, Expression onExpression, @Nullable Expression where) {
        Table result = new Table();
        RowJEP sqljep = new RowJEP(onExpression.toString());
        HashMap<String, Integer> colMapping = getIdentMapping(onExpression.toString());
        try {
            sqljep.parseExpression(colMapping);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Can't parse expression: " + onExpression, e);
        }

        for (Row leftRow : leftTable.getRows()) {
            for (Row rightRow : rightTable.getRows()) {
                Comparable[] row = new Comparable[colMapping.size()];

                for (Map.Entry<String, Integer> colMappingEntry : colMapping.entrySet()) {
                    row[colMappingEntry.getValue()] = getValue(leftRow, rightRow, colMappingEntry.getKey());
                }

                try {
                    Boolean expressionValue = (Boolean) sqljep.getValue(row);
                    if (expressionValue) {
                        result.add(joinRows(leftRow, rightRow));
                    }
                } catch (ParseException e) {
                    throw new IllegalStateException("Can't execute expression: " + onExpression, e);
                }
            }
        }

        return result;
    }

    private static Table join(Table leftTable, Table rightTable, @Nullable Expression where) {
        Table result = new Table();

        for (Row leftRow : leftTable.getRows()) {
            for (Row rightRow : rightTable.getRows()) {
                result.add(joinRows(leftRow, rightRow));
            }
        }

        return result;
    }

    private static Row joinRows(Row left, Row right) {
        Row result = new Row();
        for (String leftColumn : left.getColumns()) {
            result.add(leftColumn, left.getObject(leftColumn), left.getType(leftColumn));
        }

        for (String rightColumn : right.getColumns()) {
            result.add(rightColumn, right.getObject(rightColumn), right.getType(rightColumn));
        }

        return result;
    }

    private static Comparable getValue(Row left, Row right, String key) {
        if (!left.contains(key) ^ right.contains(key)) {
            throw new IllegalStateException("Column " + key + " is clashed");
        }

        if (left.contains(key)) {
            return (Comparable) left.getObject(key);
        } else {
            return (Comparable) right.getObject(key);
        }
    }

    private static HashMap<String, Integer> getIdentMapping(String expression) {
        Matcher matcher = IDENT_REGEXP.matcher(expression.replaceAll("'.*'", ""));
        HashMap<String, Integer> mapping = new HashMap<>();
        int index = 0;
        while (matcher.find()) {
            if (!FORBIDDEN_STRINGS.contains(matcher.group(1).toUpperCase())) {
                mapping.put(matcher.group(1), index++);
            }
        }

        return mapping;
    }
}
