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

import static ru.bmstu.sqlfornosql.executor.ExecutorUtils.getIdentMapping;
import static ru.bmstu.sqlfornosql.executor.ExecutorUtils.prepareSqlJEP;

@ParametersAreNonnullByDefault
public class Joiner {
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
    private static Table join(Table leftTable, Table rightTable, Expression onExpression, @Nullable Expression where) {
        Table result = new Table();
        HashMap<String, Integer> colMapping = getIdentMapping(onExpression.toString());
        RowJEP sqljep = prepareSqlJEP(onExpression, colMapping);

        for (Row leftRow : leftTable.getRows()) {
            for (Row rightRow : rightTable.getRows()) {
                Comparable[] row = new Comparable[colMapping.size()];

                for (Map.Entry<String, Integer> colMappingEntry : colMapping.entrySet()) {
                    row[colMappingEntry.getValue()] = getValue(leftRow, rightRow, colMappingEntry.getKey());
                }

                try {
                    Boolean expressionValue = (Boolean) sqljep.getValue(row);
                    if (expressionValue) {
                        addRow(result, joinRows(result, leftRow, rightRow, leftTable, rightTable), where);
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
                addRow(result, joinRows(result, leftRow, rightRow, leftTable, rightTable), where);
            }
        }

        return result;
    }

    private static Row joinRows(Table table, Row left, Row right, Table leftTable, Table rightTable) {
        Row result = new Row(table);
        for (String leftColumn : left.getColumns()) {
            result.add(leftColumn, left.getObject(leftColumn));
            table.setType(leftColumn, leftTable.getType(leftColumn));
        }

        for (String rightColumn : right.getColumns()) {
            result.add(rightColumn, right.getObject(rightColumn));
            table.setType(rightColumn, rightTable.getType(rightColumn));
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

    private static Comparable getValue(Row row, String key) {
        return (Comparable) row.getObject(key);
    }

    private static void addRow(Table table, Row row, @Nullable Expression where) {
        if (where == null) {
            table.add(row);
        } else {
            HashMap<String, Integer> colMapping = getIdentMapping(where.toString());
            RowJEP sqljep = prepareSqlJEP(where, colMapping);

            Comparable[] values = new Comparable[colMapping.size()];

            for (Map.Entry<String, Integer> colMappingEntry : colMapping.entrySet()) {
                values[colMappingEntry.getValue()] = getValue(row, colMappingEntry.getKey());
            }

            try {
                Boolean expressionValue = (Boolean) sqljep.getValue(values);
                if (expressionValue) {
                    table.add(row);
                }
            } catch (ParseException e) {
                throw new IllegalStateException("Can't execute expression: " + where, e);
            }
        }
    }
}
