package ru.bmstu.sqlfornosql.executor;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Join;
import org.medfoster.sqljep.ParseException;
import org.medfoster.sqljep.RowJEP;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectField;
import ru.bmstu.sqlfornosql.model.Row;
import ru.bmstu.sqlfornosql.model.Table;
import ru.bmstu.sqlfornosql.model.TableIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static ru.bmstu.sqlfornosql.executor.ExecutorUtils.getIdentMapping;
import static ru.bmstu.sqlfornosql.executor.ExecutorUtils.prepareSqlJEP;

@ParametersAreNonnullByDefault
public class Joiner {
    public static TableIterator join(SqlHolder holder, TableIterator from, List<TableIterator> joinTables, List<Join> joins, @Nullable Expression where) {
        return new TableIterator() {
            private Iterator<Table> leftTableIterator = from.iterator();

            @Nonnull
            @Override
            public Iterator<Table> iterator() {
                return join(holder, from, joinTables, joins, where);
            }

            @Override
            public boolean hasNext() {
                return leftTableIterator.hasNext();
            }

            @Override
            public Table next() {
                if (hasNext()) {
                    Table leftTable = null;
                    CompletableFuture<Table> leftTableFuture = CompletableFuture.supplyAsync(leftTableIterator::next, Executor.EXECUTOR);
                    for (int i = 0; i < joins.size(); i++) {
                        Iterator<Table> rightTableIterator = joinTables.get(i).iterator();
                        Join join = joins.get(i);
                        if (join.getOnExpression() != null) {
                            while (rightTableIterator.hasNext()) {
                                CompletableFuture<Table> rightTable = CompletableFuture.supplyAsync(rightTableIterator::next, Executor.EXECUTOR);
                                leftTable = join(holder, leftTableFuture.join(), rightTable.join(), join.getOnExpression(), where);
                            }
                        } else {
                            while (rightTableIterator.hasNext()) {
                                CompletableFuture<Table> rightTable = CompletableFuture.supplyAsync(rightTableIterator::next, Executor.EXECUTOR);
                                leftTable = join(holder, leftTableFuture.join(), rightTable.join(), where);
                            }
                        }
                    }

                    if (leftTable != null) {
                        return leftTable;
                    } else {
                        return leftTableFuture.join();
                    }
                }

                throw new NoSuchElementException();
            }
        };
//        return CompletableFuture.supplyAsync(
//                () -> {
//
//                    TableIterator leftTable = from;
//                    for (int i = 0; i < joins.size(); i++) {
//                        Table rightTable = joinTables.get(i).join();
//                        Join join = joins.get(i);
//
//                        if (join.getOnExpression() != null) {
//                            leftTable = join(holder, leftTable, rightTable, join.getOnExpression(), where);
//                        } else {
//                            leftTable = join(holder, leftTable, rightTable, where);
//                        }
//                    }
//
//                    return leftTable;
//                },
//                Executor.EXECUTOR);
    }

    //TODO join работает сейчас только по полям, которые есть в selectItems!!!
    private static Table join(SqlHolder holder, Table leftTable, Table rightTable, Expression onExpression, @Nullable Expression where) {
        Table result = new Table();
        HashMap<String, Integer> colMapping = getIdentMapping(onExpression.toString());
        RowJEP sqljep = prepareSqlJEP(onExpression, colMapping);

        for (Row leftRow : leftTable.getRows()) {
            for (Row rightRow : rightTable.getRows()) {
                Comparable[] row = new Comparable[colMapping.size()];

                for (Map.Entry<String, Integer> colMappingEntry : colMapping.entrySet()) {
                    row[colMappingEntry.getValue()] = getValue(leftRow, rightRow, holder.getByUserInput(colMappingEntry.getKey()));
                }

                try {
                    Boolean expressionValue = (Boolean) sqljep.getValue(row);
                    if (expressionValue) {
                        addRow(holder, result, joinRows(result, leftRow, rightRow, leftTable, rightTable), where);
                    }
                } catch (ParseException e) {
                    throw new IllegalStateException("Can't execute expression: " + onExpression, e);
                }
            }
        }

        return result;
    }

    private static Table join(SqlHolder holder, Table leftTable, Table rightTable, @Nullable Expression where) {
        Table result = new Table();

        for (Row leftRow : leftTable.getRows()) {
            for (Row rightRow : rightTable.getRows()) {
                addRow(holder, result, joinRows(result, leftRow, rightRow, leftTable, rightTable), where);
            }
        }

        return result;
    }

    private static Row joinRows(Table table, Row left, Row right, Table leftTable, Table rightTable) {
        Row result = new Row(table);
        for (SelectField leftColumn : left.getColumns()) {
            result.add(leftColumn, left.getObject(leftColumn));
            table.setType(leftColumn, leftTable.getType(leftColumn));
        }

        for (SelectField rightColumn : right.getColumns()) {
            result.add(rightColumn, right.getObject(rightColumn));
            table.setType(rightColumn, rightTable.getType(rightColumn));
        }

        return result;
    }

    private static Comparable getValue(Row left, Row right, SelectField key) {
        if (!left.contains(key) ^ right.contains(key)) {
            throw new IllegalStateException("Column " + key + " is clashed");
        }

        if (left.contains(key)) {
            return (Comparable) left.getObject(key);
        } else {
            return (Comparable) right.getObject(key);
        }
    }

    private static Comparable getValue(Row row, SelectField key) {
        return (Comparable) row.getObject(key);
    }

    //TODO holder сделать полем Joiner (когда бин будет)
    private static void addRow(SqlHolder holder, Table table, Row row, @Nullable Expression where) {
        if (where == null) {
            table.add(row);
        } else {
            HashMap<String, Integer> colMapping = getIdentMapping(where.toString());
            RowJEP sqljep = prepareSqlJEP(where, colMapping);

            Comparable[] values = new Comparable[colMapping.size()];

            for (Map.Entry<String, Integer> colMappingEntry : colMapping.entrySet()) {
                values[colMappingEntry.getValue()] = getValue(row, holder.getByUserInput(colMappingEntry.getKey()));
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
