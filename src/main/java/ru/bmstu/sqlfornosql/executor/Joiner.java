package ru.bmstu.sqlfornosql.executor;

import com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Join;
import org.medfoster.sqljep.ParseException;
import org.medfoster.sqljep.RowJEP;
import org.springframework.stereotype.Component;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectField;
import ru.bmstu.sqlfornosql.model.Row;
import ru.bmstu.sqlfornosql.model.Table;
import ru.bmstu.sqlfornosql.model.TableIterator;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static ru.bmstu.sqlfornosql.executor.ExecutorUtils.getIdentMapping;
import static ru.bmstu.sqlfornosql.executor.ExecutorUtils.prepareSqlJEP;

@ParametersAreNonnullByDefault
@Component
public class Joiner {
    public TableIterator join(SqlHolder holder, TableIterator from, List<TableIterator> joinTables, Collection<SelectField> additionalFields) {
        return new TableIterator() {
            private Iterator<Table> leftTableIterator = from.iterator();
            private int joinsSize = holder.getJoins().size();
            private int curJoin = 0;
            private Iterator<Table> curRightTableIterator = joinTables.get(curJoin).iterator();
            private CompletableFuture<Table> curLeftTableFuture;
            private CompletableFuture<Table> curRightTableFuture;

            @Nonnull
            @Override
            public Iterator<Table> iterator() {
                return join(holder, from, joinTables, additionalFields);
            }

            @Override
            public boolean hasNext() {
                boolean hasNext = leftTableIterator.hasNext() || curRightTableIterator.hasNext() || curJoin < joinsSize;
                if (!hasNext) {
                    return false;
                }

                if (curRightTableIterator.hasNext()) {
                    if (curRightTableFuture == null) {
                        curRightTableFuture = CompletableFuture.supplyAsync(curRightTableIterator::next, Executor.EXECUTOR);
                    }
                    if (curLeftTableFuture == null) {
                        curLeftTableFuture = CompletableFuture.supplyAsync(leftTableIterator::next, Executor.EXECUTOR);
                    }
                    return true;
                }

                curRightTableFuture = null;
                if (curJoin < joinsSize - 1) {
                    curJoin++;
                    curRightTableIterator = joinTables.get(curJoin).iterator();
                    return hasNext();
                }

                curLeftTableFuture = null;

                if (leftTableIterator.hasNext()) {
                    curJoin = 0;
                    curRightTableIterator = joinTables.get(curJoin).iterator();
                    curRightTableFuture = null;
                    if (curLeftTableFuture == null) {
                        curLeftTableFuture = CompletableFuture.supplyAsync(leftTableIterator::next, Executor.EXECUTOR);
                    }
                    return hasNext();
                }

                return false;
            }

            @Override
            public Table next() {
                if (hasNext()) {
                    if (curRightTableIterator.hasNext()) {
                        Table leftTable = curLeftTableFuture.join();
                        Table rightTable = curRightTableFuture.join();
                        Join join = holder.getJoins().get(curJoin);

                        curRightTableFuture = null;
                        Table joined = joinTables(leftTable, rightTable, join, holder, additionalFields);
                        if (joined.isEmpty() && hasNext()) {
                            joined = next();
                        }

                        if (!hasNext()) {
                            afterAll.forEach(Runnable::run);
                        }

                        return joined;
                    } else if (leftTableIterator.hasNext()) {
                        Table leftTable = curLeftTableFuture.join();
                        Table rightTable = curRightTableFuture.join();
                        Join join = holder.getJoins().get(curJoin);

                        curLeftTableFuture = null;
                        Table result = joinTables(leftTable, rightTable, join, holder, additionalFields);

                        if (!hasNext()) {
                            afterAll.forEach(Runnable::run);
                        }

                        return result;
                    }
                }

                throw new NoSuchElementException();
            }

            private Table joinTables(Table leftTable, Table rightTable, Join join, SqlHolder holder, Collection<SelectField> additionalFields) {
                if (join.getOnExpression() != null) {
                    return join(holder, leftTable, rightTable, join.getOnExpression(), additionalFields);
                } else {
                    return join(holder, leftTable, rightTable, additionalFields);
                }
            }
        };
    }

    private Table join(SqlHolder holder, Table leftTable, Table rightTable, Expression onExpression, Collection<SelectField> additionalFields) {
        String[] idents = onExpression.toString().split("=");
        if (onExpression.toString().split("=").length == 2
                && Executor.IDENT_REGEXP.matcher(idents[0]).find()
                && Executor.IDENT_REGEXP.matcher(idents[1]).find()
        ) {
            return hashJoin(holder, leftTable, rightTable, onExpression, additionalFields);
            //return innerLoopsJoin(holder, leftTable, rightTable, onExpression, additionalFields);
        } else {
            return innerLoopsJoin(holder, leftTable, rightTable, onExpression, additionalFields);
        }
    }

    @Nonnull
    private Table hashJoin(SqlHolder holder, Table leftTable, Table rightTable, Expression onExpression, Collection<SelectField> additionalFields) {
        String[] idents = onExpression.toString().split("=");
        idents[0] = idents[0].trim();
        idents[1] = idents[1].trim();

        Table minTable = leftTable.size() < rightTable.size() ? leftTable : rightTable;
        Table maxTable = leftTable == minTable ? rightTable : leftTable;

        if (minTable.isEmpty() || maxTable.isEmpty()) {
            return new Table();
        }

        String minTableIndex = minTable.getColumns().stream().anyMatch(col -> col.getUserInputName().equalsIgnoreCase(idents[0])) ? idents[0] : idents[1];
        String maxTableIndex = idents[0] == minTableIndex ? idents[1] : idents[0];

        HashMap<Object, List<Row>> minTableMap = new HashMap<>(minTable.size());

        for (Row row : minTable.getRows()) {
            Object rowIndex = row.getObject(holder.getByUserInput(minTableIndex));

            if (minTableMap.containsKey(rowIndex)) {
                minTableMap.get(rowIndex).add(row);
            } else {
                minTableMap.put(rowIndex, Lists.newArrayList(row));
            }
        }

        Table result = new Table();
        for (Row row : maxTable.getRows()) {
            List<Row> rowsToJoin = minTableMap.get(row.getObject(holder.getByUserInput(maxTableIndex)));
            if (rowsToJoin == null || rowsToJoin.isEmpty()) {
                continue;
            }
            for (Row rowToJoin : rowsToJoin) {
                addRow(holder, result, joinRows(result, row, rowToJoin, leftTable, rightTable), additionalFields);
            }
        }

        return result;
    }

    @Nonnull
    private Table innerLoopsJoin(SqlHolder holder, Table leftTable, Table rightTable, Expression onExpression, Collection<SelectField> additionalFields) {
        HashMap<String, Integer> colMapping = getIdentMapping(onExpression.toString());
        RowJEP sqljep = prepareSqlJEP(onExpression, colMapping);

        Table result = new Table();
        for (Row leftRow : leftTable.getRows()) {
            for (Row rightRow : rightTable.getRows()) {
                Comparable[] row = new Comparable[colMapping.size()];

                for (Map.Entry<String, Integer> colMappingEntry : colMapping.entrySet()) {
                    row[colMappingEntry.getValue()] = getValue(leftRow, rightRow, holder.getByUserInput(colMappingEntry.getKey()));
                }

                try {
                    Boolean expressionValue = (Boolean) sqljep.getValue(row);
                    if (expressionValue) {
                        addRow(holder, result, joinRows(result, leftRow, rightRow, leftTable, rightTable), additionalFields);
                    }
                } catch (ParseException e) {
                    throw new IllegalStateException("Can't execute expression: " + onExpression, e);
                }
            }
        }

        return result;
    }

    private Table join(SqlHolder holder, Table leftTable, Table rightTable, Collection<SelectField> additionalFields) {
        Table result = new Table();

        for (Row leftRow : leftTable.getRows()) {
            for (Row rightRow : rightTable.getRows()) {
                addRow(holder, result, joinRows(result, leftRow, rightRow, leftTable, rightTable), additionalFields);
            }
        }

        return result;
    }

    private Row joinRows(Table table, Row left, Row right, Table leftTable, Table rightTable) {
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

    private Comparable getValue(Row left, Row right, SelectField key) {
        if (!left.contains(key) ^ right.contains(key)) {
            throw new IllegalStateException("Column " + key + " is clashed");
        }

        if (left.contains(key)) {
            return (Comparable) left.getObject(key);
        } else {
            return (Comparable) right.getObject(key);
        }
    }

    private Comparable getValue(Row row, SelectField key) {
        return (Comparable) row.getObject(key);
    }

    //TODO holder сделать полем Joiner (когда бин будет)
    private void addRow(SqlHolder holder, Table table, Row row, Collection<SelectField> additionalFields) {
        Expression where = holder.getWhereClause();
        if (where == null) {
            row.remove(additionalFields);
            additionalFields.forEach(table.getTypeMap()::remove);
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
                    additionalFields.forEach(table.getTypeMap()::remove);
                    row.remove(additionalFields);
                    table.add(row);
                }
            } catch (ParseException e) {
                throw new IllegalStateException("Can't execute expression: " + where, e);
            }
        }
    }
}
