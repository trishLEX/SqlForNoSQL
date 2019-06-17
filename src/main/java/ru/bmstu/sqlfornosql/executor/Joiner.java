package ru.bmstu.sqlfornosql.executor;

import com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Join;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
    private static final Logger logger = LogManager.getLogger(Joiner.class);
    public TableIterator join(
            SqlHolder sqlHolder,
            TableIterator left, TableIterator right,
            SelectField leftKey, SelectField rightKey,
            Collection<SelectField> additionalFields
    ) {
        return new TableIterator() {
            private Iterator<Table> leftTableIterator = left.iterator();
            private Iterator<Table> rightTableIterator = right.iterator();
            private CompletableFuture<Table> curLeftTableFuture;
            private CompletableFuture<Table> curRightTableFuture;
            private long limit = sqlHolder.getLimit();
            private long count = 0;
            private Comparable lastLeftJoinKey;
            private Comparable lastRightJoinKey;
            private SelectField leftTableKey = leftKey;
            private SelectField rightTableKey = rightKey;
            private SqlHolder holder = sqlHolder;

            @Nonnull
            @Override
            public Iterator<Table> iterator() {
                return join(holder, left, right, leftKey, rightKey, additionalFields);
            }

            @Override
            public boolean hasNext() {
                if (limit > 0 && count >= limit) {
                    return false;
                }

                if (curRightTableFuture != null && curLeftTableFuture != null) {
                    return true;
                } else {
                    boolean hasNext = leftTableIterator.hasNext() && rightTableIterator.hasNext();
                    if (!hasNext) {
                        return false;
                    }

                    if (curRightTableFuture == null) {
                        curRightTableFuture = CompletableFuture.supplyAsync(rightTableIterator::next, Executor.EXECUTOR);
                    }
                    if (curLeftTableFuture == null) {
                        curLeftTableFuture = CompletableFuture.supplyAsync(leftTableIterator::next, Executor.EXECUTOR);
                    }
                    return true;
                }
            }

            @Override
            public Table next() {
                if (hasNext()) {
                        Table leftTable = curLeftTableFuture.join();
                        Table rightTable = curRightTableFuture.join();

                        Table joined = joinTables(leftTable, rightTable, holder.getJoin(), additionalFields);
                        lastLeftJoinKey = getValue(leftTable.getRows().get(leftTable.size() - 1), leftKey);
                        lastRightJoinKey = getValue(rightTable.getRows().get(rightTable.size() - 1), rightKey);
                        if (lastLeftJoinKey instanceof Integer) {
                            lastLeftJoinKey = ((Integer) lastLeftJoinKey).doubleValue();
                        }
                        if (lastRightJoinKey instanceof Integer) {
                            lastRightJoinKey = ((Integer) lastRightJoinKey).doubleValue();
                        }
                        if (lastLeftJoinKey.compareTo(lastRightJoinKey) < 0) {
                            curLeftTableFuture = null;
                        } else {
                            curRightTableFuture = null;
                        }

                        count += joined.size();

                        if (!hasNext()) {
                            afterAll.forEach(Runnable::run);
                        }

                        return joined;
                }

                throw new NoSuchElementException();
            }

            private Table joinTables(Table leftTable, Table rightTable, Join join, Collection<SelectField> additionalFields) {
                if (join.getOnExpression() != null) {
                    return join(
                            holder,
                            leftTable, rightTable,
                            leftTableKey, rightTableKey,
                            join.getOnExpression(), additionalFields, limit
                    );
                } else {
                    return join(holder, leftTable, rightTable, additionalFields);
                }
            }
        };
    }

    private Table join(SqlHolder holder,
                       Table leftTable, Table rightTable,
                       SelectField leftTableKey, SelectField rightTableKey,
                       Expression onExpression, Collection<SelectField> additionalFields, long limit
    ) {
        String[] idents = onExpression.toString().split("=");
        if (onExpression.toString().split("=").length == 2
                && Executor.IDENT_REGEXP.matcher(idents[0]).find()
                && Executor.IDENT_REGEXP.matcher(idents[1]).find()
        ) {
            return hashJoin(holder, leftTable, rightTable, leftTableKey, rightTableKey, additionalFields, limit);
            //return innerLoopsJoin(holder, leftTable, rightTable, onExpression, additionalFields);
        } else {
            return innerLoopsJoin(holder, leftTable, rightTable, onExpression, additionalFields);
        }
    }

    @Nonnull
    private Table hashJoin(SqlHolder holder,
                           Table leftTable, Table rightTable,
                           SelectField leftTableKey, SelectField rightTableKey,
                           Collection<SelectField> additionalFields, long limit
    ) {
        Table minTable;
        Table maxTable;
        SelectField minTableIndex;
        SelectField maxTableIndex;
        if (leftTable.size() < rightTable.size()) {
            minTable = leftTable;
            maxTable = rightTable;
            minTableIndex = leftTableKey;
            maxTableIndex = rightTableKey;
        } else {
            maxTable = leftTable;
            minTable = rightTable;
            maxTableIndex = leftTableKey;
            minTableIndex = rightTableKey;
        }

        if (minTable.isEmpty() || maxTable.isEmpty()) {
            return new Table();
        }

        HashMap<Object, List<Row>> minTableMap = new HashMap<>(minTable.size());

        for (Row row : minTable.getRows()) {
            Object rowIndex = row.getObject(minTableIndex);

            if (minTableMap.containsKey(rowIndex)) {
                minTableMap.get(rowIndex).add(row);
            } else {
                minTableMap.put(rowIndex, Lists.newArrayList(row));
            }
        }

        Table result = new Table();
        for (Row row : maxTable.getRows()) {
            List<Row> rowsToJoin = minTableMap.get(row.getObject(maxTableIndex));
            if (rowsToJoin == null || rowsToJoin.isEmpty()) {
                continue;
            }
            for (Row rowToJoin : rowsToJoin) {
                addRow(holder, result, joinRows(result, row, rowToJoin, maxTable, minTable), additionalFields);
                if (result.size() == limit) {
                    return result;
                }
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
