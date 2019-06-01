package ru.bmstu.sqlfornosql.executor;

import org.junit.Before;
import org.junit.Test;
import ru.bmstu.sqlfornosql.model.Table;

import java.util.Iterator;

public class SimplePostgresSelectTest {
    private Executor executor;

    @Before
    public void before() {
        executor = new Executor();
    }

    @Test
    public void simpleSelect() {
        Iterator<Table> table = executor.execute("SELECT postgres.postgres.test.test.dateField, postgres.postgres.test.test.intField FROM postgres.postgres.test.test");
        printResult(table);
    }

    @Test
    public void whereSelect() {
        Iterator<Table> table = executor.execute(
                "SELECT postgres.postgres.test.test.dateField, postgres.postgres.test.test.intField " +
                        "FROM postgres.postgres.test.test " +
                        "WHERE postgres.postgres.test.test.intField = 123"
        );
        printResult(table);
    }

    @Test
    public void maxMinCountAggregateSelectWithoutGroup() {
        Iterator<Table> table = executor.execute(
                "SELECT max(test.dateField), min(test.dateField), count(test.strField) FROM postgres.postgres.test.test"
        );
        printResult(table);
    }

    @Test
    public void avgSumAggregateSelectWithoutGroup() {
        Iterator<Table> table = executor.execute(
                "SELECT avg(test.intField), sum(test.intField) FROM postgres.postgres.test.test"
        );
        printResult(table);
    }

    @Test
    public void aggregateWithGroupBy() {
        Iterator<Table> table = executor.execute(
                "SELECT avg(test.intField), sum(test.intField) FROM postgres.postgres.test.test GROUP BY postgres.postgres.test.test.dateField"
        );
        printResult(table);
    }

    @Test
    public void aggregateWhereGroupBy() {
        Iterator<Table> table = executor.execute(
                "SELECT count(test.intField) FROM postgres.postgres.test.test WHERE postgres.test.test.dateField IS NULL GROUP BY postgres.postgres.test.test.dateField"
        );
        printResult(table);
    }

    @Test
    public void aggregateGroupByHaving() {
        Iterator<Table> table = executor.execute(
                "SELECT count(test.intField), test.dateField FROM postgres.postgres.test.test GROUP BY postgres.postgres.test.test.dateField HAVING postgres.test.test.dateField IS NULL"
        );
        printResult(table);
    }

    @Test
    public void selectOrder() {
        Iterator<Table> table = executor.execute(
                "SELECT test.intField FROM postgres.postgres.test.test ORDER BY test.intField"
        );
        printResult(table);
    }

    @Test //TODO
    public void selectOrderLimit() {
        Iterator<Table> table = executor.execute(
                "SELECT test.intField FROM postgres.postgres.test.test ORDER BY test.intField LIMIT 2"
        );
        printResult(table);
    }

    private void printResult(Iterator<Table> table) {
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }
}
