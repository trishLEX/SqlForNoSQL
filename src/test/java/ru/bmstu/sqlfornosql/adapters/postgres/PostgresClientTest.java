package ru.bmstu.sqlfornosql.adapters.postgres;

import org.junit.Before;
import org.junit.Test;
import ru.bmstu.sqlfornosql.executor.Executor;
import ru.bmstu.sqlfornosql.model.Table;

import java.util.concurrent.CompletableFuture;

public class PostgresClientTest {
    private Executor executor;

    @Before
    public void before() {
        executor = new Executor();
    }

    @Test
    public void selectAllTest() {
        CompletableFuture<Table> table = executor.execute("SELECT * FROM postgres.postgres.test.test");
        System.out.println(table.join());
    }

    @Test
    public void groupByTest() {
        CompletableFuture<Table> table = executor.execute("SELECT max(test.intField), max(test.dateField) FROM postgres.postgres.test.test GROUP BY test.intField");
        System.out.println(table.join());
    }
}
