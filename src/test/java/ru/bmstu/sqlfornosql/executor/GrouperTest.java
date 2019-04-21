package ru.bmstu.sqlfornosql.executor;

import org.junit.Test;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.adapters.sql.SqlUtils;
import ru.bmstu.sqlfornosql.model.Table;

import java.util.concurrent.CompletableFuture;

public class GrouperTest {
    @Test
    public void testGroupBy() {
        String query = "SELECT max(postgres.postgres.test.test.dateField) FROM postgres.postgres.test.test GROUP BY postgres.postgres.test.test.intField";
        Executor executor = new Executor();
        CompletableFuture<Table> table = executor.execute("SELECT postgres.postgres.test.test.dateField, postgres.postgres.test.test.intField FROM postgres.postgres.test.test");
        SqlHolder holder = SqlUtils.fillSqlMeta(query);
        System.out.println(Grouper.groupInDb(holder, table.join(), holder.getGroupBys(), holder.getSelectFields(), null));
    }

    //TODO HAVING не работает (нужны alias'ы)
    @Test
    public void testGroupByHaving() {
        String query = "SELECT max(postgres.postgres.test.test.dateField) FROM postgres.postgres.test.test GROUP BY postgres.postgres.test.test.intField";
        Executor executor = new Executor();
        CompletableFuture<Table> table = executor.execute("SELECT postgres.postgres.test.test.dateField, postgres.postgres.test.test.intField FROM postgres.postgres.test.test");
        SqlHolder holder = SqlUtils.fillSqlMeta(query);
        System.out.println(Grouper.groupInDb(holder, table.join(), holder.getGroupBys(), holder.getSelectFields(), null));
    }
}
