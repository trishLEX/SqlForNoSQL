package ru.bmstu.sqlfornosql.executor;

import org.junit.Before;
import org.junit.Test;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.adapters.sql.SqlUtils;
import ru.bmstu.sqlfornosql.model.Table;
import ru.bmstu.sqlfornosql.model.TableIterator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;

public class GrouperTest {
    @Before
    public void before() {
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Can't load driver", e);
        }

        try (Connection connection = DriverManager.getConnection("jdbc:h2:~/sqlForNoSql;AUTO_SERVER=TRUE", "h2", "")) {
            connection.createStatement().execute("DROP ALL OBJECTS");
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void testGroupBy() {
        String query = "SELECT max(postgres.postgres.test.test.dateField) FROM postgres.postgres.test.test GROUP BY postgres.postgres.test.test.intField";
        Executor executor = new Executor();
        TableIterator table = executor.execute("SELECT postgres.postgres.test.test.dateField, postgres.postgres.test.test.intField FROM postgres.postgres.test.test");
        SqlHolder holder = SqlUtils.fillSqlMeta(query);
        table = Grouper.groupInDb(holder, table, "support_table1");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    //TODO HAVING не работает (нужны alias'ы)
    @Test
    public void testGroupByHaving() {
        String query = "SELECT max(postgres.postgres.test.test.dateField) AS t FROM postgres.postgres.test.test GROUP BY postgres.postgres.test.test.intField HAVING t IS NULL";
        Executor executor = new Executor();
        Iterator<Table> table = executor.execute("SELECT postgres.postgres.test.test.dateField, postgres.postgres.test.test.intField FROM postgres.postgres.test.test");
        SqlHolder holder = SqlUtils.fillSqlMeta(query);
        table = Grouper.groupInDb(holder, table, "support_table2");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }
}
