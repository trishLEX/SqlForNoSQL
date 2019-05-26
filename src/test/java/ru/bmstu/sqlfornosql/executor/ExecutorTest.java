package ru.bmstu.sqlfornosql.executor;

import org.junit.Before;
import org.junit.Test;
import ru.bmstu.sqlfornosql.model.Table;

import java.util.Iterator;
import java.util.concurrent.CompletionException;
import java.util.regex.Matcher;

public class ExecutorTest {
    private Executor executor;

    @Before
    public void before() {
        this.executor = new Executor();
    }

    @Test
    public void testIdentMatcher() {
        String expression = "table1.a = table2.b AND table1.c = table1.d AND table.str LIKE 'hello'"
                .replaceAll("'.*'", "");
        Matcher matcher = Executor.IDENT_REGEXP.matcher(expression);
        while (matcher.find()) {
            if (!Executor.FORBIDDEN_STRINGS.contains(matcher.group(1))) {
                System.out.println(matcher.group(1));
            }
        }
    }

    @Test
    public void simpleSelectMongoTest() {
        String query = "SELECT * FROM mongodb.test.test";
        Iterator<Table> table = executor.execute(query);
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void simplePostgresTest() {
        String query = "SELECT * FROM postgres.postgres.test.test";
        Iterator<Table> table = executor.execute(query);
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test(expected = CompletionException.class)
    public void selectSeveralFromItemsException() {
        String query = "SELECT test.a, test.b FROM mongodb.test.test, postgres.postgres.test.test";
        Iterator<Table> table = executor.execute(query);
    }

    @Test
    public void selectSeveralFromItemsTest() {
        String query = "SELECT mongodb.test.test.intField, postgres.test.test.datefield FROM mongodb.test.test, postgres.postgres.test.test";
        Iterator<Table> table = executor.execute(query);
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void selectJoinTest() {
        String query = "SELECT mongodb.test.test.intField, postgres.test.test.intField, postgres.test.test.datefield FROM mongodb.test.test " +
                "JOIN postgres.postgres.test.test ON mongodb.test.test.intField = postgres.test.test.intField";
        Iterator<Table> table = executor.execute(query);
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void selectJoinOnFieldNotInSelect() {
        String query = "SELECT postgres.test.test.intField, postgres.test.test.datefield FROM mongodb.test.test " +
                "JOIN postgres.postgres.test.test ON mongodb.test.test.intField = postgres.test.test.intField";
        Iterator<Table> table = executor.execute(query);
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void selectJoinTestWhereOneTable() {
        String query = "SELECT postgres.test.test.intField, postgres.test.test.datefield FROM mongodb.test.test " +
                "JOIN postgres.postgres.test.test ON mongodb.test.test.intField = postgres.test.test.intField " +
                "WHERE mongodb.test.test.intField = 123";
        Iterator<Table> table = executor.execute(query);
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void selectJoinGroupBy() {
        String query = "SELECT max(postgres.test.test.dateField), postgres.test.test.intField FROM postgres.postgres.test.test JOIN mongodb.test.test " +
                "ON mongodb.test.test.intField = postgres.test.test.intField " +
                "GROUP BY postgres.test.test.intField";
        Iterator<Table> table = executor.execute(query);
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void selectJoinTestWhereTwoTables() {
        String query = "SELECT mongodb.test.test.intField, postgres.test.test.intField, postgres.test.test.datefield FROM mongodb.test.test " +
                "JOIN postgres.postgres.test.test ON mongodb.test.test.intField = postgres.test.test.intField " +
                "WHERE mongodb.test.test.intField + postgres.test.test.intField = 246";
        Iterator<Table> table = executor.execute(query);
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    //TODO нет логов
    @Test
    public void simpleSubSelect() {
        String query = "SELECT t.intField FROM (SELECT * FROM postgres.postgres.test.test) AS t WHERE t.intField = 123";
        Iterator<Table> table = executor.execute(query);
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void groupBySubSelect() {
        String query = "SELECT sum(t.intField) FROM (SELECT * FROM postgres.postgres.test.test) as t GROUP BY t.intField";
        Iterator<Table> table = executor.execute(query);
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    //TODO глянуть почему нет логов
    @Test
    public void groupBySubSelectWithWhere() {
        String query = "SELECT sum(t.intField) FROM " +
                "(SELECT * FROM postgres.postgres.test.test) as t WHERE t.intField = 123 " +
                "GROUP BY t.intField";
        Iterator<Table> table = executor.execute(query);
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void joinWithSubSelect() {
        String query = "SELECT t.intField, mongodb.test.test.dateField FROM (SELECT * FROM postgres.postgres.test.test) as t " +
                "JOIN mongodb.test.test ON t.intField = mongodb.test.test.intField";
        Iterator<Table> table = executor.execute(query);
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }
}
