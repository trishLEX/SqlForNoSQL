package ru.bmstu.sqlfornosql.executor;

import org.junit.Before;
import org.junit.Test;
import ru.bmstu.sqlfornosql.model.Table;

import java.util.regex.Matcher;

public class ExecutorTest {
    //TODO в тестах сделать assert на кол-во элементов
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
        Table table = executor.execute(query);
        System.out.println(table);
    }

    @Test
    public void simplePostgresTest() {
        String query = "SELECT * FROM postgres.postgres.test.test";
        Table table = executor.execute(query);
        System.out.println(table);
    }

    @Test(expected = IllegalArgumentException.class)
    public void selectSeveralFromItemsException() {
        String query = "SELECT test.a, test.b FROM mongodb.test.test, postgres.postgres.test.test";
        Table table = executor.execute(query);
        System.out.println(table);
    }

    @Test
    public void selectSeveralFromItemsTest() {
        String query = "SELECT mongodb.test.test.intField, postgres.test.test.datefield FROM mongodb.test.test, postgres.postgres.test.test";
        Table table = executor.execute(query);
        System.out.println(table);
    }

    @Test
    public void selectJoinTest() {
        String query = "SELECT mongodb.test.test.intField, postgres.test.test.intField, postgres.test.test.datefield FROM mongodb.test.test " +
                "JOIN postgres.postgres.test.test ON mongodb.test.test.intField = postgres.test.test.intField";
        Table table = executor.execute(query);
        System.out.println(table);
    }

    @Test
    public void selectJoinTestWhereOneTable() {
        String query = "SELECT postgres.test.test.intField, postgres.test.test.datefield FROM mongodb.test.test " +
                "JOIN postgres.postgres.test.test ON mongodb.test.test.intField = postgres.test.test.intField " +
                "WHERE mongodb.test.test.intField = 123";
        Table table = executor.execute(query);
        System.out.println(table);
    }

    @Test
    public void selectJoinTestWhereTwoTables() {
        String query = "SELECT mongodb.test.test.intField, postgres.test.test.intField, postgres.test.test.datefield FROM mongodb.test.test " +
                "JOIN postgres.postgres.test.test ON mongodb.test.test.intField = postgres.test.test.intField " +
                "WHERE mongodb.test.test.intField + postgres.test.test.intField = 246";
        Table table = executor.execute(query);
        System.out.println(table);
    }

    @Test
    public void simpleSubSelect() {
        String query = "SELECT postgres.postgres.test.test.intField FROM (SELECT * FROM postgres.postgres.test.test) WHERE postgres.postgres.test.test.intField = 123";
        Table table = executor.execute(query);
        System.out.println(table);
    }

    @Test
    public void groupBySubSelect() {
        String query = "SELECT sum(postgres.postgres.test.test.intField) FROM" +
                " (SELECT * FROM postgres.postgres.test.test) GROUP BY postgres.postgres.test.test.intField";
        Table table = executor.execute(query);
        System.out.println(table);
    }

    @Test
    public void groupBySubSelectWithWhere() {
        String query = "SELECT sum(postgres.postgres.test.test.intField) FROM" +
                " (SELECT * FROM postgres.postgres.test.test) WHERE postgres.postgres.test.test.intField = 123 " +
                "GROUP BY postgres.postgres.test.test.intField";

        Table table = executor.execute(query);
        System.out.println(table);
    }

    @Test
    public void joinWithSubSelect() {
        String query = "SELECT postgres.postgres.test.test.intField, mongodb.test.test.dateField FROM (SELECT * FROM postgres.postgres.test.test) " +
                "JOIN mongodb.test.test ON postgres.postgres.test.test.intField = mongodb.test.test.intField";
        Table table = executor.execute(query);
        System.out.println(table);
    }
}
