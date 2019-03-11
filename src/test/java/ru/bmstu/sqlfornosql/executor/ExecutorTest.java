package ru.bmstu.sqlfornosql.executor;

import java.util.regex.Matcher;

import org.junit.Before;
import org.junit.Test;
import ru.bmstu.sqlfornosql.model.Table;

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
    public void simpleSelectTestMongo() {
        String query = "SELECT * FROM mongodb.test.test";
        Table table = executor.execute(query);
        System.out.println(table);
    }

    @Test(expected = IllegalArgumentException.class)
    public void selectSeveralFromItemsException() {
        String query = "SELECT test.a, test.b FROM mongodb.test.test, postgres.db.schema.test";
        Table table = executor.execute(query);
        System.out.println(table);
    }

    @Test
    public void selectSeveralFromItems() {
        //TODO можно мапить имена колонок в их fullQualifiedName
        //TODO затестить fullQualifiedName в mongo
        String query = "SELECT test.test.a, schema.test.b FROM mongodb.test.test, postgres.db.schema.test";
        Table table = executor.execute(query);
        System.out.println(table);
    }
}
