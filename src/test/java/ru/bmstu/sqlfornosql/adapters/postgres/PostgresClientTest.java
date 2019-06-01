package ru.bmstu.sqlfornosql.adapters.postgres;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import ru.bmstu.sqlfornosql.FunctionalTest;
import ru.bmstu.sqlfornosql.executor.Executor;
import ru.bmstu.sqlfornosql.model.Table;

import java.util.Iterator;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {FunctionalTest.class})
public class PostgresClientTest extends FunctionalTest {
    @Autowired
    private Executor executor;

    @Test
    public void selectAllTest() {
        Iterator<Table> table = executor.execute("SELECT * FROM postgres.postgres.test.test");
        System.out.println(table.next());
    }

    @Test
    public void groupByTest() {
        Iterator<Table> table = executor.execute("SELECT max(test.intField), max(test.dateField) FROM postgres.postgres.test.test GROUP BY test.intField");
        System.out.println(table.next());
    }
}
