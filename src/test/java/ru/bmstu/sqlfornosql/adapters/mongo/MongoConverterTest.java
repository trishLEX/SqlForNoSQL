package ru.bmstu.sqlfornosql.adapters.mongo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import ru.bmstu.sqlfornosql.FunctionalTest;
import ru.bmstu.sqlfornosql.model.Table;

import java.util.Iterator;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {FunctionalTest.class})
public class MongoConverterTest extends FunctionalTest {
    private com.mongodb.MongoClient mongoClient;

    @Before
    public void before() {
        mongoClient = new com.mongodb.MongoClient();
    }

    @Test
    public void simpleSelect() {
        MongoClient client = new MongoClient("test", "test");
        Iterator<Table> table = client.executeQuery("SELECT test.intField, test.dateField FROM mongodb.test.test");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void simpleSelectWhere() {
        MongoClient client = new MongoClient("test", "test");
        Iterator<Table> table = client.executeQuery("SELECT test.intField, test.dateField FROM mongodb.test.test WHERE test.intField = 123");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void simpleSelectWhereLike() {
        MongoClient client = new MongoClient("test", "test");
        Iterator<Table> table = client.executeQuery("SELECT test.intField, test.dateField FROM mongodb.test.test WHERE test.stringField LIKE '%world'");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void simpleSelectCountAll() {
        MongoClient client = new MongoClient("test", "test");
        Iterator<Table> table = client.executeQuery("SELECT count(*) FROM mongodb.test.test WHERE test.intField = 123");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void selectCountAndSumField() {
        MongoClient client = new MongoClient("test", "test");
        Iterator<Table> table = client.executeQuery("SELECT count(test.intField), sum(test.intField) FROM mongodb.test.test");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void selectCountFieldWhere() {
        MongoClient client = new MongoClient("test", "test");
        Iterator<Table> table = client.executeQuery("SELECT count(test.intField) FROM mongodb.test.test WHERE test.intField = 123");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleSelectSumException() {
        MongoClient client = new MongoClient("test", "test");
        Iterator<Table> table = client.executeQuery("SELECT sum(test.intField), test FROM mongodb.test.test WHERE test.intField = 123");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleSelectGroupByException() {
        MongoClient client = new MongoClient("test", "test");
        Iterator<Table> table = client.executeQuery("SELECT test.intField, test.dateField FROM mongodb.test.test GROUP BY test.intField");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void simpleSelectGroupByAgg() {
        MongoClient client = new MongoClient("test", "test");
        Iterator<Table> table = client.executeQuery("SELECT sum(test.intField) FROM mongodb.test.test GROUP BY test.intField");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleSelectGroupByError() {
        MongoClient client = new MongoClient("test", "test");
        Iterator<Table> table = client.executeQuery("SELECT sum(test.intField), test.dateField FROM mongodb.test.test GROUP BY test.intField");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void simpleSelectGroupByHaving() {
        MongoClient client = new MongoClient("test", "test");
        Iterator<Table> table = client.executeQuery("SELECT sum(test.intField) FROM mongodb.test.test GROUP BY test.intField HAVING sum(test.intField) > 500");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void simpleSelectGroupByOrderBy() {
        MongoClient client = new MongoClient("test", "test");
        Iterator<Table> table = client.executeQuery("SELECT test.intField FROM mongodb.test.test GROUP BY test.intField ORDER BY test.intField");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void simpleSelectGroupByOrderByOrderBy() {
        MongoClient client = new MongoClient("test", "test");
        Iterator<Table> table = client.executeQuery("SELECT max(test.dateField) FROM mongodb.test.test GROUP BY test.dateField ORDER BY test.intField");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void selectWithQualifiedName() {
        MongoClient client = new MongoClient("test", "test");
        Iterator<Table> table = client.executeQuery("SELECT test.intField FROM mongodb.test.test");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @Test
    public void selectAll(){
        MongoClient client = new MongoClient("test", "test");
        Iterator<Table> table = client.executeQuery("SELECT * FROM mongodb.test.test");
        Table result = new Table();
        while (table.hasNext()) {
            result.add(table.next());
        }
        System.out.println(result);
    }

    @After
    public void after() {
        mongoClient.close();
    }
}
