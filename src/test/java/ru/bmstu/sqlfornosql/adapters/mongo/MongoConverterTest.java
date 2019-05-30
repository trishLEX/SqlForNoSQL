package ru.bmstu.sqlfornosql.adapters.mongo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MongoConverterTest {
    private com.mongodb.MongoClient mongoClient;

    @Before
    public void before() {
        mongoClient = new com.mongodb.MongoClient();
    }

    @Test
    public void simpleSelect() {
        MongoClient client = new MongoClient("test", "test");
        System.out.println(client.executeQuery("SELECT test.intField, test.dateField FROM mongodb.test.test"));
    }

    @Test
    public void simpleSelectWhere() {
        MongoClient client = new MongoClient("test", "test");
        System.out.println(client.executeQuery("SELECT test.intField, test.dateField FROM mongodb.test.test WHERE test.intField = 123"));
    }

    @Test
    public void simpleSelectWhereLike() {
        MongoClient client = new MongoClient("test", "test");
        System.out.println(client.executeQuery("SELECT test.intField, test.dateField FROM mongodb.test.test WHERE test.stringField LIKE '%world'"));
    }

    @Test
    public void simpleSelectCountAll() {
        MongoClient client = new MongoClient("test", "test");
        System.out.println(client.executeQuery("SELECT count(*) FROM mongodb.test.test WHERE test.intField = 123"));
    }

    @Test
    public void selectCountAndSumField() {
        MongoClient client = new MongoClient("test", "test");
        System.out.println(client.executeQuery("SELECT count(test.intField), sum(test.intField) FROM mongodb.test.test"));
    }

    @Test
    public void selectCountFieldWhere() {
        MongoClient client = new MongoClient("test", "test");
        System.out.println(client.executeQuery("SELECT count(test.intField) FROM mongodb.test.test WHERE test.intField = 123"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleSelectSumException() {
        MongoClient client = new MongoClient("test", "test");
        System.out.println(client.executeQuery("SELECT sum(test.intField), test FROM mongodb.test.test WHERE test.intField = 123"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleSelectGroupByException() {
        MongoClient client = new MongoClient("test", "test");
        System.out.println(client.executeQuery("SELECT test.intField, test.dateField FROM mongodb.test.test GROUP BY test.intField"));
    }

    @Test
    public void simpleSelectGroupByAgg() {
        MongoClient client = new MongoClient("test", "test");
        System.out.println(client.executeQuery("SELECT sum(test.intField) FROM mongodb.test.test GROUP BY test.intField"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleSelectGroupByError() {
        MongoClient client = new MongoClient("test", "test");
        System.out.println(client.executeQuery("SELECT sum(test.intField), test.dateField FROM mongodb.test.test GROUP BY test.intField"));
    }

    @Test
    public void simpleSelectGroupByHaving() {
        MongoClient client = new MongoClient("test", "test");
        System.out.println(client.executeQuery("SELECT sum(test.intField) FROM mongodb.test.test GROUP BY test.intField HAVING sum(test.intField) > 500"));
    }

    @Test
    public void simpleSelectGroupByOrderBy() {
        MongoClient client = new MongoClient("test", "test");
        System.out.println(client.executeQuery("SELECT test.intField FROM mongodb.test.test GROUP BY test.intField ORDER BY test.intField"));
    }

    @Test
    public void simpleSelectGroupByOrderByOrderBy() {
        MongoClient client = new MongoClient("test", "test");
        System.out.println(client.executeQuery("SELECT max(test.dateField) FROM mongodb.test.test GROUP BY test.dateField ORDER BY test.intField"));
    }

    @Test
    public void selectWithQualifiedName() {
        MongoClient client = new MongoClient("test", "test");
        System.out.println(client.executeQuery("SELECT test.intField FROM mongodb.test.test"));
    }

    @Test
    public void selectAll(){
        MongoClient client = new MongoClient("test", "test");
        System.out.println(client.executeQuery("SELECT * FROM mongodb.test.test"));
    }

    @After
    public void after() {
        mongoClient.close();
    }
}
