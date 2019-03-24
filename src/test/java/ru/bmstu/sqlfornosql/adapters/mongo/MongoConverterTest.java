package ru.bmstu.sqlfornosql.adapters.mongo;

import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MongoConverterTest {
    private MongoDatabase mongoDatabase;
    private MongoAdapter adapter;
    private com.mongodb.MongoClient mongoClient;

    @Before
    public void before() {
        mongoClient = new com.mongodb.MongoClient();
        mongoDatabase = mongoClient.getDatabase("test");
        adapter = new MongoAdapter();
    }

    @Test
    public void simpleSelect() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class));
        System.out.println(client.executeQuery(adapter.translate("SELECT test.intField, test.dateField FROM mongodb.test.test")));
    }

    @Test
    public void simpleSelectWhere() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class));
        System.out.println(client.executeQuery(adapter.translate("SELECT test.intField, test.dateField FROM mongodb.test.test WHERE test.intField = 123")));
    }

    @Test
    public void simpleSelectWhereLike() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class));
        System.out.println(client.executeQuery(adapter.translate("SELECT test.intField, test.dateField FROM mongodb.test.test WHERE test.stringField LIKE '%world'")));
    }

    @Test
    public void simpleSelectCountAll() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class));
        System.out.println(client.executeQuery(adapter.translate("SELECT count(*) FROM mongodb.test.test WHERE test.intField = 123")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleSelectSumException() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class));
        System.out.println(client.executeQuery(adapter.translate("SELECT sum(test.intField), test FROM mongodb.test.test WHERE test.intField = 123")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleSelectGroupByException() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class));
        System.out.println(client.executeQuery(adapter.translate("SELECT test.intField, test.dateField FROM mongodb.test.test GROUP BY test.intField")));
    }

    @Test
    public void simpleSelectGroupByAgg() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class));
        System.out.println(client.executeQuery(adapter.translate("SELECT sum(test.intField) FROM mongodb.test.test GROUP BY test.intField")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleSelectGroupByError() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class));
        System.out.println(client.executeQuery(adapter.translate("SELECT sum(test.intField), test.dateField FROM mongodb.test.test GROUP BY test.intField")));
    }

    @Test
    public void simpleSelectGroupByHaving() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class));
        System.out.println(client.executeQuery(adapter.translate("SELECT sum(test.intField) FROM mongodb.test.test GROUP BY test.intField HAVING test.intField < 500")));
    }

    @Test
    public void simpleSelectGroupByOrderBy() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class));
        System.out.println(client.executeQuery(adapter.translate("SELECT test.intField FROM mongodb.test.test GROUP BY test.intField ORDER BY test.intField")));
    }

    @Test
    public void simpleSelectGroupByOrderByOrderBy() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class));
        System.out.println(client.executeQuery(adapter.translate("SELECT max(test.dateField) FROM mongodb.test.test GROUP BY test.dateField ORDER BY test.intField")));
    }

    @Test
    public void selectWithQualifiedName() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class));
        System.out.println(client.executeQuery(adapter.translate("SELECT test.intField FROM mongodb.test.test")));
    }

    @After
    public void after() {
        mongoClient.close();
    }
}
