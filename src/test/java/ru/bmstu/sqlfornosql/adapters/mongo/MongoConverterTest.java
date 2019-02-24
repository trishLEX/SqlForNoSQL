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
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class), BsonDocument.class);
        System.out.println(client.executeQuery(adapter.translate("SELECT intField, dateField FROM mongodb.test.test")));
    }

    @Test
    public void simpleSelectWhere() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class), BsonDocument.class);
        System.out.println(client.executeQuery(adapter.translate("SELECT intField, dateField FROM test WHERE intField = 123")));
    }

    @Test
    public void simpleSelectWhereLike() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class), BsonDocument.class);
        System.out.println(client.executeQuery(adapter.translate("SELECT intField, dateField FROM test WHERE stringField LIKE '%world'")));
    }

    @Test
    public void simpleSelectCountAll() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class), BsonDocument.class);
        System.out.println(client.executeQuery(adapter.translate("SELECT count(*) FROM test WHERE intField = 123")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleSelectSum() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class), BsonDocument.class);
        System.out.println(client.executeQuery(adapter.translate("SELECT sum(intField), dateField FROM test WHERE intField = 123")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleSelectGroupBy() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class), BsonDocument.class);
        System.out.println(client.executeQuery(adapter.translate("SELECT intField, dateField FROM test GROUP BY intField")));
    }

    @Test
    public void simpleSelectGroupByAgg() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class), BsonDocument.class);
        System.out.println(client.executeQuery(adapter.translate("SELECT sum(intField) FROM test GROUP BY intField")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void simpleSelectGroupByError() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class), BsonDocument.class);
        System.out.println(client.executeQuery(adapter.translate("SELECT sum(intField), dateField FROM test GROUP BY intField")));
    }

    @Test
    public void simpleSelectGroupByHaving() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class), BsonDocument.class);
        System.out.println(client.executeQuery(adapter.translate("SELECT sum(intField) FROM test GROUP BY intField HAVING intField < 500")));
    }

    @Test
    public void simpleSelectGroupByOrderBy() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class), BsonDocument.class);
        System.out.println(client.executeQuery(adapter.translate("SELECT intField FROM test GROUP BY intField ORDER BY intField")));
    }

    @Test
    public void simpleSelectGroupByOrderByOrderBy() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class), BsonDocument.class);
        System.out.println(client.executeQuery(adapter.translate("SELECT max(dateField) FROM test GROUP BY dateField ORDER BY intField")));
    }

    @After
    public void after() {
        mongoClient.close();
    }
}
