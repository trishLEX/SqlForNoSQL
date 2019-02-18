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
        System.out.println(client.executeQuery(adapter.translate("SELECT sum(intField) FROM test GROUP BY intField")));
    }

    @After
    public void after() {
        mongoClient.close();
    }
}
