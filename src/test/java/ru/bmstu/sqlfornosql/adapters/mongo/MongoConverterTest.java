package ru.bmstu.sqlfornosql.adapters.mongo;

import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.junit.Before;
import org.junit.Test;

public class MongoConverterTest {
    private MongoDatabase mongoDatabase;
    private MongoAdapter adapter;

    @Before
    public void before() {
        com.mongodb.MongoClient mongoClient = new com.mongodb.MongoClient();
        mongoDatabase = mongoClient.getDatabase("test");
        adapter = new MongoAdapter();
    }

    @Test
    public void simpleSelect() {
        MongoClient<BsonDocument> client = new MongoClient<>(mongoDatabase.getCollection("test", BsonDocument.class), BsonDocument.class);
        System.out.println(client.executeQuery(adapter.translate("SELECT sum(intField) FROM test")));
    }
}
