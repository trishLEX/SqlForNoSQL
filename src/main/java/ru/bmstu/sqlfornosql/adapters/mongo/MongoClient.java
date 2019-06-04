package ru.bmstu.sqlfornosql.adapters.mongo;

import com.google.common.collect.Iterables;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.Document;
import ru.bmstu.sqlfornosql.adapters.AbstractClient;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.adapters.sql.SqlUtils;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectFieldExpression;
import ru.bmstu.sqlfornosql.model.Table;

import java.util.ArrayList;
import java.util.List;

public class MongoClient extends AbstractClient {
    private static final Logger logger = LogManager.getLogger(MongoClient.class);
    private static final MongoAdapter ADAPTER = new MongoAdapter();

    private MongoCollection<BsonDocument> collection;
    private com.mongodb.MongoClient client;

    public MongoClient(String dbName, String table) {
        client = new com.mongodb.MongoClient();
        MongoDatabase database = client.getDatabase(dbName);
        collection = database.getCollection(table, BsonDocument.class);
    }

    public Table executeQuery(String query) {
        return executeQuery(SqlUtils.fillSqlMeta(query));
    }

    public Table executeQuery(SqlHolder holder) {
        MongoHolder query = ADAPTER.translate(holder);
        //TODO возможно стоит сделать его синглтоном
        MongoMapper mapper = new MongoMapper();
        if (query.isDistinct()) {
            logger.debug("EXECUTING DISTINCT QUERY: " + query);
            throw new UnsupportedOperationException();
        } else if (query.isCountAll()) {
            logger.debug("EXECUTING COUNT ALL QUERY: " + query);
            return mapper.mapCountAll(collection.countDocuments(query.getQuery()), query);
        } else if (query.getGroupBys().size() > 0 ||
                query.getSelectFields().stream().allMatch(field -> field instanceof SelectFieldExpression)
        ) {
            logger.debug("EXECUTING GROUP BY QUERY: " + query);
            List<Document> documents = new ArrayList<>();

            if (query.getQuery() != null && query.getQuery().size() > 0) {
                documents.add(new Document("$match", query.getQuery()));
            }

            documents.add(new Document("$group", query.getProjection()));

            if (query.getQueryAfter() != null && query.getQueryAfter().size() > 0) {
                documents.add(new Document("$match", query.getQueryAfter()));
            }

            if (query.getSort() != null && query.getSort().size() > 0) {
                documents.add(new Document("$sort", query.getSort()));
            }

            if (query.getLimit() != -1) {
                documents.add(new Document("$limit", query.getLimit()));
            }

            if (query.getOffset() != -1) {
                documents.add(new Document("$skip", query.getOffset()));
            }

            return mapper.mapGroupBy(collection.aggregate(documents, BsonDocument.class), query);
        } else {
            logger.debug("EXECUTING FIND QUERY: " + query);
            FindIterable<BsonDocument> findIterable = collection.find(query.getQuery(), BsonDocument.class).projection(query.getProjection());
            if (query.getSort() != null && query.getSort().size() > 0) {
                findIterable.sort(query.getSort());
            }

            if (query.getLimit() != -1) {
                findIterable.limit((int) query.getLimit());
            }

            if (query.getOffset() != -1) {
                findIterable.skip((int) query.getOffset());
            }

            return mapper.mapFind(findIterable, query);
        }
    }

//    @Override
//    public void open() {
//        client = new com.mongodb.MongoClient();
//        MongoDatabase database = client.getDatabase(dbName);
//        collection = database.getCollection(table, BsonDocument.class);
//    }

    @Override
    public void close() {
        client.close();
    }

    private String getDistinctFieldName(MongoHolder query) {
        return Iterables.get(query.getProjection().keySet(),0);
    }
}
