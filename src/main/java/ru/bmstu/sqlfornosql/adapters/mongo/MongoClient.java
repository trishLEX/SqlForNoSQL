package ru.bmstu.sqlfornosql.adapters.mongo;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.Document;
import ru.bmstu.sqlfornosql.adapters.AbstractClient;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.adapters.sql.SqlUtils;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectFieldExpression;
import ru.bmstu.sqlfornosql.model.Table;
import ru.bmstu.sqlfornosql.model.TableIterator;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class MongoClient extends AbstractClient {
    private static final Logger logger = LogManager.getLogger(MongoClient.class);
    private static final MongoAdapter ADAPTER = new MongoAdapter();
    private static final MongoMapper MAPPER = new MongoMapper();

    private MongoCollection<BsonDocument> collection;
    private com.mongodb.MongoClient client;

    public MongoClient(String dbName, String table) {
        client = new com.mongodb.MongoClient();
        MongoDatabase database = client.getDatabase(dbName);
        collection = database.getCollection(table, BsonDocument.class);
    }

    @VisibleForTesting
    TableIterator executeQuery(String query) {
        return executeQuery(SqlUtils.fillSqlMeta(query));
    }

    public TableIterator executeQuery(SqlHolder holder) {
        return new TableIterator() {
            private MongoHolder query = ADAPTER.translate(holder);
            private Iterator<BsonDocument> result = executeQuery(query).iterator();

            @Nonnull
            @Override
            public Iterator<Table> iterator() {
                return executeQuery(holder);
            }

            @Override
            public Table next() {
                if (hasNext()) {
                    Table table;
                    if (query.isCountAll()) {
                        lastBatchSize = 0;
                        table = MAPPER.mapCountAll(countAll(query), query);
                    } else {
                        table = MAPPER.map(result, query);
                        lastBatchSize = table.size();
                    }

                    if (!hasNext()) {
                        afterAll.forEach(Runnable::run);
                    }

                    return table;
                }

                throw new NoSuchElementException("There are no more elements");
            }
        };
    }

    private MongoIterable<BsonDocument> executeQuery(MongoHolder query) {
        if (!query.getGroupBys().isEmpty() ||
                query.getSelectFields().stream().allMatch(field -> field instanceof SelectFieldExpression)
        ) {
            return groupBy(query);
        } else {
            return find(query);
        }
    }

    private long countAll(MongoHolder query) {
        logger.debug("EXECUTING COUNT ALL QUERY: " + query);
        return collection.countDocuments(query.getQuery());
    }

    private MongoIterable<BsonDocument> groupBy(MongoHolder query) {
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

        return collection.aggregate(documents, BsonDocument.class).batchSize((int) TableIterator.BATCH_SIZE);
    }

    private FindIterable<BsonDocument> find(MongoHolder query) {
        logger.debug("EXECUTING FIND QUERY: " + query);
        FindIterable<BsonDocument> findIterable = collection.find(query.getQuery(), BsonDocument.class).projection(query.getProjection()).batchSize((int) TableIterator.BATCH_SIZE);
        if (query.getSort() != null && query.getSort().size() > 0) {
            findIterable.sort(query.getSort());
        }

        if (query.getLimit() != -1) {
            findIterable.limit((int) query.getLimit());
        }

        if (query.getOffset() != -1) {
            findIterable.skip((int) query.getOffset());
        }

        return findIterable;
    }

    @Override
    public void close() {
        client.close();
    }
}
