package ru.bmstu.sqlfornosql.adapters.mongo;

import com.google.common.collect.Iterables;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.Document;
import ru.bmstu.sqlfornosql.model.Table;

import java.util.ArrayList;
import java.util.List;

public class MongoClient<T extends BsonDocument> {
    private static final Logger logger = LogManager.getLogger(MongoClient.class);

    private MongoCollection<T> collection;

    public MongoClient(MongoCollection<T> mongoCollection) {
        this.collection = mongoCollection;
    }

    public Table executeQuery(MongoHolder query) {
        //TODO возможно стоит сделать его синглтоном
        MongoMapper mapper = new MongoMapper();
        //TODO distinct больше не ставится в true, вместо него выполянется group by => данная ветка бесполезна
        if (query.isDistinct()) {
            logger.debug("EXECUTING DISTINCT QUERY: " + query);
            throw new UnsupportedOperationException();
//            return collection.distinct(getDistinctFieldName(query), query.getQuery(), clazz);
        } else if (query.isCountAll()) {
            return mapper.mapCountAll(collection.countDocuments(query.getQuery()), query);
        } else if (query.getGroupBys().size() > 0) {
            logger.debug("EXECUTING GROUP BY QUERY: " + query);
            List<Document> documents = new ArrayList<>();

            if (query.getQuery() != null && query.getQuery().size() > 0) {
                documents.add(new Document("$match", query.getQuery()));
            }

            documents.add(new Document("$group", query.getProjection()));

            if (query.getSort() != null && query.getSort().size() > 0) {
                documents.add(new Document("$sort", query.getSort()));
            }

            if (query.getLimit() != -1) {
                documents.add(new Document("$limit", query.getLimit()));
            }

            return mapper.mapGroupBy(collection.aggregate(documents, BsonDocument.class), query);
        } else {
            //TODO
            logger.debug("EXECUTING FIND QUERY: " + query);
            FindIterable<BsonDocument> findIterable = collection.find(query.getQuery(), BsonDocument.class).projection(query.getProjection());
            if (query.getSort() != null && query.getSort().size() > 0) {
                findIterable.sort(query.getSort());
            }

            if (query.getLimit() != -1) {
                findIterable.limit((int) query.getLimit());
            }

            return mapper.mapFind(findIterable, query);
        }
    }

    private String getDistinctFieldName(MongoHolder query) {
        return Iterables.get(query.getProjection().keySet(),0);
    }
}
