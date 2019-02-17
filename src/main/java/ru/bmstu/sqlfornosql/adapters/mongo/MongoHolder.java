package ru.bmstu.sqlfornosql.adapters.mongo;

import one.util.streamex.StreamEx;
import org.bson.Document;
import ru.bmstu.sqlfornosql.SqlUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MongoHolder {
    public static final String MONGO_ID = "_id";

    private String collection;
    private Document query;
    private Document projection;
    private Document sort;
    private boolean distinct;
    private boolean countAll;
    private List<String> groupBys;
    private boolean hasAggregateFunctions;
    private long limit;

    private MongoHolder() {
        query = new Document();
        projection = new Document();
        sort = new Document();
        distinct = false;
        countAll = false;
        groupBys = new ArrayList<>();
        hasAggregateFunctions = false;
        limit = -1;
    }

    public MongoHolder(String collection) {
        this();
        this.collection = collection;
    }

    public static MongoHolder createBySql(SqlHolder sqlHolder) {
        MongoHolder mongoHolder = new MongoHolder(sqlHolder.getTable().toString());

        if (sqlHolder.isDistinct()) {
            //if (sqlHolder.getSelectItems().size() > 1 || !sqlHolder.getGroupBys().isEmpty()) {
                mongoHolder.groupBys = StreamEx.of(sqlHolder.getSelectItems().stream())
                        .map(Object::toString)
                        .append(sqlHolder.getGroupBys().stream())
                        .collect(Collectors.toList());

                mongoHolder.projection = MongoUtils.createProjectionsFromSelectItems(sqlHolder.getSelectItems(), mongoHolder);
//            } else {
//                Document mongoProjection = new Document();
//                mongoProjection.put(sqlHolder.getSelectItems().get(0).toString(), 1);
//                mongoHolder.projection = mongoProjection;
//                mongoHolder.distinct = true;
//            }
        } else if (!sqlHolder.getGroupBys().isEmpty()) {
            mongoHolder.groupBys = sqlHolder.getGroupBys();
            mongoHolder.projection = MongoUtils.createProjectionsFromSelectItems(sqlHolder.getSelectItems(), mongoHolder);
        } else if (sqlHolder.isCountAll()) {
            mongoHolder.countAll = true;
        } else if (!SqlUtils.isSelectAll(sqlHolder.getSelectItems())) {
            Document mongoProjection = new Document();
            mongoProjection.put(MONGO_ID, 0);

            sqlHolder.getSelectItems().forEach(item -> mongoProjection.put(item.toString(), 1));

            mongoHolder.projection = mongoProjection;
        }

        if (sqlHolder.getOrderByElements() != null && !sqlHolder.getOrderByElements().isEmpty()) {
            mongoHolder.sort = MongoUtils.createSortInfoFromOrderByElements(sqlHolder.getOrderByElements(), mongoHolder);
        }

        if (sqlHolder.getWhereClause() != null) {
            mongoHolder.query = (Document) WhereClauseParser.parseExpression(
                    new Document(), sqlHolder.getWhereClause(), null
            );
            if (sqlHolder.getGroupBys().size() > 0 && sqlHolder.getHavingClause() != null) {
                mongoHolder.query.putAll((Document) WhereClauseParser.parseExpression(
                        new Document(), sqlHolder.getHavingClause(), null
                ));
            }
        }

        mongoHolder.limit = sqlHolder.getLimit();

        mongoHolder.validate();
        return mongoHolder;
    }

    public String getCollection() {
        return collection;
    }

    public Document getQuery() {
        return query;
    }

    public Document getProjection() {
        return projection;
    }

    public Document getSort() {
        return sort;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public boolean isCountAll() {
        return countAll;
    }

    public List<String> getGroupBys() {
        return groupBys;
    }

    public long getLimit() {
        return limit;
    }

    public boolean hasAggregateFunctions() {
        return hasAggregateFunctions;
    }

    public void setHasAggregateFunctions(boolean hasAggregateFunctions) {
        this.hasAggregateFunctions = hasAggregateFunctions;
    }

    @Override
    public String toString() {
        return "MongoHolder{" +
                "collection='" + collection + '\'' +
                ", query=" + query +
                ", projection=" + projection +
                ", sort=" + sort +
                ", distinct=" + distinct +
                ", countAll=" + countAll +
                ", groupBys=" + groupBys +
                ", limit=" + limit +
                '}';
    }

    private void validate() {
        if (!groupBys.isEmpty()) {
            if (projection.get(MONGO_ID) instanceof Document) {
                for (String column : projection.get(MONGO_ID, Document.class).keySet()) {
                    if (!groupBys.contains(column)) {
                        throw new IllegalArgumentException("Column: " + column + " must be in group by or aggregation function");
                    }
                }
            } else {
                String column = MongoUtils.normalizeColumnName(projection.getString(MONGO_ID));
                if (!groupBys.contains(column)) {
                    throw new IllegalArgumentException("Column: " + column + " must be in group by or aggregation function");
                }
            }
        }
    }
}
