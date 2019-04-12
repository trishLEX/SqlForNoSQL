package ru.bmstu.sqlfornosql.adapters.mongo;

import one.util.streamex.StreamEx;
import org.bson.Document;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.adapters.sql.SqlUtils;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectField;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectFieldExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class MongoHolder {
    public static final String MONGO_ID = "_id";

    private String table;
    private String database;

    private Document query;
    private Document projection;
    private Document sort;
    private boolean distinct;
    private boolean countAll;
    private boolean selectAll;
    private List<String> groupBys;
    private List<SelectField> selectFields;
    private Map<String, SelectField> columnNameToSelectField;
    private boolean hasAggregateFunctions;
    private long limit;
    private long offset;

    private MongoHolder() {
        query = new Document();
        projection = new Document();
        sort = new Document();
        distinct = false;
        countAll = false;
        selectAll = false;
        groupBys = new ArrayList<>();
        hasAggregateFunctions = false;
        limit = -1;
        offset = -1;
    }

    public MongoHolder(String database, String table) {
        this();
        this.database = database;
        this.table = table;
    }

    public static MongoHolder createBySql(SqlHolder sqlHolder) {
        MongoHolder mongoHolder = new MongoHolder(sqlHolder.getDatabase().getDatabaseName(), sqlHolder.getDatabase().getTable());

        mongoHolder.selectAll = sqlHolder.isSelectAll();
        mongoHolder.countAll = sqlHolder.isCountAll();

        mongoHolder.selectFields = sqlHolder.getSelectFields();
        mongoHolder.columnNameToSelectField = sqlHolder.getColumnNameToSelectField();

        if (sqlHolder.isDistinct()) {
                mongoHolder.groupBys = StreamEx.of(sqlHolder.getSelectFields())
                        .map(SelectField::getNonQualifiedContent)
                        .append(sqlHolder.getGroupBys().stream())
                        .map(name -> name.contains(".") ? name.substring(name.lastIndexOf('.') + 1) : name)
                        .collect(Collectors.toList());

                mongoHolder.projection = MongoUtils.createProjectionsFromSelectItems(sqlHolder.getSelectItems(), mongoHolder);
        } else if (!sqlHolder.getGroupBys().isEmpty() || sqlHolder.getSelectFields().stream().allMatch(field -> field instanceof SelectFieldExpression)) {
            mongoHolder.groupBys = sqlHolder.getGroupBys()
                    .stream()
                    .map(name -> name.contains(".") ? name.substring(name.lastIndexOf('.') + 1) : name)
                    .collect(Collectors.toList());
            mongoHolder.projection = MongoUtils.createProjectionsFromSelectItems(sqlHolder.getSelectItems(), mongoHolder);
        } else if (sqlHolder.isCountAll()) {
            mongoHolder.countAll = true;
        } else if (!SqlUtils.isSelectAll(sqlHolder.getSelectItems())) {
            Document mongoProjection = new Document();
            mongoProjection.put(MONGO_ID, 0);

            sqlHolder.getSelectFields().forEach(item -> {
                mongoProjection.put(item.getNonQualifiedContent(), 1);
            });

            mongoHolder.projection = mongoProjection;
        }

        if (sqlHolder.getOrderByElements() != null && !sqlHolder.getOrderByElements().isEmpty()) {
            mongoHolder.sort = MongoUtils.createSortInfoFromOrderByElements(sqlHolder.getOrderByElements(), mongoHolder);
        }

        if (sqlHolder.getWhereClause() != null) {
            mongoHolder.query = (Document) WhereClauseParser.parseExpression(
                    new Document(), sqlHolder.getWhereClause(), null
            );
        }

        if (!sqlHolder.getGroupBys().isEmpty() && sqlHolder.getHavingClause() != null) {
            mongoHolder.query.putAll((Document) WhereClauseParser.parseExpression(
                    new Document(), sqlHolder.getHavingClause(), null
            ));
        }

        mongoHolder.limit = sqlHolder.getLimit();
        mongoHolder.offset = sqlHolder.getOffset();

        mongoHolder.validate();
        return mongoHolder;
    }

    public String getTable() {
        return table;
    }

    public String getDatabase() {
        return database;
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

    public boolean isSelectAll() {
        return selectAll;
    }

    public List<String> getGroupBys() {
        return groupBys;
    }

    public List<SelectField> getSelectFields() {
        return selectFields;
    }

    public long getLimit() {
        return limit;
    }

    public long getOffset() {
        return offset;
    }

    public boolean hasAggregateFunctions() {
        return hasAggregateFunctions;
    }

    public void setHasAggregateFunctions(boolean hasAggregateFunctions) {
        this.hasAggregateFunctions = hasAggregateFunctions;
    }

   public SelectField getByNonQualifiedName(String name) {
        for (SelectField selectField : selectFields) {
            if (selectField.getNonQualifiedContent().equals(name)) {
                return selectField;
            }
        }

        throw new NoSuchElementException("No element with name: " + name + " in select fields");
   }

    @Override
    public String toString() {
        return "MongoHolder{" +
                "db='" + database + '\'' +
                ", table='" + table + '\'' +
                ", query=" + query +
                ", projection=" + projection +
                ", sort=" + sort +
                ", distinct=" + distinct +
                ", countAll=" + countAll +
                ", groupBys=" + groupBys +
                ", limit=" + limit +
                ", offset=" + offset +
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

        boolean hasAggregateFunctions = selectFields.stream().anyMatch(selectField -> selectField instanceof SelectFieldExpression);

        if (hasAggregateFunctions) {
            for (SelectField selectField : selectFields) {
                if (!groupBys.contains(selectField.getQualifiedContent()) && !(selectField instanceof SelectFieldExpression)) {
                    throw new IllegalArgumentException("Column: " + selectField + " must be in group by or aggregation function");
                }
            }
        }
    }

    private String extractFieldFromFunction(String field) {
        if (field.startsWith("sum")
                || field.startsWith("min")
                || field.startsWith("max")
                || field.startsWith("avg")) {
            return field.substring(3, field.length() - 1);
        } else {
            return field.substring(4, field.length() - 1);
        }
    }

    private boolean isFieldWithAggregationFunction(String field) {
        return field.startsWith("sum")
                || field.startsWith("min")
                || field.startsWith("max")
                || field.startsWith("count")
                || field.startsWith("avg");
    }
}
