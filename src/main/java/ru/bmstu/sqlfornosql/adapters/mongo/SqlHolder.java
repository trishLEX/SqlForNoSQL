package ru.bmstu.sqlfornosql.adapters.mongo;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import ru.bmstu.sqlfornosql.DbType;
import ru.bmstu.sqlfornosql.SqlUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SqlHolder {
    private boolean isDistinct;
    private boolean isCountAll;
    private FromItem fromItem;
    private long limit;
    private Expression whereClause;
    private List<SelectItem> selectItems;
    private List<String> selectItemsStrings;
    private List<Join> joins;
    private List<String> groupBys;
    private Expression havingClause;
    private List<OrderByElement> orderByElements;
    private DbType dbType;
    private String database;
    private String schema;
    private String table;

    public SqlHolder() {
        isDistinct = false;
        isCountAll = false;
        limit = -1;
        selectItems = new ArrayList<>();
        joins = new ArrayList<>();
        groupBys = new ArrayList<>();
        orderByElements = new ArrayList<>();
    }

    public SqlHolder withDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
        return this;
    }

    public SqlHolder withCountAll(boolean isCountAll) {
        this.isCountAll = isCountAll;
        return this;
    }

    public SqlHolder withFromItem(FromItem fromItem) {
        this.fromItem = fromItem;

        Table table = (Table) fromItem;
        String fullQualifiedName = table.getFullyQualifiedName();
        String[] parts = fullQualifiedName.split("\\.");
        if (parts.length < 3) {
            throw new IllegalArgumentException("DbType, db and table must be specified at least");
        }

        if (parts.length == 3) {
            this.dbType = DbType.valueOf(parts[0].toUpperCase());
            this.database = parts[1];
            this.table = parts[2];
        } else if (parts.length == 4) {
            this.dbType = DbType.valueOf(parts[0].toUpperCase());
            this.database = parts[1];
            this.schema = parts[2];
            this.table = parts[3];
        } else {
            throw new IllegalArgumentException("Name: " + fullQualifiedName + " is malformed");
        }

        return this;
    }

    public SqlHolder withLimit(long limit) {
        this.limit = limit;
        return this;
    }

    public SqlHolder withWhere(Expression whereClause) {
        this.whereClause = whereClause;
        return this;
    }

    public SqlHolder withSelectItems(List<SelectItem> selectItems) {
        this.selectItems = selectItems;
        this.selectItemsStrings = this.selectItems.stream()
                .map(item -> {
                    Expression expression = ((SelectExpressionItem) item).getExpression();
                    return SqlUtils.getStringValue(expression);
                })
                .collect(Collectors.toList());
        return this;
    }

    public SqlHolder withJoins(List<Join> joins) {
        this.joins = joins;
        return this;
    }

    public SqlHolder withGroupBy(List<String> groupBys) {
        this.groupBys = groupBys;
        return this;
    }

    public SqlHolder withHaving(Expression havingClause) {
        this.havingClause = havingClause;
        return this;
    }

    public SqlHolder withOrderBy(List<OrderByElement> orderByElements) {
        this.orderByElements = orderByElements;
        return this;
    }

    public SqlHolder withDbType(DbType dbType) {
        this.dbType = dbType;
        return this;
    }

    public SqlHolder withDatabase(String database) {
        this.database = database;
        return this;
    }

    public SqlHolder withSchema(String schema) {
        this.schema = schema;
        return this;
    }

    public SqlHolder withTable(String table) {
        this.table = table;
        return this;
    }

    public DbType getDbType() {
        return dbType;
    }

    public String getDatabase() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public boolean isCountAll() {
        return isCountAll;
    }

    public FromItem getFromItem() {
        return fromItem;
    }

    public long getLimit() {
        return limit;
    }

    public Expression getWhereClause() {
        return whereClause;
    }

    public List<SelectItem> getSelectItems() {
        return selectItems;
    }

    public List<String> getSelectItemsStrings() {
        return selectItemsStrings;
    }

    public List<Join> getJoins() {
        return joins;
    }

    public List<String> getGroupBys() {
        return groupBys;
    }

    public Expression getHavingClause() {
        return havingClause;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }
}
