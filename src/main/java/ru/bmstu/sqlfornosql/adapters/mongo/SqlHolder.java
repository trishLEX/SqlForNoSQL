package ru.bmstu.sqlfornosql.adapters.mongo;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

public class SqlHolder {
    private boolean isDistinct;
    private boolean isCountAll;
    private FromItem table;
    private long limit;
    private Expression whereClause;
    private List<SelectItem> selectItems;
    private List<Join> joins;
    private List<String> groupBys;
    private Expression havingClause;
    private List<OrderByElement> orderByElements;

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

    public SqlHolder withTable(FromItem table) {
        this.table = table;
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

    public boolean isDistinct() {
        return isDistinct;
    }

    public boolean isCountAll() {
        return isCountAll;
    }

    public FromItem getTable() {
        return table;
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
