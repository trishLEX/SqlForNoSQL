package ru.bmstu.sqlfornosql.adapters.sql;

import com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import ru.bmstu.sqlfornosql.model.DatabaseName;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.*;
import java.util.stream.Collectors;

@ParametersAreNonnullByDefault
public class SqlHolder {
    private boolean isDistinct;
    private boolean isCountAll;

    @Nullable
    private FromItem fromItem;
    private long limit;

    @Nullable
    private Expression whereClause;
    private List<SelectItem> selectItems;
    private List<String> selectItemsStrings;
    private List<Join> joins;
    private List<String> groupBys;

    @Nullable
    private Expression havingClause;
    private List<OrderByElement> orderByElements;

    private Map<FromItem, List<SelectItem>> selectItemMap;

    @Nullable
    private DatabaseName database;

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

        if (fromItem instanceof Table) {
            Table table = (Table) fromItem;
            this.database = new DatabaseName(table.getFullyQualifiedName());
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
        this.selectItemsStrings = getSelectItemsStringFromSelectItems(selectItems);
        return this;
    }

    private List<String> getSelectItemsStringFromSelectItems(List<SelectItem> selectItems) {
        return selectItems.stream()
                .map(this::getStringFromSelectItem)
                .collect(Collectors.toList());
    }

    private String getStringFromSelectItem(SelectItem item) {
        Expression expression = ((SelectExpressionItem) item).getExpression();
        return SqlUtils.getStringValue(expression);
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

    //TODO method is not ready
    public SqlHolder build() {
//        List<String> usedDbs = new ArrayList<>();
//        if (fromItem instanceof Table) {
//            Table table = (Table) fromItem;
//            usedDbs.add(table.getFullyQualifiedName());
//        } else if (fromItem instanceof SubSelect) {
//            SubSelect subSelect = (SubSelect) fromItem;
//            PlainSelect select = ((PlainSelect) subSelect.getSelectBody());
//            usedDbs.addAll(getAllUsedDbs(select));
//        }
//
//        if (!joins.isEmpty()) {
//            joins
//                    .stream()
//                    .map(Join::getRightItem)
//                    .forEach(fromItem -> usedDbs.addAll(getTableOfFromItem(fromItem)));
//        }

        List<SubSelect> subSelects = new ArrayList<>();
        List<Table> tables = new ArrayList<>();
        fillVisibleColumnsAndTables(subSelects, tables, fromItem);

        for (Join join : joins) {
            FromItem fromItem = join.getRightItem();
            fillVisibleColumnsAndTables(subSelects, tables, fromItem);
        }

        Map<SelectItem, SubSelect> columnToSubSelect = new HashMap<>();

        List<SelectItem> visibleColumns = subSelects.stream()
                .flatMap(subSelect -> {
                    PlainSelect body = (PlainSelect) subSelect.getSelectBody();
                    List<SelectItem> selectItemsStr = body.getSelectItems();
                    for (SelectItem item : selectItemsStr) {
                        if (columnToSubSelect.containsKey(item)) {
                            throw new IllegalStateException("Column name '" + item + "' is clashed");
                        } else {
                            columnToSubSelect.put(item, subSelect);
                        }
                    }

                    return selectItemsStr.stream();
                })
                .collect(Collectors.toList());

        selectItemMap = new HashMap<>();

        for (SelectItem column : selectItems) {
            boolean existsInVisibleColumns = visibleColumns.contains(column);
            if (existsInVisibleColumns) {
                SubSelect subSelect = columnToSubSelect.get(column);
                if (selectItemMap.containsKey(subSelect)) {
                    selectItemMap.get(subSelect).add(column);
                } else {
                    selectItemMap.put(subSelect, Lists.newArrayList(column));
                }
            }

            String columnStr = getStringFromSelectItem(column);
            String columnPrefix = columnStr.substring(0, columnStr.lastIndexOf('.'));
            int count = tables
                    .stream()
                    .map(table -> {
                        if (table.getFullyQualifiedName().endsWith(columnPrefix)) {
                            if (selectItemMap.containsKey(table)) {
                                selectItemMap.get(table).add(column);
                            } else {
                                selectItemMap.put(table, Lists.newArrayList(column));
                            }
                            return 1;
                        } else {
                            return 0;
                        }
                    })
                    .reduce(0, (x, y) -> x + y);
            if (count > 1) {
                throw new IllegalArgumentException("Column '" + column + "' clashes");
            }

            if (existsInVisibleColumns == (count == 1)) {
                throw new IllegalArgumentException("Column '" + column + "' clashes");
            }
        }

        return this;
    }

    private void fillVisibleColumnsAndTables(List<SubSelect> visibleColumns, List<Table> tables, FromItem fromItem) {
        if (fromItem instanceof SubSelect) {
            SubSelect select = (SubSelect) fromItem;
            visibleColumns.add(select);
        } else if (fromItem instanceof Table) {
            Table table = (Table) fromItem;
            tables.add(table);
        } else {
            throw new IllegalStateException("Can't determine type of fromItem: " + fromItem);
        }
    }

    private List<String> getAllUsedDbs(PlainSelect select) {
        List<String> res = new ArrayList<>();

        if (!select.getJoins().isEmpty()) {
            select.getJoins()
                    .stream()
                    .map(Join::getRightItem)
                    .forEach(fromItem -> res.addAll(getTableOfFromItem(fromItem)));
        }

        res.addAll(getTableOfFromItem(select.getFromItem()));
        return res;
    }

    private List<String> getTableOfFromItem(FromItem fromItem) {
        if (fromItem instanceof Table) {
            Table table = (Table) fromItem;
            return Collections.singletonList(table.getFullyQualifiedName());
        } else if (fromItem instanceof SubSelect) {
            SubSelect subSelect = (SubSelect) fromItem;
            PlainSelect plainSelect = (PlainSelect) subSelect.getSelectBody();
            return getAllUsedDbs(plainSelect);
        } else {
            throw new IllegalArgumentException("Illegal type of fromItem");
        }
    }

    @Nullable
    public DatabaseName getDatabase() {
        return database;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public boolean isCountAll() {
        return isCountAll;
    }

    @Nullable
    public FromItem getFromItem() {
        return fromItem;
    }

    public long getLimit() {
        return limit;
    }

    @Nullable
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

    @Nullable
    public Expression getHavingClause() {
        return havingClause;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public Map<FromItem, List<SelectItem>> getSelectItemMap() {
        return selectItemMap;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");

        if (isDistinct) {
            sb.append("DISTINCT ");
        }

        sb.append(String.join(", ", selectItemsStrings));

        if (fromItem != null) {
            sb.append(" FROM ");
            sb.append(((Table) fromItem).getName());
        }

        for (Join join : joins) {
            sb.append(join);
        }

        if (whereClause != null) {
            sb.append(" WHERE ");
            sb.append(whereClause);
        }

        if (!groupBys.isEmpty()) {
            sb.append(" GROUP BY ");
            sb.append(String.join(", ", groupBys));

            if (havingClause != null) {
                sb.append(" HAVING ");
                sb.append(havingClause);
            }
        }

        if (!orderByElements.isEmpty()) {
            sb.append(" ORDER BY ");
            sb.append(orderByElements
                    .stream()
                    .map(OrderByElement::toString)
                    .collect(Collectors.joining(", "))
            );
        }

        return sb.toString();
    }
}
