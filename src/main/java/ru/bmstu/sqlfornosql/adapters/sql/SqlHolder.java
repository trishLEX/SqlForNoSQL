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
        selectItemMap = new HashMap<>();
    }

    public static class SqlHolderBuilder {
        private SqlHolder holder;

        public SqlHolderBuilder() {
            this.holder = new SqlHolder();
        }

        public SqlHolderBuilder withDistinct(boolean isDistinct) {
            holder.isDistinct = isDistinct;
            return this;
        }

        public SqlHolderBuilder withCountAll(boolean isCountAll) {
            holder.isCountAll = isCountAll;
            return this;
        }

        public SqlHolderBuilder withFromItem(FromItem fromItem) {
            holder.fromItem = fromItem;

            if (fromItem instanceof Table) {
                Table table = (Table) fromItem;
                holder.database = new DatabaseName(table.getFullyQualifiedName());
            }

            return this;
        }

        public SqlHolderBuilder withLimit(long limit) {
            holder.limit = limit;
            return this;
        }

        public SqlHolderBuilder withWhere(Expression whereClause) {
            holder.whereClause = whereClause;
            return this;
        }

        public SqlHolderBuilder withSelectItems(List<SelectItem> selectItems) {
            holder.selectItems = selectItems;
            holder.selectItemsStrings = getSelectItemsStringFromSelectItems(selectItems);
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

        public SqlHolderBuilder withJoins(List<Join> joins) {
            holder.joins = joins;
            return this;
        }

        public SqlHolderBuilder withGroupBy(List<String> groupBys) {
            holder.groupBys = groupBys;
            return this;
        }

        public SqlHolderBuilder withHaving(Expression havingClause) {
            holder.havingClause = havingClause;
            return this;
        }

        public SqlHolderBuilder withOrderBy(List<OrderByElement> orderByElements) {
            holder.orderByElements = orderByElements;
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
            if (holder.fromItem != null) {
                fillVisibleColumnsAndTables(subSelects, tables, holder.fromItem);
            }

            for (Join join : holder.joins) {
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

            for (SelectItem column : holder.selectItems) {
                boolean existsInVisibleColumns = visibleColumns.contains(column);
                if (existsInVisibleColumns) {
                    SubSelect subSelect = columnToSubSelect.get(column);
                    if (holder.selectItemMap.containsKey(subSelect)) {
                        holder.selectItemMap.get(subSelect).add(column);
                    } else {
                        holder.selectItemMap.put(subSelect, Lists.newArrayList(column));
                    }
                }

                String columnStr = getStringFromSelectItem(column);
                String columnPrefix = columnStr.substring(0, columnStr.lastIndexOf('.'));
                int count = tables
                        .stream()
                        .map(table -> {
                            if (table.getFullyQualifiedName().endsWith(columnPrefix)) {
                                if (holder.selectItemMap.containsKey(table)) {
                                    holder.selectItemMap.get(table).add(column);
                                } else {
                                    holder.selectItemMap.put(table, Lists.newArrayList(column));
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

            return holder;
        }

        private void fillVisibleColumnsAndTables(List<SubSelect> visibleColumns, List<Table> tables,
                FromItem fromItem)
        {
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
    
    public List<String> getSelectIdents() {
        return selectItemsStrings.stream().map(this::getIdentFromItem).collect(Collectors.toList());
    }
    
    private String getIdentFromItem(String item) {
        if (item.startsWith("sum(")
                || item.startsWith("avg(")
                || item.startsWith("min(")
                || item.startsWith("max(")) {
            return item.substring(4, item.length() - 1);
        } else if (item.startsWith("count(")) {
            if (item.contains("*")){
                return "count";
            } else {
                return item.substring(6, item.length() - 1);
            }
        } else {
            return item;
        }
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
