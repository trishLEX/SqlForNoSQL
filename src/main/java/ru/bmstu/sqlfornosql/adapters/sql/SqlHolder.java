package ru.bmstu.sqlfornosql.adapters.sql;

import com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import ru.bmstu.sqlfornosql.adapters.mongo.MongoUtils;
import ru.bmstu.sqlfornosql.model.DatabaseName;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.*;
import java.util.stream.Collectors;

@ParametersAreNonnullByDefault
public class SqlHolder {
    private boolean isDistinct;
    private boolean isCountAll;
    private boolean isSelectAll;

    @Nullable
    private FromItem fromItem;

    private long limit;
    private long offset;

    @Nullable
    private Expression whereClause;
    private List<SelectItem> selectItems;
    private List<String> selectItemsStrings;
    private List<String> additionalSelectItemsStrings;
    private List<Join> joins;
    private List<String> groupBys;

    @Nullable
    private Expression havingClause;
    private List<OrderByElement> orderByElements;

    private Map<FromItem, List<SelectItem>> selectItemMap;
    private Map<String, String> qualifiedNamesMap;

    private DatabaseName database;

    public SqlHolder() {
        isDistinct = false;
        isCountAll = false;
        isSelectAll = false;
        limit = -1;
        offset = -1;
        selectItems = new ArrayList<>();
        additionalSelectItemsStrings = new ArrayList<>();
        joins = new ArrayList<>();
        groupBys = new ArrayList<>();
        orderByElements = new ArrayList<>();
        selectItemMap = new LinkedHashMap<>();
        qualifiedNamesMap = new HashMap<>();
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

        public SqlHolderBuilder withFromItem(@Nullable FromItem fromItem) {
            if (fromItem != null) {
                holder.fromItem = fromItem;

                if (fromItem instanceof Table) {
                    Table table = (Table) fromItem;
                    holder.database = new DatabaseName(table.getFullyQualifiedName());
                }
            }

            return this;
        }

        public SqlHolderBuilder withLimit(long limit) {
            holder.limit = limit;
            return this;
        }

        public SqlHolderBuilder withOffset(long offset) {
            holder.offset = offset;
            return this;
        }

        public SqlHolderBuilder withWhere(@Nullable Expression whereClause) {
            if (whereClause != null) {
                holder.whereClause = whereClause;
            }
            return this;
        }

        public SqlHolderBuilder withSelectItems(@Nullable List<SelectItem> selectItems) {
            if (selectItems != null) {
                holder.selectItems = selectItems;
                holder.selectItemsStrings = getSelectItemsStringFromSelectItems(selectItems);

                holder.isSelectAll = holder.selectItemsStrings.contains("*");
            }
            return this;
        }

        private List<String> getSelectItemsStringFromSelectItems(List<SelectItem> selectItems) {
            return selectItems.stream()
                    .map(this::getStringFromSelectItem)
                    .collect(Collectors.toList());
        }

        private String getStringFromSelectItem(SelectItem item) {
            if (item instanceof AllColumns) {
                return "*";
            } else {
                Expression expression = ((SelectExpressionItem) item).getExpression();
                return SqlUtils.getStringValue(expression);
            }
        }

        public SqlHolderBuilder withJoins(@Nullable List<Join> joins) {
            if (joins != null) {
                holder.joins = joins;
            }
            return this;
        }

        public SqlHolderBuilder withGroupBy(@Nullable List<String> groupBys) {
            if (groupBys != null) {
                holder.groupBys = groupBys;
            }
            return this;
        }

        public SqlHolderBuilder withHaving(@Nullable Expression havingClause) {
            if (havingClause != null) {
                holder.havingClause = havingClause;
            }
            return this;
        }

        public SqlHolderBuilder withOrderBy(@Nullable List<OrderByElement> orderByElements) {
            if (orderByElements != null) {
                holder.orderByElements = orderByElements;
            }
            return this;
        }

        private boolean hasColumnAggregationFunction(String col) {
            return col.matches("sum\\(.*\\)|avg\\(.*\\)|min\\(.*\\)|max\\(.*\\)|count\\(.*\\)");
        }

        public SqlHolder build() {
            holder.selectItemsStrings.forEach(col -> holder.qualifiedNamesMap.put(
                    col.contains(".") ? col.substring(col.lastIndexOf(".") + 1).toLowerCase() : col.toLowerCase(),
                    col)
            );

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

            holder.selectItemMap.put(holder.fromItem, Lists.newArrayList());

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

                if (!holder.selectItemsStrings.equals(Collections.singletonList("*"))) {
                    String columnStr = getStringFromSelectItem(column);
                    String columnPrefix;
                    if (columnStr.contains(".")) {
                        columnPrefix = columnStr.substring(0, columnStr.lastIndexOf('.'));
                    } else {
                        columnPrefix = columnStr;
                    }
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

                    if (existsInVisibleColumns == (count == 1) && !holder.joins.isEmpty()) {
                        throw new IllegalArgumentException("Column '" + column + "' clashes");
                    }
                } else {
                    for (Table table : tables) {
                        holder.selectItemMap.put(table, holder.selectItems);
                    }
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

    public void updateSelectItems() {
        if (!isSelectAll) {
            selectItemsStrings.forEach(col -> {
                qualifiedNamesMap.put(MongoUtils.makeMongoColName(col).toLowerCase(), col);
                String selectItem = selectItemsStrings.get(selectItemsStrings.indexOf(col));
                selectItem += " AS " + MongoUtils.makeMongoColName(col);
                selectItemsStrings.set(selectItemsStrings.indexOf(col), selectItem);
            });
        }
    }

    public DatabaseName getDatabase() {
        return database;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public boolean isCountAll() {
        return isCountAll;
    }

    public boolean isSelectAll() {
        return isSelectAll;
    }

    @Nullable
    public FromItem getFromItem() {
        return fromItem;
    }

    public long getLimit() {
        return limit;
    }

    public long getOffset() {
        return offset;
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

    public List<String> getAdditionalSelectItemsStrings() {
        return additionalSelectItemsStrings;
    }

    public void addAllAdditionalItemStrings(List<String> item) {
        additionalSelectItemsStrings.addAll(item);
    }

    public void addAdditionalItemString(String item) {
        additionalSelectItemsStrings.add(item);
    }

    public List<String> getSelectIdents() {
        return selectItemsStrings.stream().map(SqlUtils::getIdentFromSelectItem).collect(Collectors.toList());
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

    public Map<String, String> getQualifiedNamesMap() {
        return qualifiedNamesMap;
    }

    private String getStringFromJoin(Join join) {
        if (join.isSimple()) {
            String joinStr = join.getRightItem().toString();
            return joinStr.substring(joinStr.indexOf('.') + 1);
        } else {
            String type = "";

            if (join.isRight()) {
                type += "RIGHT ";
            } else if (join.isNatural()) {
                type += "NATURAL ";
            } else if (join.isFull()) {
                type += "FULL ";
            } else if (join.isLeft()) {
                type += "LEFT ";
            } else if (join.isCross()) {
                type += "CROSS ";
            }

            if (join.isOuter()) {
                type += "OUTER ";
            } else if (join.isInner()) {
                type += "INNER ";
            } else if (join.isSemi()) {
                type += "SEMI ";
            }

            String rightItemStr = join.getRightItem().toString();
            rightItemStr = rightItemStr.substring(rightItemStr.indexOf('.') + 1);

            return type + "JOIN " + rightItemStr + ((join.getOnExpression() != null) ? " ON " + join.getOnExpression() + "" : "")
                    + PlainSelect.getFormatedList(join.getUsingColumns(), "USING", true, true);
        }
    }

    public String getSqlQuery() {
        StringBuilder sb = new StringBuilder();
        addSelect(sb);

        if (fromItem != null) {
            sb.append(" FROM ");
            if (fromItem instanceof Table) {
                String from = ((Table) fromItem).getFullyQualifiedName();
                sb.append(from.substring(from.indexOf('.') + 1));
            } else {
                sb.append(fromItem.toString());
            }
        }

        for (Join join : joins) {
            String joinStr = getStringFromJoin(join);
            sb.append(" ").append(joinStr).append(" ");
        }

        addWhere(sb);
        addGroupBy(sb);
        addOrderBy(sb);
        addLimit(sb);
        addOffset(sb);

        return sb.toString();
    }

    private void addSelect(StringBuilder sb) {
        sb.append("SELECT ");

        addDistinct(sb);

        Set<String> selectItems = new LinkedHashSet<>(selectItemsStrings);
        selectItems.addAll(additionalSelectItemsStrings);
        sb.append(String.join(", ", selectItems));
    }

    private void addOrderBy(StringBuilder sb) {
        if (!orderByElements.isEmpty()) {
            sb.append(" ORDER BY ");
            sb.append(orderByElements
                    .stream()
                    .map(OrderByElement::toString)
                    .collect(Collectors.joining(", "))
            );
        }
    }

    private void addGroupBy(StringBuilder sb) {
        if (!groupBys.isEmpty()) {
            sb.append(" GROUP BY ").append(String.join(", ", groupBys));

            if (havingClause != null) {
                sb.append(" HAVING ").append(havingClause);
            }
        }
    }

    private void addDistinct(StringBuilder sb) {
        if (isDistinct) {
            sb.append("DISTINCT ");
        }
    }

    private void addWhere(StringBuilder sb) {
        if (whereClause != null) {
            sb.append(" WHERE ").append(whereClause);
        }
    }

    private void addLimit(StringBuilder sb) {
        if (limit != -1) {
            sb.append(" LIMIT ").append(limit);
        }
    }

    private void addOffset(StringBuilder sb) {
        if (offset != -1) {
            sb.append(" OFFSET ").append(offset);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        addSelect(sb);

        if (fromItem != null) {
            sb.append(" FROM ");
            if (fromItem instanceof Table) {
                sb.append(((Table) fromItem).getFullyQualifiedName());
            } else {
                sb.append(fromItem.toString());
            }
        }

        for (Join join : joins) {
            sb.append(" ");
            sb.append(join);
        }

        addWhere(sb);
        addGroupBy(sb);
        addOrderBy(sb);
        addLimit(sb);
        addOffset(sb);

        return sb.toString();
    }
}
