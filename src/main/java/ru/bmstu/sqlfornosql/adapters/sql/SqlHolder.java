package ru.bmstu.sqlfornosql.adapters.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.Column;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.OrderableSelectField;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectField;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectFieldExpression;
import ru.bmstu.sqlfornosql.model.DatabaseName;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ru.bmstu.sqlfornosql.adapters.sql.selectfield.AllColumns.ALL_COLUMNS;

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
    private List<Join> joins;
    private List<SelectField> groupBys;

    @Nullable
    private Expression havingClause;
    private List<OrderableSelectField> orderBys;
    private List<OrderByElement> orderByElements;

    private Map<FromItem, List<SelectItem>> selectItemMap;
    private List<SelectField> selectFields;
    private Set<SelectField> additionalSelectFields;
    private Map<String, SelectField> columnNameToSelectField;
    private Map<SelectItem, SelectField> itemToField;

    private DatabaseName database;

    public SqlHolder() {
        isDistinct = false;
        isCountAll = false;
        isSelectAll = false;
        limit = -1;
        offset = -1;
        selectItems = new ArrayList<>();
        joins = new ArrayList<>();
        groupBys = new ArrayList<>();
        orderByElements = new ArrayList<>();
        orderBys = new ArrayList<>();
        selectItemMap = new LinkedHashMap<>();
        selectFields = new ArrayList<>();
        additionalSelectFields = new HashSet<>();
        columnNameToSelectField = new HashMap<>();
        itemToField = new HashMap<>();
    }

    public SqlHolderBuilder toBuilder() {
        return new SqlHolderBuilder()
                .withDistinct(isDistinct)
                .withCountAll(isCountAll)
                .withLimit(limit)
                .withOffset(offset)
                .withSelectItems(selectItems)
                .withJoins(joins)
                .withGroupBy(groupBys.stream().map(SelectField::toString).collect(Collectors.toList()))
                .withOrderByElements(orderByElements)
                .withFromItem(fromItem)
                .withWhere(whereClause)
                .withHaving(havingClause);
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    public void setOffset(long offset) {
        this.offset = offset;
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
                holder.selectFields = getSelectFieldsFromSelectItems(selectItems);

                holder.isSelectAll = holder.selectFields.contains(ALL_COLUMNS);
            }
            return this;
        }

        private List<SelectField> getSelectFieldsFromSelectItems(List<SelectItem> selectItems) {
            return selectItems.stream()
                    .map(item -> {
                        SelectField selectField = SqlUtils.getSelectFieldFromString(item);
                        holder.itemToField.put(item, selectField);
                        return selectField;
                    })
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
                for (Join join : joins) {
                    holder.selectItemMap.put(join.getRightItem(), Lists.newArrayList());
                }
            }
            return this;
        }

        //TODO кажется fromItem может быть разный
        public SqlHolderBuilder withGroupBy(@Nullable List<String> groupBys) {
            if (groupBys != null) {
                holder.groupBys = groupBys
                        .stream()
                        .map(column -> {
                            if (holder.containsByUserInput(column)) {
                                return holder.getByUserInput(column);
                            } else {
                                return new Column(column).withSource(holder.fromItem);
                            }
                        })
                        .collect(Collectors.toList());
            }
            return this;
        }

        public SqlHolderBuilder withHaving(@Nullable Expression havingClause) {
            if (havingClause != null) {
                holder.havingClause = havingClause;
            }
            return this;
        }

        private SqlHolderBuilder withOrderBy(@Nullable List<OrderableSelectField> orderByElements) {
            if (orderByElements != null) {
                holder.orderBys = orderByElements;
            }
            return this;
        }

        public SqlHolderBuilder withOrderByElements(@Nullable List<OrderByElement> orderByElements) {
            if (orderByElements != null) {
                holder.orderByElements = orderByElements;
                return withOrderBy(orderByElements.stream().map(SqlUtils::toSelectField).collect(Collectors.toList()));
            }
            return this;
        }

        //TODO переписать, уж больно сложный метод
        public SqlHolder build() {
            List<SubSelect> subSelects = new ArrayList<>();
            List<FromItem> tables = new ArrayList<>();
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
                boolean existsInVisibleColumns = visibleColumns.contains(column);;

                if (existsInVisibleColumns) {
                    SubSelect subSelect = columnToSubSelect.get(column);
                    if (columnToSubSelect.size() == 1 && columnToSubSelect.keySet().stream().map(SelectItem::toString).anyMatch(c -> c.equals("*"))) {
                        subSelect = Iterables.getOnlyElement(columnToSubSelect.values());
                    }
                    if (columnToSubSelect.keySet().stream().map(SelectItem::toString).anyMatch(c -> c.equals("*"))
                            || holder.selectItemMap.containsKey(subSelect)) {
                        holder.selectItemMap.get(subSelect).add(column);
                    } else {
                        holder.selectItemMap.put(subSelect, Lists.newArrayList(column));
                    }
                }

                if (!holder.isSelectAll) {
                    SelectField selectField = SqlUtils.getSelectFieldFromString(column);
                    String columnStr = selectField instanceof Column ? selectField.getUserInputName() : ((SelectFieldExpression) selectField).getColumn().getUserInputName();
                    String columnPrefix;
                    if (columnStr.contains(".")) {
                        columnPrefix = columnStr.substring(0, columnStr.lastIndexOf('.'));
                    } else {
                        columnPrefix = columnStr;
                    }
                    int count = tables
                            .stream()
                            .map(table -> {
                                String fromStr = table.getAlias() != null ? table.getAlias().getName() : ((Table) table).getFullyQualifiedName();
                                String fromStrSuffix = fromStr.contains(".") ? fromStr.substring(fromStr.lastIndexOf('.') + 1) : fromStr;
                                if (fromStr.endsWith(columnPrefix) && columnPrefix.contains(fromStrSuffix)) {
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
                            .reduce(0, Integer::sum);
                    //TODO не подходит способ (пример: test.test и t -- test заканчиватеся на t, но это 2 разные таблицы)
//                    if (count > 1) {
//                        throw new IllegalArgumentException("Column '" + column + "' clashes");
//                    }

//                    if (existsInVisibleColumns == (count == 1) && !holder.joins.isEmpty()) {
//                        throw new IllegalArgumentException("Column '" + column + "' clashes");
//                    }
                } else {
                    for (FromItem table : tables) {
                        holder.selectItemMap.put(table, holder.selectItems);
                    }
                }
            }

            for (Map.Entry<FromItem, List<SelectItem>> fromItemListEntry : holder.selectItemMap.entrySet()) {
                for (SelectItem selectItem : fromItemListEntry.getValue()) {
                    holder.itemToField.get(selectItem).setSource(fromItemListEntry.getKey());
                }
            }

            return holder;
        }

        private void fillVisibleColumnsAndTables(List<SubSelect> visibleColumns, List<FromItem> tables,
                FromItem fromItem)
        {
            if (fromItem instanceof SubSelect) {
                SubSelect select = (SubSelect) fromItem;
                Preconditions.checkNotNull(select.getAlias(), "SubSelects must have alias");
                visibleColumns.add(select);
                tables.add(fromItem);
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

    //TODO alias не поддерживаются
    public void updateSelectItems() {
        if (!isSelectAll) {
//            selectItemsStrings.forEach(col -> {
//                qualifiedNamesMap.put(MongoUtils.makeMongoColName(col).toLowerCase(), col);
//                String selectItem = selectItemsStrings.get(selectItemsStrings.indexOf(col));
//                selectItem += " AS " + MongoUtils.makeMongoColName(col);
//                selectItemsStrings.set(selectItemsStrings.indexOf(col), selectItem);
//            });
            for (SelectField selectField : selectFields) {
                if (selectField instanceof Column && ((Column) selectField).getAlias() == null) {
                    Column column = (Column) selectField;
                    column.setAlias(column.getNonQualifiedContent());
                } else if (selectField instanceof SelectFieldExpression && ((SelectFieldExpression) selectField).getAlias() == null) {
                    SelectFieldExpression expression = (SelectFieldExpression) selectField;
                    expression.setAlias(expression.getNonQualifiedContent());
                }
            }
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

    public List<SelectField> getSelectFields() {
        return selectFields;
    }

    public Set<SelectField> getAdditionalSelectFields() {
        return additionalSelectFields;
    }

    public void addAllAdditionalSelectFields(Collection<SelectField> items) {
        additionalSelectFields.addAll(items);
    }

    public List<String> getSelectIdents() {
        return selectFields.stream().map(SelectField::getQualifiedIdent).collect(Collectors.toList());
    }

    public List<Join> getJoins() {
        return joins;
    }

    public List<SelectField> getGroupBys() {
        return groupBys;
    }

    @Nullable
    public Expression getHavingClause() {
        return havingClause;
    }

    public List<OrderableSelectField> getOrderBys() {
        return orderBys;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public Map<FromItem, List<SelectItem>> getSelectItemMap() {
        return selectItemMap;
    }

    public Map<String, SelectField> getColumnNameToSelectField() {
        return columnNameToSelectField;
    }

    public SelectField getSelectFieldByColumnName(String columnName) {
        if (columnNameToSelectField.containsKey(columnName)) {
            return columnNameToSelectField.get(columnName);
        }

        for (SelectField selectField : selectFields) {
            if (selectField.getNonQualifiedContent().equalsIgnoreCase(columnName)) {
                return selectField;
            }
        }

        for (SelectField selectField : additionalSelectFields) {
            if (selectField.getUserInputName().equalsIgnoreCase(columnName)) {
                return selectField;
            }
        }

        throw new NoSuchElementException("Column '" + columnName + "' not in select list");
    }

    public SelectField getFieldByNonQualifiedName(String name) {
        for (SelectField field : selectFields) {
            if (field.getNonQualifiedContent().equalsIgnoreCase(name)) {
                return field;
            }
        }

        for (SelectField selectField : additionalSelectFields) {
            if (selectField.getUserInputName().equalsIgnoreCase(name)) {
                return selectField;
            }
        }

        throw new NoSuchElementException("No field with name: " + name + " in select items");
    }

    public SelectField getFieldByFullQualifiedName(String name) {
        for (SelectField field : selectFields) {
            if (field.getQualifiedContent().equalsIgnoreCase(name)) {
                return field;
            }
        }

        for (SelectField selectField : additionalSelectFields) {
            if (selectField.getQualifiedContent().equalsIgnoreCase(name)) {
                return selectField;
            }
        }

        throw new NoSuchElementException("No field with name: " + name + "was found");
    }

    public SelectField getByUserInput(String name) {
        for (SelectField selectField : selectFields) {
            if (selectField.getUserInputName().equalsIgnoreCase(name)) {
                return selectField;
            }
        }

        for (SelectField selectField : additionalSelectFields) {
            if (selectField.getUserInputName().equalsIgnoreCase(name)) {
                return selectField;
            }
        }

        throw new NoSuchElementException("No elements with user input name: " + name + " was found");
    }

    public boolean containsByUserInput(String name) {
        for (SelectField selectField : selectFields) {
            if (selectField.getUserInputName().equalsIgnoreCase(name)) {
                return true;
            }
        }

        return false;
    }

    public void fillColumnMap() {
        columnNameToSelectField = selectFields.stream().collect(Collectors.toMap(
                SelectField::getNonQualifiedName,
                Function.identity()
        ));
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
        addSelectToQuery(sb);

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
        addGroupByToQuery(sb);
        addOrderByToQuery(sb);
        addLimit(sb);
        addOffset(sb);

        return sb.toString();
    }

    private void addSelectToString(StringBuilder sb) {
        sb.append("SELECT ");

        addDistinct(sb);

        Set<String> selectItems = selectFields.stream()
                .map(SelectField::getQualifiedContent)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        selectItems.addAll(additionalSelectFields.stream()
                .map(SelectField::getQualifiedContent)
                .collect(Collectors.toSet())
        );
        sb.append(String.join(", ", selectItems));
    }

    private void addSelectToQuery(StringBuilder sb) {
        sb.append("SELECT ");

        addDistinct(sb);

        Set<String> selectItems = selectFields.stream()
                .map(field -> {
                    if (field instanceof Column && ((Column) field).getAlias() != null) {
                        return field.getNativeInDbName() + " AS \"" + ((Column) field).getAlias() + "\"";
                    } else if (field instanceof SelectFieldExpression && ((SelectFieldExpression) field).getAlias() != null) {
                        return field.getNativeInDbName() + " AS \"" + ((SelectFieldExpression) field).getAlias() + "\"";
                    } else {
                        return field.getNativeInDbName();
                    }
                })
                .collect(Collectors.toCollection(LinkedHashSet::new));

        selectItems.addAll(additionalSelectFields.stream()
                .map(SelectField::getNativeInDbName)
                .collect(Collectors.toSet())
        );
        sb.append(String.join(", ", selectItems));
    }

    private void addOrderByToString(StringBuilder sb) {
        if (!orderBys.isEmpty()) {
            sb.append(" ORDER BY ")
                    .append(orderBys.stream()
                            .map(OrderableSelectField::toString)
                            .collect(Collectors.joining(", "))
            );
        }
    }

    private void addOrderByToQuery(StringBuilder sb) {
        if (!orderByElements.isEmpty()) {
            sb.append(" ORDER BY ")
                    .append(orderBys.stream()
                            .map(OrderableSelectField::toString)
                            .collect(Collectors.joining(", "))
            );
        }
    }

    private void addGroupByToString(StringBuilder sb) {
        if (!groupBys.isEmpty()) {
            sb.append(" GROUP BY ")
                    .append(groupBys.stream()
                            .map(SelectField::getQualifiedContent)
                            .collect(Collectors.joining(", "))
                    );

            if (havingClause != null) {
                sb.append(" HAVING ").append(havingClause);
            }
        }
    }

    private void addGroupByToQuery(StringBuilder sb) {
        if (!groupBys.isEmpty()) {
            sb.append(" GROUP BY ")
                    .append(groupBys.stream()
                            .map(SelectField::getNativeInDbName)
                            .collect(Collectors.joining(", "))
                    );

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
        if (limit > 0) {
            sb.append(" LIMIT ").append(limit);
        }
    }

    private void addOffset(StringBuilder sb) {
        if (offset > 0) {
            sb.append(" OFFSET ").append(offset);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        addSelectToString(sb);

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
        addGroupByToString(sb);
        addOrderByToString(sb);
        addLimit(sb);
        addOffset(sb);

        return sb.toString();
    }
}
