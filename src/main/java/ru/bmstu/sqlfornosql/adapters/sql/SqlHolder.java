package ru.bmstu.sqlfornosql.adapters.sql;

import com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import one.util.streamex.StreamEx;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.Column;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.OrderableSelectField;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectField;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectFieldExpression;
import ru.bmstu.sqlfornosql.executor.ExecutorUtils;
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

    public void setLimit(long limit) {
        this.limit = limit;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public static class Builder {
        private SqlHolder holder;

        public Builder() {
            this.holder = new SqlHolder();
        }

        public Builder withDistinct(boolean isDistinct) {
            holder.isDistinct = isDistinct;
            return this;
        }

        public Builder withCountAll(boolean isCountAll) {
            holder.isCountAll = isCountAll;
            return this;
        }

        public Builder withFromItem(@Nullable FromItem fromItem) {
            if (fromItem != null) {
                holder.fromItem = fromItem;
                holder.selectItemMap.put(fromItem, Lists.newArrayList());

                if (fromItem instanceof Table) {
                    Table table = (Table) fromItem;
                    holder.database = new DatabaseName(table.getFullyQualifiedName());
                }
            }

            return this;
        }

        public Builder withLimit(long limit) {
            holder.limit = limit;
            return this;
        }

        public Builder withOffset(long offset) {
            holder.offset = offset;
            return this;
        }

        public Builder withWhere(@Nullable Expression whereClause) {
            if (whereClause != null) {
                holder.whereClause = whereClause;
            }
            return this;
        }

        public Builder withSelectItems(@Nullable List<SelectItem> selectItems) {
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

        public Builder withJoins(@Nullable List<Join> joins) {
            if (joins != null) {
                holder.joins = joins;
                for (Join join : joins) {
                    holder.selectItemMap.put(join.getRightItem(), Lists.newArrayList());
                }
            }
            return this;
        }

        //TODO кажется fromItem может быть разный
        public Builder withGroupBy(@Nullable List<String> groupBys) {
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

        public Builder withHaving(@Nullable Expression havingClause) {
            if (havingClause != null) {
                holder.havingClause = havingClause;
            }
            return this;
        }

        private Builder withOrderBy(@Nullable List<OrderableSelectField> orderByElements) {
            if (orderByElements != null) {
                holder.orderBys = orderByElements;
            }
            return this;
        }

        public Builder withOrderByElements(@Nullable List<OrderByElement> orderByElements) {
            if (orderByElements != null) {
                holder.orderByElements = orderByElements;
                return withOrderBy(orderByElements.stream().map(SqlUtils::toSelectField).collect(Collectors.toList()));
            }
            return this;
        }

        public SqlHolder build() {
            List<FromItem> fromItems = StreamEx
                    .of(holder.fromItem)
                    .append(holder.joins.stream().map(Join::getRightItem))
                    .collect(Collectors.toList());

            for (SelectItem selectItem : holder.selectItems) {
                List<FromItem> potentialSources = selectItem.toString().equals("*") ?
                        Lists.newArrayList(holder.fromItem) : getPotentialSources(selectItem, fromItems);
                if (potentialSources.size() != 1) {
                    throw new IllegalArgumentException("Can't determine source of column: " + selectItem);
                }

                holder.selectItemMap.get(potentialSources.get(0)).add(selectItem);
            }

            for (Map.Entry<FromItem, List<SelectItem>> fromItemListEntry : holder.selectItemMap.entrySet()) {
                for (SelectItem selectItem : fromItemListEntry.getValue()) {
                    holder.itemToField.get(selectItem).setSource(fromItemListEntry.getKey());
                }
            }

            return holder;
        }

        private List<FromItem> getPotentialSources(SelectItem selectItem, List<FromItem> fromItems) {
            return fromItems.stream()
                    .filter(from -> Builder.isSelectItemFromSource(selectItem, from))
                    .collect(Collectors.toList());
        }

        private static boolean isSelectItemFromSource(SelectItem selectItem, FromItem fromItem) {
            String selectItemStr = SqlUtils.getIdentFromSelectItem(selectItem);
            if (fromItem instanceof SubSelect || fromItem.getAlias() != null) {
                return selectItemStr.substring(0, selectItemStr.indexOf('.')).equals(fromItem.getAlias().getName());
            } else {
                String[] selectItemParts = selectItemStr.split("\\.");
                String[] fromItemParts = fromItem.toString().split("\\.");
                for (int i = selectItemParts.length - 2, j = 1;
                     i >= 0;
                     i--, j++
                ) {
                    if (j > fromItemParts.length || !selectItemParts[i].equals(fromItemParts[fromItemParts.length - j])) {
                        return false;
                    }
                }

                return true;
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

        addWhereToQuery(sb);
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
                .map(SelectField::getQualifiedIdent)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        selectItems.addAll(additionalSelectFields.stream()
                .map(SelectField::getQualifiedIdent)
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

    private void addWhereToQuery(StringBuilder sb) {
        if (whereClause != null) {
            sb.append(" WHERE ");
            List<String> idents = ExecutorUtils.getIdentsFromString(whereClause.toString());
            String where = whereClause.toString();
            for (String ident : idents) {
                if (ident.codePoints().filter(cp -> cp == '.').count() == 4) {
                    String newIdent = ident.substring(ident.indexOf('.') + 1);
                    where = where.replaceAll(ident, newIdent);
                }
            }
            sb.append(where);
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
