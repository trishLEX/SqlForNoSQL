package ru.bmstu.sqlfornosql.executor;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.mongodb.client.MongoDatabase;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.*;
import org.bson.BsonDocument;
import org.medfoster.sqljep.ParseException;
import org.medfoster.sqljep.RowJEP;
import ru.bmstu.sqlfornosql.adapters.mongo.MongoAdapter;
import ru.bmstu.sqlfornosql.adapters.mongo.MongoClient;
import ru.bmstu.sqlfornosql.adapters.mongo.MongoUtils;
import ru.bmstu.sqlfornosql.adapters.postgres.PostgresClient;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.adapters.sql.SqlUtils;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.Column;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectField;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectFieldExpression;
import ru.bmstu.sqlfornosql.model.Row;
import ru.bmstu.sqlfornosql.model.Table;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ru.bmstu.sqlfornosql.executor.ExecutorUtils.*;

public class Executor {
    public static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    //TODO вводится правило, что работаем только с lowerCase'ми
    static final Pattern IDENT_REGEXP = Pattern.compile("([a-zA-Z]+[0-9a-zA-Z.]*)");

    static final Set<String> FORBIDDEN_STRINGS = ImmutableSet.of(
            "SELECT",
            "AND",
            "IN",
            "BETWEEN",
            "NOT",
            "IS",
            "DATE",
            "TIME",
            "TIMESTAMP",
            "LIKE",
            "TRUE",
            "FALSE",
            "NULL"
    );

    public CompletableFuture<Table> execute(String sql) {
        SqlHolder sqlHolder = SqlUtils.fillSqlMeta(sql);
        if (sqlHolder.getJoins().isEmpty()) {
            return simpleSelect(sqlHolder);
        } else {
            return selectWithJoins(sqlHolder);
        }
    }

    /**
     * Выполняет SELECT запрос без JOIN'ов и с одной таблицей
     * @param sqlHolder - холдер, содержащий запрос
     * @return таблицу с результатом выполнения запроса
     */
    //TODO здесь fromItem может быть подзапросом - это нужно обработать
    private CompletableFuture<Table> simpleSelect(SqlHolder sqlHolder) {
        sqlHolder.fillColumnMap();
        if (sqlHolder.getFromItem() instanceof net.sf.jsqlparser.schema.Table) {
            switch (sqlHolder.getDatabase().getDbType()) {
                case POSTGRES: {
                    PostgresClient client = new PostgresClient("localhost", 5432, "postgres", "0212", "postgres");
                    CompletableFuture<Table> result = CompletableFuture.supplyAsync(() -> client.executeQuery(sqlHolder), EXECUTOR);
                    result.thenAccept(t -> client.close());
                    return result;
                }
                case MONGODB: {
                    com.mongodb.MongoClient client = new com.mongodb.MongoClient();
                    MongoDatabase database = client.getDatabase(sqlHolder.getDatabase().getDatabaseName());
                    MongoAdapter adapter = new MongoAdapter();
                    MongoClient<BsonDocument> mongoClient = new MongoClient<>(
                            database.getCollection(sqlHolder.getDatabase().getTable(), BsonDocument.class)
                    );
                    CompletableFuture<Table> result = CompletableFuture.supplyAsync(() -> mongoClient.executeQuery(adapter.translate(sqlHolder)), EXECUTOR);
                    result.thenAccept(table -> client.close());
                    return result;
                }
                default:
                    throw new IllegalArgumentException("Unknown database type");
            }
        } else if (sqlHolder.getFromItem() instanceof SubSelect) {
            SubSelect subSelect = ((SubSelect) sqlHolder.getFromItem());
            CompletableFuture<Table> subSelectResultFuture;
            ((PlainSelect) subSelect.getSelectBody()).getFromItem().setAlias(subSelect.getAlias());
            String subSelectStr = subSelect.toString();
            if (subSelect.isUseBrackets()) {
                subSelectResultFuture = execute(subSelectStr.substring(1, subSelectStr.lastIndexOf(')')));
            } else {
                subSelectResultFuture = execute(subSelectStr);
            }

            //subSelectResult.getColumns().forEach(column -> column.setFromItemAlias(subSelect.getAlias()));

//            for (String col : sqlHolder.getSelectItemsStrings()) {
//                if (col.startsWith("avg(")) {
//                    sqlHolder.addAdditionalItemString("count" + col.substring(3));
//                }
//            }

            Table subSelectResult = subSelectResultFuture.join();
            return CompletableFuture.supplyAsync(
                    () -> {
                        Table result = new Table();
                        for (Row subSelectRow : subSelectResult.getRows()) {
                            Row row = new Row(result);
                            for (SelectField column : sqlHolder.getSelectFields()) {
                                if (column instanceof SelectFieldExpression) {
                                    Column ident = ((SelectFieldExpression) column).getColumn();
                                    row.add(ident, subSelectRow.getObject(ident));
                                    result.setType(column, subSelectResult.getType(ident));
                                } else {
                                    row.add(column, subSelectRow.getObject(column));
                                    result.setType(column, subSelectResult.getType(column));
                                }
                            }

                            if (sqlHolder.getWhereClause() != null) {
                                HashMap<String, Integer> colMapping = getIdentMapping(sqlHolder.getWhereClause().toString().toLowerCase());
                                RowJEP sqljep = prepareSqlJEP(sqlHolder.getWhereClause(), colMapping);
                                Comparable[] values = new Comparable[colMapping.size()];

                                for (Map.Entry<String, Integer> colMappingEntry : colMapping.entrySet()) {
                                    values[colMappingEntry.getValue()] = getValue(row, colMappingEntry.getKey());
                                }

                                try {
                                    Boolean expressionValue = (Boolean) sqljep.getValue(values);
                                    if (expressionValue) {
                                        result.add(row);
                                    }
                                } catch (ParseException e) {
                                    throw new IllegalStateException("Can't execute expression: " + sqlHolder.getWhereClause(), e);
                                }
                            } else {
                                result.add(row);
                            }
                        }

                        if (!sqlHolder.getGroupBys().isEmpty()) {
                            result = Grouper.group(sqlHolder, result, sqlHolder.getGroupBys(), sqlHolder.getSelectFields(), sqlHolder.getHavingClause());
                        }

                        if (!sqlHolder.getOrderByElements().isEmpty()) {
                            LinkedHashMap<SelectField, Boolean> orderByMap = new LinkedHashMap<>();
                            for (OrderByElement element : sqlHolder.getOrderByElements()) {
                                //TODO order by можно выполянть по expression'у
                                orderByMap.put(
                                        sqlHolder.getFieldByNonQualifiedName(
                                                MongoUtils.getNonQualifiedName(element.toString())
                                        ),
                                        element.isAsc());
                            }
                            result.sort(orderByMap);
                        }

                        return result;
                    },
                    EXECUTOR);
        } else {
            throw new IllegalStateException("Can't determine type of FROM argument");
        }
    }

    /**
     * Выполяняет SELECT запрос из нескольких таблиц и/или с JOIN
     * @param sqlHolder - холдер, содержащий запрос
     * @return таблицу с результато выполнения запроса
     */
    //TODO нужно отрефакторить
    private CompletableFuture<Table> selectWithJoins(SqlHolder sqlHolder) {
        Map<FromItem, CompletableFuture<Table>> resultParts = new LinkedHashMap<>();
        List<String> queries = new ArrayList<>();

        Set<SelectField> additionalSelectItems = new HashSet<>();
        for (Map.Entry<FromItem, List<SelectItem>> fromItemListEntry : sqlHolder.getSelectItemMap().entrySet()) {
            SqlHolder holder = new SqlHolder.SqlHolderBuilder()
                    .withSelectItems(fromItemListEntry.getValue())
                    .withFromItem(fromItemListEntry.getKey())
                    .build();

            Set<SelectField> selectItemsStr = Sets.newHashSet(holder.getSelectFields());

            holder.addAllAdditionalSelectFields(getAdditionalSelectItemsStrings(fromItemListEntry.getKey(), sqlHolder.getJoins(), selectItemsStr));

            if (sqlHolder.getWhereClause() != null) {
                holder.addAllAdditionalSelectFields(getIdentsFromExpression(fromItemListEntry.getKey(), sqlHolder.getWhereClause(), selectItemsStr));
            }

            Set<SelectField> holderAdditionalColumns = Sets.newHashSet(holder.getAdditionalSelectFields());
            additionalSelectItems.addAll(holderAdditionalColumns);
            selectItemsStr.addAll(holderAdditionalColumns);

            if (selectItemsStr.isEmpty()) {
                resultParts.put(fromItemListEntry.getKey(), CompletableFuture.supplyAsync(Table::new, EXECUTOR));
                continue;
            }

            String query = holder.toString();

            List<String> queryOrParts = new ArrayList<>();

            if (sqlHolder.getWhereClause() != null) {
                fillIdents(selectItemsStr, queryOrParts, sqlHolder.getWhereClause());

                if (!queryOrParts.isEmpty()) {
                    query += " WHERE " + String.join(" OR ", queryOrParts);
                }
            }

            if (!sqlHolder.getGroupBys().isEmpty()) {
                List<SelectField> groupBys = new ArrayList<>();
                for (SelectField groupByItem : sqlHolder.getGroupBys()) {
                    if (selectItemsStr.contains(groupByItem)) {
                        groupBys.add(groupByItem);
                    }
                }

                if (!groupBys.isEmpty()) {
                    query += " GROUP BY " + groupBys.stream().map(SelectField::getQualifiedContent).collect(Collectors.joining(" ,"));
                }

                if (sqlHolder.getHavingClause() != null) {
                    List<String> havingOrParts = new ArrayList<>();
                    fillIdents(selectItemsStr, havingOrParts, sqlHolder.getHavingClause());

                    if (!havingOrParts.isEmpty()) {
                        query += " HAVING " + String.join(" OR ", havingOrParts);
                    }
                }
            }

            if (!sqlHolder.getOrderByElements().isEmpty()) {
                List<String> orderBys = new ArrayList<>();
                for (OrderByElement orderByItem : sqlHolder.getOrderByElements()) {
                    fillIdents(selectItemsStr, orderBys, orderByItem.toString());
                }

                if (!orderBys.isEmpty()) {
                    query += " ORDER BY " + String.join(" ,", orderBys);
                }
            }

            queries.add(query);

            CompletableFuture<Table> result = execute(query);
            resultParts.put(fromItemListEntry.getKey(), result);
        }

        System.out.println(queries);
        sqlHolder.addAllAdditionalSelectFields(additionalSelectItems);

        CompletableFuture<Table> from = resultParts.remove(sqlHolder.getFromItem());
        if (from == null) {
            throw new IllegalStateException("FromItem can not be null");
        }

        //TODO можно оптимизировать: не нужно дополнительный раз вставлять whereClause
        return Joiner.join(
                sqlHolder,
                from.join(),
                resultParts.values().stream().collect(Collectors.toList()),
                sqlHolder.getJoins(),
                sqlHolder.getWhereClause()
        ).thenApplyAsync(
                table -> {
                    table.remove(Sets.difference(additionalSelectItems, Sets.newHashSet(sqlHolder.getSelectFields())));
                    return table;
                },
                EXECUTOR);
    }

    //TODO не поддерживатся USING
    private List<SelectField> getAdditionalSelectItemsStrings(FromItem from, List<Join> joins, Set<SelectField> existingSelectItems) {
        List<SelectField> selectFields = new ArrayList<>();
        for (Join join : joins) {
            if (join.getOnExpression() != null) {
                selectFields.addAll(getIdentsFromExpression(from, join.getOnExpression(), existingSelectItems));
            }
        }

        return selectFields;
    }

    private List<SelectField> getIdentsFromExpression(FromItem fromItem, Expression expression, Set<SelectField> existingSelectItems) {
        Set<String> existingSelectItemsStr = existingSelectItems.stream().map(SelectField::getNonQualifiedIdent).collect(Collectors.toSet());
        String stringExpr = expression.toString();
        Matcher matcher = IDENT_REGEXP.matcher(stringExpr.replaceAll("'.*'", ""));
        List<SelectField> idents = new ArrayList<>();
        String fromItemSuffix = fromItem.getAlias() == null ?  fromItem.toString().substring(fromItem.toString().lastIndexOf('.') + 1) : fromItem.getAlias().getName();
        while (matcher.find()) {
            String potentialIdent = matcher.group(1);
            String prefix = potentialIdent.contains(".") ? potentialIdent.substring(0, potentialIdent.lastIndexOf('.')) : potentialIdent;
            if (!FORBIDDEN_STRINGS.contains(matcher.group(1).toUpperCase())
                    && fromItem.toString().endsWith(prefix)
                    && prefix.contains(fromItemSuffix)
                    && !existingSelectItemsStr.contains(potentialIdent))
            {
                idents.add(new Column(potentialIdent).withSource(fromItem));
            }
        }

        return idents;
    }

    private void fillIdents(Set<SelectField> selectItems, List<String> destination, String str) {
        Set<String> selectItemsStr = selectItems.stream().map(SelectField::getNonQualifiedIdent).collect(Collectors.toSet());
        List<String> idents = ExecutorUtils.getIdentsFromString(str);

        if (selectItemsStr.containsAll(idents)) {
            destination.add(str);
        }
    }

    private void fillIdents(Set<SelectField> selectItemsStr, List<String> whereOrParts, Expression whereClause) {
        String whereExpression = whereClause.toString();
        String[] orParts = whereExpression.split("\\sOR\\s");
        for (String part : orParts) {
            fillIdents(selectItemsStr, whereOrParts, part);
        }
    }
}
