package ru.bmstu.sqlfornosql.executor;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.mongodb.client.MongoDatabase;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.bson.BsonDocument;
import ru.bmstu.sqlfornosql.adapters.sql.SqlUtils;
import ru.bmstu.sqlfornosql.adapters.mongo.MongoAdapter;
import ru.bmstu.sqlfornosql.adapters.mongo.MongoClient;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.model.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Executor {
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

    public Table execute(String sql) {
        SqlHolder sqlHolder = SqlUtils.fillSqlMeta(sql);
        if (sqlHolder.getJoins().isEmpty()) {
            return simpleSelect(sqlHolder);
        } else {
            //TODO
            throw new UnsupportedOperationException("Not implemented yet");
        }
    }

    /**
     * Выполняет SELECT запрос без JOIN'ов и с одной таблицей
     * @param sqlHolder - холдер, содержащий запрос
     * @return таблицу с результатом выполнения запроса
     */
    //TODO здесь fromItem может быть подзапросом - это нужно обработать
    private Table simpleSelect(SqlHolder sqlHolder) {
        if (sqlHolder.getFromItem() instanceof Table) {
            switch (sqlHolder.getDatabase().getDbType()) {
                case POSTGRES:
                    throw new UnsupportedOperationException("not implemented yet");
                case MONGODB:
                    try (com.mongodb.MongoClient client = new com.mongodb.MongoClient()) {
                        MongoDatabase database = client.getDatabase(sqlHolder.getDatabase().getDatabaseName());
                        MongoAdapter adapter = new MongoAdapter();
                        MongoClient<BsonDocument> mongoClient = new MongoClient<>(
                                database.getCollection(sqlHolder.getDatabase().getTable(), BsonDocument.class)
                        );
                        return mongoClient.executeQuery(adapter.translate(sqlHolder));
                    }
                default:
                    throw new IllegalArgumentException("Unknown database type");
            }
        } else if (sqlHolder.getFromItem() instanceof SubSelect) {
            SubSelect subSelect = ((SubSelect) sqlHolder.getFromItem());
            String subSelectStr = subSelect.toString();
            Table subSelectResult;
            if (subSelect.isUseBrackets()) {
                subSelectResult = execute(subSelectStr.substring(1, subSelectStr.length() - 1));
            } else {
                subSelectResult = execute(subSelectStr);
            }

            //TODO merge results
            throw new UnsupportedOperationException("Not implemented yet");
        } else {
            throw new IllegalStateException("Can't determine type of FROM argument");
        }
    }

    /**
     * Выполяняет SELECT запрос из нескольких таблиц и/или с JOIN
     * @param sqlHolder - холдер, содержащий запрос
     * @return таблицу с результато выполнения запроса
     */
    private Table selectWithJoins(SqlHolder sqlHolder) {
        if (sqlHolder.getFromItem() instanceof SubSelect) {
            SubSelect subSelect = ((SubSelect) sqlHolder.getFromItem());
            String subSelectStr = subSelect.toString();
            Table subSelectResult;
            if (subSelect.isUseBrackets()) {
                subSelectResult = execute(subSelectStr.substring(1, subSelectStr.length() - 1));
            } else {
                subSelectResult = execute(subSelectStr);
            }
        } else {
            int sourceCount = sqlHolder.getSelectItemMap().size();
            List<SqlHolder> sqlHolders = new ArrayList<>(sourceCount);
            for (Map.Entry<FromItem, List<SelectItem>> fromItemListEntry : sqlHolder.getSelectItemMap().entrySet()) {
                SqlHolder holder = new SqlHolder.SqlHolderBuilder()
                        .withSelectItems(fromItemListEntry.getValue())
                        .withFromItem(fromItemListEntry.getKey()).build();

                String query = holder.toString();

                List<String> queryOrParts = new ArrayList<>();

                Set<String> selectItemsStr = Sets.newHashSet(holder.getSelectItemsStrings());

                if (sqlHolder.getWhereClause() != null) {
                    String whereExpression = sqlHolder.getWhereClause().toString();
                    String[] orParts = whereExpression.split("\\sOR\\s");
                    for (String part : orParts) {
                        Matcher matcher = IDENT_REGEXP.matcher(part.replaceAll("'.*'", ""));
                        List<String> idents = new ArrayList<>();
                        while (matcher.find()) {
                            if (!FORBIDDEN_STRINGS.contains(matcher.group(1).toUpperCase())) {
                                idents.add(matcher.group(1));
                            }
                        }

                        if (selectItemsStr.containsAll(idents)) {
                            queryOrParts.add(part);
                        }
                    }
                }

                query += " " + String.join(" OR ", queryOrParts);

                if (!sqlHolder.getGroupBys().isEmpty()) {
                    List<String> groupBys = new ArrayList<>();
                    for (String groupByItem : sqlHolder.getGroupBys()) {
                        if (selectItemsStr.contains(groupByItem)) {
                            groupBys.add(groupByItem);
                        }
                    }

                    //TODO
                }
            }
        }

        throw new UnsupportedOperationException("not implemented yet");
    }
}
