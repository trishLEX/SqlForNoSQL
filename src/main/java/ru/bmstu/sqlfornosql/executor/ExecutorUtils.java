package ru.bmstu.sqlfornosql.executor;

import net.sf.jsqlparser.expression.Expression;
import org.medfoster.sqljep.ParseException;
import org.medfoster.sqljep.RowJEP;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.Column;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectField;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectFieldExpression;
import ru.bmstu.sqlfornosql.model.Row;
import ru.bmstu.sqlfornosql.model.RowType;
import ru.bmstu.sqlfornosql.model.Table;
import ru.bmstu.sqlfornosql.model.TableIterator;

import javax.annotation.Nonnull;
import java.sql.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static ru.bmstu.sqlfornosql.executor.Executor.FORBIDDEN_STRINGS;
import static ru.bmstu.sqlfornosql.executor.Executor.IDENT_REGEXP;

public class ExecutorUtils {
    private static final String SUPPORT_TABLE = "support_table_";

    public static HashMap<String, Integer> getIdentMapping(String expression) {
        Matcher matcher = IDENT_REGEXP.matcher(expression.replaceAll("'.*'", ""));
        HashMap<String, Integer> mapping = new HashMap<>();
        int index = 0;
        while (matcher.find()) {
            if (!FORBIDDEN_STRINGS.contains(matcher.group(1).toUpperCase())) {
                mapping.put(matcher.group(1).toLowerCase(), index++);
            }
        }

        return mapping;
    }

    public static RowJEP prepareSqlJEP(Expression expression, HashMap<String, Integer> colMapping) {
        RowJEP sqljep = new RowJEP(expression.toString().toLowerCase());
        try {
            sqljep.parseExpression(colMapping);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Can't parse expression: " + expression, e);
        }

        return sqljep;
    }

    public static Comparable getValue(Row row, SelectField key) {
        return (Comparable) row.getObject(key);
    }

    public static Comparable getValue(Row row, String key) {
        return (Comparable) row.getObject(key);
    }

    public static List<String> getIdentsFromString(String str) {
        Matcher matcher = IDENT_REGEXP.matcher(str.replaceAll("'.*'", ""));
        List<String> idents = new ArrayList<>();
        while (matcher.find()) {
            if (!FORBIDDEN_STRINGS.contains(matcher.group(1).toUpperCase())) {
                idents.add(matcher.group(1));
            }
        }

        return idents;
    }

    static void insertInH2SupportTable(Iterator<Table> tables, String supportTableName, Connection connection) throws SQLException {
        try {
            Table table;
            try (Statement statement = connection.createStatement()) {
                table = createTable(tables, statement, supportTableName);
            }

            try (Statement statement = connection.createStatement()) {
                insertValues(table, statement, supportTableName);
                while (tables.hasNext()) {
                    table = tables.next();
                    insertValues(table, statement, supportTableName);
                }
            }

            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }

    private static Table createTable(Iterator<Table> tables, Statement statement, String suppoertTableName) throws SQLException {
        StringBuilder query = new StringBuilder("CREATE TABLE " + suppoertTableName + " ( ");
        int i = 0;
        Table table = tables.next();
        for (Map.Entry<SelectField, RowType> field : table.getTypeMap().entrySet()) {
            query.append('"').append(field.getKey().getQualifiedContent().toLowerCase()).append('"').append(" ").append(field.getValue().getSqlName());
            if (i < table.getTypeMap().size() - 1) {
                query.append(", ");
                i++;
            }
        }
        query.append(");");
        statement.execute(query.toString());
        return table;
    }

    private static void insertValues(Table table, Statement statement, String supportTableName) throws SQLException {
        if (table.size() > 0) {
            StringBuilder query = new StringBuilder("INSERT INTO " + supportTableName + " VALUES ");
            int i = 0;
            for (Row row : table.getRows()) {
                query.append('(');
                int j = 0;
                for (SelectField selectField : table.getColumns()) {
                    switch (table.getType(selectField)) {
                        case STRING:
                            if (row.getObject(selectField) == null) {
                                query.append(row.getObject(selectField));
                            } else {
                                query.append("'").append(row.getString(selectField)).append("'");
                            }
                            break;
                        case DATE:
                            if (row.getObject(selectField) == null) {
                                query.append(row.getObject(selectField));
                            } else {
                                query
                                        .append("'")
                                        .append(row.getDate(selectField).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                                        .append("'")
                                        .append("::")
                                        .append("TIMESTAMP");
                            }
                            break;
                        case BOOLEAN:
                            query.append(row.getBool(selectField));
                            break;
                        case DOUBLE:
                            query.append(row.getDouble(selectField));
                            break;
                        case INT:
                            query.append(row.getInt(selectField));
                            break;
                        case NULL:
                            query.append("null");
                            break;
                        default:
                            throw new IllegalStateException("Unknown type");
                    }

                    if (j < table.getColumns().size() - 1) {
                        query.append(", ");
                        j++;
                    }
                }
                query.append(")");
                if (i < table.getRows().size() - 1) {
                    query.append(", ");
                    i++;
                }
            }
            statement.execute(query.toString());
        }
    }

    private static void dropTable(Statement statement, String supportTableName) throws SQLException {
        statement.execute("DROP TABLE IF EXISTS " + supportTableName);
    }

    static void dropSupportTable(String supportTableName) {
        try (
                Connection connection = DriverManager.getConnection(
                        "jdbc:h2:~/sqlForNoSql;AUTO_SERVER=TRUE",
                        "h2",
                        ""
                );
                Statement statement = connection.createStatement()
        ) {
            dropTable(statement, supportTableName);
        } catch (SQLException e) {
            throw new IllegalStateException("Can't drop support table: " + supportTableName, e);
        }
    }

    static String createBaseQuery(SqlHolder holder, String supportTableName) {
        String query = "SELECT " +
                holder.getSelectFields().stream()
                        .map(selectField -> {
                            if (selectField instanceof Column
                                    && ((Column) selectField).getAlias() != null)
                            {
                                return selectField.getQuotedFullQualifiedContent() +
                                        " AS \"" + ((Column) selectField).getAlias() + "\"";
                            } else if (selectField instanceof SelectFieldExpression
                                    && ((SelectFieldExpression) selectField).getAlias() != null)
                            {
                                return selectField.getQuotedFullQualifiedContent() +
                                        " AS \"" + ((SelectFieldExpression) selectField).getAlias() + "\"";
                            } else {
                                return selectField.getQuotedFullQualifiedContent();
                            }
                        })
                        .map(String::toLowerCase)
                        .collect(Collectors.joining(", ")) +
                " FROM " + supportTableName;

        if (holder.getWhereClause() != null) {
            List<String> idents = ExecutorUtils.getIdentsFromString(holder.getWhereClause().toString());
            String whereQuery = " WHERE " + holder.getWhereClause().toString();
            for (String ident : idents) {
                whereQuery = whereQuery.replace(ident, "\"" + ident.toLowerCase() + "\"");
            }

            query += whereQuery;
        }
        return query;
    }

    static class H2Iterator implements Iterator<ResultSet>, Iterable<ResultSet> {
        private Statement statement;
        private String query;
        private int offsetIndex;
        private long lastBatchSize;

        H2Iterator(Statement statement, String query) {
            this.statement = statement;
            this.query = query;
            this.lastBatchSize = TableIterator.BATCH_SIZE;
            this.offsetIndex = 0;
        }

        @Override
        @Nonnull
        public Iterator<ResultSet> iterator() {
            try {
                return new H2Iterator(
                        DriverManager
                                .getConnection(
                                        "jdbc:h2:~/sqlForNoSql;AUTO_SERVER=TRUE",
                                        "h2",
                                        "")
                                .createStatement(
                                        ResultSet.TYPE_SCROLL_INSENSITIVE,
                                        ResultSet.CONCUR_READ_ONLY
                                ),
                        query);
            } catch (SQLException e) {
                throw new IllegalStateException("Can't open connection", e);
            }
        }

        @Override
        public boolean hasNext() {
            return lastBatchSize >= TableIterator.BATCH_SIZE;
        }

        @Override
        public ResultSet next() {
            String resultQuery;
            if (offsetIndex > 0) {
                resultQuery = query + " LIMIT " + TableIterator.BATCH_SIZE + " OFFSET " + TableIterator.BATCH_SIZE * offsetIndex;
            } else {
                resultQuery = query + " LIMIT " + TableIterator.BATCH_SIZE;
            }
            offsetIndex++;
            try {
                ResultSet resultSet = statement.executeQuery(resultQuery);
                resultSet.last();
                lastBatchSize = resultSet.getRow();
                resultSet.beforeFirst();
                return resultSet;
            } catch (SQLException e) {
                throw new IllegalStateException("Can't perform query in h2: " + query, e);
            }
        }
    }

    static String createSupportTableName() {
        return SUPPORT_TABLE + System.currentTimeMillis();
    }
}
