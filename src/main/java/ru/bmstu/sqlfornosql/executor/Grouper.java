package ru.bmstu.sqlfornosql.executor;

import ru.bmstu.sqlfornosql.adapters.postgres.PostgresMapper;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectField;
import ru.bmstu.sqlfornosql.model.Row;
import ru.bmstu.sqlfornosql.model.RowType;
import ru.bmstu.sqlfornosql.model.Table;
import ru.bmstu.sqlfornosql.model.TableIterator;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.*;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ParametersAreNonnullByDefault
public class Grouper {
    private static final PostgresMapper MAPPER = new PostgresMapper();

    static {
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Can't load driver", e);
        }
    }

    public static TableIterator groupInDb(
            SqlHolder holder,
            Iterator<Table> tables,
            String supportTableName
    ) {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:~/sqlForNoSql;AUTO_SERVER=TRUE", "h2", "")) {
            connection.setAutoCommit(false);

            try {
                try (Statement statement = connection.createStatement()) {
                    dropTable(statement);
                }

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

            H2Iterator rs = group(holder, connection.createStatement(), supportTableName);

            return MAPPER.mapResultSet(rs, holder);
        } catch (SQLException e) {
            throw new IllegalStateException("Can't perform h2 query", e);
        }
    }

    private static void dropTable(Statement statement) throws SQLException {
        statement.execute("DROP TABLE IF EXISTS supportTable1");
    }

    private static H2Iterator group(
            SqlHolder holder,
            Statement statement,
            String supportTableName
    ) throws SQLException {
        String query = "SELECT " +
                holder.getSelectFields().stream()
                        .map(SelectField::getQuotedFullQualifiedContent)
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


        query += " GROUP BY " +
                holder.getGroupBys().stream()
                        .map(SelectField::getQuotedFullQualifiedContent)
                        .map(String::toLowerCase)
                        .collect(Collectors.joining(", "));

        if (holder.getHavingClause() != null) {
            List<String> idents = ExecutorUtils.getIdentsFromString(holder.getHavingClause().toString());
            String havingQuery = " HAVING " + holder.getHavingClause().toString();
            for (String ident : idents) {
                havingQuery = havingQuery.replace(ident, "\"" + ident.toLowerCase() + "\"");
            }

            query += havingQuery;
        }

        //TODO когда вместо OrderByElement будет SelectField дописать OrderBy

        return new H2Iterator(statement, query);
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

    //TODO Check that collections are sets
    @Deprecated
    public static Iterator<Table> group(
            SqlHolder holder,
            Iterator<Table> table,
            String supportTableName
    ) {
        return groupInDb(holder, table, supportTableName);
    }

    private static class H2Iterator implements Iterator<ResultSet>, Iterable<ResultSet> {
        private Statement statement;
        private String query;
        private int offsetIndex;
        private long lastBatchSize;

        private H2Iterator(Statement statement, String query) {
            this.statement = statement;
            this.query = query;
            this.lastBatchSize = TableIterator.BATCH_SIZE;
            this.offsetIndex = 0;
        }

        @Override
        @Nonnull
        public Iterator<ResultSet> iterator() {
            try {
                return new H2Iterator(DriverManager.getConnection("jdbc:h2:~/sqlForNoSql;AUTO_SERVER=TRUE", "h2", "").createStatement(), query);
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
            //TODO не писать OFFSET 0
            String resultQuery = query += " OFFSET " + TableIterator.BATCH_SIZE * offsetIndex + " LIMIT " + TableIterator.BATCH_SIZE;
            offsetIndex++;
            try {
                return statement.executeQuery(resultQuery);
            } catch (SQLException e) {
                throw new IllegalStateException("Can't perform query in h2: " + query, e);
            }
        }
    }
}
