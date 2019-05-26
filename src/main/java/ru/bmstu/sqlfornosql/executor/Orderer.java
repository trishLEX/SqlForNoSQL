package ru.bmstu.sqlfornosql.executor;

import ru.bmstu.sqlfornosql.adapters.postgres.PostgresMapper;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.OrderableSelectField;
import ru.bmstu.sqlfornosql.model.Table;
import ru.bmstu.sqlfornosql.model.TableIterator;

import java.sql.*;
import java.util.Iterator;
import java.util.stream.Collectors;

import static ru.bmstu.sqlfornosql.executor.ExecutorUtils.*;

public class Orderer {
    private static final PostgresMapper MAPPER = new PostgresMapper();

    static {
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Can't load driver", e);
        }
    }

    public static TableIterator orderInDb(SqlHolder holder, Iterator<Table> tables, String supportTableName) {
        try (Connection connection = DriverManager.getConnection(
                "jdbc:h2:~/sqlForNoSql;AUTO_SERVER=TRUE",
                "h2",
                "")
        ) {
            connection.setAutoCommit(false);

            insertInH2SupportTable(tables, supportTableName, connection);

            ExecutorUtils.H2Iterator rs = order(holder, connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY), supportTableName);

            TableIterator result = MAPPER.mapResultSet(rs, holder);
            result.setAfterAll(() -> dropSupportTable(supportTableName));
            return result;
        } catch (SQLException e) {
            throw new IllegalStateException("Can't perform h2 query", e);
        }
    }

    private static ExecutorUtils.H2Iterator order(
            SqlHolder holder,
            Statement statement,
            String supportTableName
    ) {
        String query = createBaseQuery(holder, supportTableName);

        if (!holder.getOrderBys().isEmpty()) {
            query += " ORDER BY" +
                    holder.getOrderBys().stream()
                            .map(OrderableSelectField::getQuotedFullQualifiedContent)
                            .map(String::toLowerCase)
                            .collect(Collectors.joining(", "));
        }

        return new ExecutorUtils.H2Iterator(statement, query);
    }
}
