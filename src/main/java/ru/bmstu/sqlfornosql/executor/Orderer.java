package ru.bmstu.sqlfornosql.executor;

import org.springframework.stereotype.Component;
import ru.bmstu.sqlfornosql.adapters.postgres.PostgresMapper;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.OrderableSelectField;
import ru.bmstu.sqlfornosql.model.Table;
import ru.bmstu.sqlfornosql.model.TableIterator;

import java.sql.*;
import java.util.Iterator;
import java.util.stream.Collectors;

import static ru.bmstu.sqlfornosql.executor.ExecutorUtils.*;

@Component
public class Orderer {
    private static final PostgresMapper MAPPER = new PostgresMapper();
    private final String h2Database;
    private final String h2User;
    private final String h2Password;

    public Orderer(ExecutorConfig config) {
        this.h2Database = config.getH2Database();
        this.h2User = config.getH2User();
        this.h2Password = config.getH2Password();
    }

    static {
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Can't load driver", e);
        }
    }

    public TableIterator orderInDb(SqlHolder holder, Iterator<Table> tables, String supportTableName) {
        try (Connection connection = DriverManager.getConnection(
                String.format("jdbc:h2:~/%s;AUTO_SERVER=TRUE", h2Database),
                h2User,
                h2Password)
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

    private ExecutorUtils.H2Iterator order(
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
