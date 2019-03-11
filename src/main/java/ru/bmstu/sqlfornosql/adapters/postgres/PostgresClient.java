package ru.bmstu.sqlfornosql.adapters.postgres;

import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.model.Table;

import java.sql.*;

public class PostgresClient implements AutoCloseable {
    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Can't load driver", e);
        }
    }

    private Connection connection;

    public PostgresClient(String host, int port, String user, String password, String database) {
        try {
            connection = DriverManager.getConnection(String.format("jdbc:postgresql://%s:%d/%s", host, port, database), user, password);
        } catch (SQLException e) {
            throw new IllegalStateException("Can't open connection", e);
        }
    }

    public Table executeQuery(SqlHolder query) {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query.toString());

            return new PostgresMapper().mapResultSet(resultSet);

        } catch (SQLException e) {
            throw new IllegalStateException("Can't execute query", e);
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new IllegalStateException("Can't close connection", e);
        }
    }
}
