package ru.bmstu.sqlfornosql.adapters.postgres;

import ru.bmstu.sqlfornosql.adapters.AbstractClient;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.model.Table;

import java.sql.*;

public class PostgresClient extends AbstractClient {
    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Can't load driver", e);
        }
    }
    private String connectionString;
    private String password;
    private String user;
    private Connection connection;

    public PostgresClient(String host, int port, String user, String password, String database) {
        try {
            String connectionString = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
            this.connectionString = connectionString;
            this.user = user;
            this.password = password;
            connection = DriverManager.getConnection(connectionString, user, password);
        } catch (SQLException e) {
            throw new IllegalStateException("Can't open connection", e);
        }
    }

    @Override
    public Table executeQuery(SqlHolder query) {
        query.updateSelectItems();
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query.getSqlQuery());

            return new PostgresMapper().mapResultSet(resultSet, query);

        } catch (SQLException e) {
            throw new IllegalStateException("Can't execute query", e);
        }
    }

    @Override
    public void open() {
        try {
            connection = DriverManager.getConnection(connectionString, user, password);
        } catch (SQLException e) {
            throw new IllegalStateException("Can't open connection", e);
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
