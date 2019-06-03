package ru.bmstu.sqlfornosql.adapters.postgres;

import org.apache.commons.dbcp2.BasicDataSource;
import ru.bmstu.sqlfornosql.adapters.AbstractClient;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.model.Table;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgresClient extends AbstractClient {
    private static final int MAX_CONNECTIONS = 3;

    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Can't load driver", e);
        }
    }
//    private String connectionString;
//    private String password;
//    private String user;
    //private Connection connection;
    private BasicDataSource connectionPool;

    public PostgresClient(String host, int port, String user, String password, String database) {
        String connectionString = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);

//        this.connectionString = connectionString;
//        this.user = user;
//        this.password = password;

        connectionPool = new BasicDataSource();
        connectionPool.setUsername(user);
        connectionPool.setPassword(password);
        connectionPool.setDriverClassName("org.postgres.Driver");
        connectionPool.setUrl(connectionString);
        connectionPool.setInitialSize(MAX_CONNECTIONS);

        //connection = DriverManager.getConnection(connectionString, user, password);
    }

    @Override
    public Table executeQuery(SqlHolder query) {
        query.updateSelectItems();
        try {
            Statement statement = connectionPool.getConnection().createStatement();
            ResultSet resultSet = statement.executeQuery(query.getSqlQuery());

            return new PostgresMapper().mapResultSet(resultSet, query);
        } catch (SQLException e) {
            throw new IllegalStateException("Can't execute query: " + query.getSqlQuery(), e);
        }
    }

    @Override
    public void close() {
        try {
            connectionPool.close();
        } catch (SQLException e) {
            throw new IllegalStateException("Can't close connection", e);
        }
    }
}
