package ru.bmstu.sqlfornosql.adapters.postgres;

import org.apache.commons.dbcp2.BasicDataSource;
import ru.bmstu.sqlfornosql.adapters.AbstractClient;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.model.Table;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgresClient extends AbstractClient {
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
//    private Connection connection;
    private BasicDataSource connectionPool;
    private volatile ThreadLocal<Connection> connectionThreadLocal;

    public PostgresClient(String host, int port, String user, String password, String database) {
            String connectionString = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
//            this.connectionString = connectionString;
//            this.user = user;
//            this.password = password;
//            connection = DriverManager.getConnection(connectionString, user, password);
            connectionPool = new BasicDataSource();
            connectionPool.setUsername(user);
            connectionPool.setPassword(password);
            connectionPool.setDriverClassName("org.postgresql.Driver");
            connectionPool.setUrl(connectionString);
            connectionPool.setInitialSize(2);
            connectionThreadLocal = new ThreadLocal<>();
    }

    @Override
    public Table executeQuery(SqlHolder query) {
        query.updateSelectItems();
        try {
            if (connectionThreadLocal.get() == null) {
                connectionThreadLocal.set(connectionPool.getConnection());
            }
            Statement statement = connectionThreadLocal.get().createStatement();
            ResultSet resultSet = statement.executeQuery(query.getSqlQuery());

            return new PostgresMapper().mapResultSet(resultSet, query);

        } catch (SQLException e) {
            throw new IllegalStateException("Can't execute query: " + query.getSqlQuery(), e);
        }
    }

//    @Override
//    public void open() {
//        try {
//            connectionPool.op
//        } catch (SQLException e) {
//            throw new IllegalStateException("Can't open connection", e);
//        }
//    }

    @Override
    public void close() {
        try {
            connectionPool.close();
        } catch (SQLException e) {
            throw new IllegalStateException("Can't close connection", e);
        }
    }
}
