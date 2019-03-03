package ru.bmstu.sqlfornosql.model;

public class DatabaseName {
    private DbType dbType;
    private String database;
    private String schema;
    private String table;

    public DatabaseName(String name) {
        String[] parts = name.split("\\.");
        if (parts.length < 3) {
            throw new IllegalArgumentException("DbType, db and table must be specified at least");
        }

        if (parts.length == 3) {
            this.dbType = DbType.valueOf(parts[0].toUpperCase());
            this.database = parts[1];
            this.table = parts[2];
        } else if (parts.length == 4) {
            this.dbType = DbType.valueOf(parts[0].toUpperCase());
            this.database = parts[1];
            this.schema = parts[2];
            this.table = parts[3];
        } else {
            throw new IllegalArgumentException("Name: " + name + " is malformed");
        }
    }

    public DbType getDbType() {
        return dbType;
    }

    public String getDatabaseName() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }
}
