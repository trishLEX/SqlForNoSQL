package ru.bmstu.sqlfornosql.model;

public enum RowType {
    BOOLEAN("BOOLEAN"),
    DATE("TIMESTAMP"),
    DOUBLE("DOUBLE"),
    INT("INT"),
    LONG("LONG"),
    STRING("VARCHAR"),
    NULL("OTHER");

    RowType(String sqlName) {
        this.sqlName = sqlName;
    }

    private String sqlName;

    public String getSqlName() {
        return sqlName;
    }
}
