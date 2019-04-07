package ru.bmstu.sqlfornosql.adapters.sql.selectfield;

public enum SqlFunction {
    MAX,
    MIN,
    SUM,
    AVG,
    COUNT;

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
