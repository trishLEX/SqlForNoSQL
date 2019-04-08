package ru.bmstu.sqlfornosql.adapters.sql.selectfield;

import net.sf.jsqlparser.statement.select.FromItem;

public abstract class SelectField {
    public abstract String getQualifiedContent();
    public abstract String getNonQualifiedContent();
    public abstract String getQualifiedIdent();
    public abstract String getNonQualifiedIdent();

    private FromItem source;
    private String columnName;

    public SelectField(String columnName) {
        this.columnName = columnName;
    }

    public void setSource(FromItem source) {
        this.source = source;
    }

    public FromItem getSource() {
        return source;
    }

    public String getColumnName() {
        return columnName;
    }

    public abstract boolean equals(Object o);

    public abstract int hashCode();
}
