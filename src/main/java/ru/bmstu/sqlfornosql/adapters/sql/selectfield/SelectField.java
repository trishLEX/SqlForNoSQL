package ru.bmstu.sqlfornosql.adapters.sql.selectfield;

public interface SelectField {
    String getQualifiedContent();
    String getNonQualifiedContent();
    String getQualifiedIdent();
    String getNonQualifiedIdent();
}
