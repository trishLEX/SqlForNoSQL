package ru.bmstu.sqlfornosql.adapters.sql.selectfield;

public class AllColumns extends SelectField {
    public static final AllColumns ALL_COLUMNS = new AllColumns();
    private static final String ALL_COLUMNS_STR = "*";

    private AllColumns() {
        super(ALL_COLUMNS_STR);
    }

    @Override
    public String getQualifiedContent() {
        return ALL_COLUMNS_STR;
    }

    @Override
    public String getNonQualifiedContent() {
        return getQualifiedContent();
    }

    @Override
    public String toString() {
        return ALL_COLUMNS_STR;
    }

    @Override
    protected void updateQualifiedName() {
        //do nothing
    }

    @Override
    public String getQualifiedIdent() {
        return getQualifiedContent();
    }

    @Override
    public String getNonQualifiedIdent() {
        return getQualifiedContent();
    }

    @Override
    public String getNativeInDbName() {
        return toString();
    }

    @Override
    public String getUserInputIdent() {
        return getNonQualifiedIdent();
    }

    @Override
    public String getQuotedFullQualifiedContent() {
        return getQualifiedContent();
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return ALL_COLUMNS_STR.hashCode();
    }
}
