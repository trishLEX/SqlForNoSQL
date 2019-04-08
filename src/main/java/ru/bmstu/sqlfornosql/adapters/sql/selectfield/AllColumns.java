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
        return ALL_COLUMNS_STR;
    }

    @Override
    public String toString() {
        return ALL_COLUMNS_STR;
    }

    @Override
    public String getQualifiedIdent() {
        return ALL_COLUMNS_STR;
    }

    @Override
    public String getNonQualifiedIdent() {
        return ALL_COLUMNS_STR;
    }
}
