package ru.bmstu.sqlfornosql.adapters.sql.selectfield;

public class SelectFieldExpression extends SelectField {
    private Column column;
    private SqlFunction function;
    private String alias;

    public SelectFieldExpression(SqlFunction function, String qualifiedName) {
        super(function.name().toLowerCase() + "(" + qualifiedName.toLowerCase() + ")");
        this.column = new Column(qualifiedName);
        this.function = function;
    }

    public SelectFieldExpression(SqlFunction function, String qualifiedName, String alias) {
        this(function, qualifiedName);
        this.alias = alias;
    }

    public Column getColumn() {
        return column;
    }

    public SqlFunction getFunction() {
        return function;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public String getQualifiedContent() {
        return toString();
    }

    @Override
    public String getNonQualifiedContent() {
        return function.name().toLowerCase() + "(" + column.getNonQualifiedContent() + ")";
    }

    @Override
    public String getNonQualifiedIdent() {
        return column.getNonQualifiedIdent();
    }

    @Override
    public String getQualifiedIdent() {
        return column.getQualifiedIdent();
    }

    @Override
    public String toString() {
        return function.name().toLowerCase() + "(" + column.getQualifiedContent() + ")";
    }
}
