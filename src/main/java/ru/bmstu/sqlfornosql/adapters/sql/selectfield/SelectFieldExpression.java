package ru.bmstu.sqlfornosql.adapters.sql.selectfield;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Objects;

@ParametersAreNonnullByDefault
public class SelectFieldExpression extends SelectField {
    private Column column;
    private SqlFunction function;
    @Nullable
    private String alias;

    public SelectFieldExpression(SqlFunction function, String userInput) {
        super(function.name().toLowerCase() + "(" + userInput + ")");
        this.column = new Column(userInput);
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

    @Nullable
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
    protected void updateQualifiedName() {
        column.setSource(getSource());
    }

    @Override
    public String getNativeInDbName() {
        return function.name() + "(" + column.getNativeInDbName() + ")";
    }

    @Override
    public String getUserInputIdent() {
        return column.getUserInputIdent();
    }

    @Override
    public String getQuotedFullQualifiedContent() {
        return function.name() + "(" + column.getQuotedFullQualifiedContent() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }

        SelectFieldExpression other = (SelectFieldExpression) o;
        return this.function.equals(other.function) && this.column.equals(other.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, column);
    }
}
