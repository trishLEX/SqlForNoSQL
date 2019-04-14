package ru.bmstu.sqlfornosql.adapters.sql.selectfield;

public class Column extends SelectField {
    private String alias;

    public Column(String userInput) {
        super(userInput);
    }

    public Column(String userInput, String alias) {
        this(userInput);
        this.alias = alias;
    }

    @Override
    public String getQualifiedContent() {
        return fullQualifiedName;
    }

    @Override
    public String getNonQualifiedContent() {
        return nonQualifiedName;
    }

    @Override
    public String getQualifiedIdent() {
        return getQualifiedContent();
    }

    @Override
    public String getNonQualifiedIdent() {
        return getNonQualifiedContent();
    }

    @Override
    public String getUserInputIdent() {
        return userInputName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }

        Column other = (Column) o;
        return this.fullQualifiedName.equalsIgnoreCase(other.fullQualifiedName);
    }

    @Override
    public int hashCode() {
        return fullQualifiedName.toLowerCase().hashCode();
    }
}
