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

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public String toString() {
        return fullQualifiedName;
    }

    //TODO аналогичный метод в MongoUtils (дупликация кода)
    private String makeNonQualifiedName(String qualifiedName) {
        if (qualifiedName.contains(".")) {
            return qualifiedName.substring(qualifiedName.lastIndexOf('.') + 1);
        } else if (qualifiedName.equals("*")) {
            return qualifiedName;
        } else {
            throw new IllegalStateException("Name must be qualified");
        }
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
        return this.nonQualifiedName.equalsIgnoreCase(other.nonQualifiedName);
    }

    @Override
    public int hashCode() {
        return nonQualifiedName.toLowerCase().hashCode();
    }
}
