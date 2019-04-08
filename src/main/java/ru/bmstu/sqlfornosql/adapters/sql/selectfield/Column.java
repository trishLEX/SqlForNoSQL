package ru.bmstu.sqlfornosql.adapters.sql.selectfield;

public class Column extends SelectField {
    private String qualifiedName;
    private String nonQualifiedName;

    private String alias;

    public Column(String qualifiedName) {
        super(qualifiedName);
        this.qualifiedName = qualifiedName;
        this.nonQualifiedName = makeNonQualifiedName(qualifiedName);
    }

    public Column(String qualifiedName, String alias) {
        this(qualifiedName);
        this.alias = alias;
    }

    @Override
    public String getQualifiedContent() {
        return qualifiedName;
    }

    @Override
    public String getNonQualifiedContent() {
        return nonQualifiedName;
    }

    @Override
    public String getQualifiedIdent() {
        return qualifiedName;
    }

    @Override
    public String getNonQualifiedIdent() {
        return nonQualifiedName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public String toString() {
        return qualifiedName;
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
}
