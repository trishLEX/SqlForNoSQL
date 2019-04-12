package ru.bmstu.sqlfornosql.adapters.sql.selectfield;

import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import ru.bmstu.sqlfornosql.adapters.mongo.MongoUtils;

public abstract class SelectField {
    public abstract String getQualifiedContent();
    public abstract String getNonQualifiedContent();
    public abstract String getQualifiedIdent();
    public abstract String getNonQualifiedIdent();

    protected FromItem source;
    protected String fullQualifiedName;
    protected String userInputName;
    protected String nonQualifiedName;

    public SelectField(String userInputName) {
        this.userInputName = userInputName;
        this.nonQualifiedName = MongoUtils.makeMongoColName(userInputName).toLowerCase();
    }

    public void setSource(FromItem source) {
        this.source = source;
        updateQualifiedName();
    }

    protected void updateQualifiedName() {
        if (getSource() instanceof SubSelect) {
            //TODO проверить обязателен ли subselect'у alias
            if (getSource().getAlias() == null){
                throw new IllegalStateException("SubSelects must have alias");
            } else {
                fullQualifiedName = getSource().getAlias().getName() + "." + nonQualifiedName;
            }
        } else {
            if (getSource().getAlias() == null) {
                fullQualifiedName = getSource().toString() + "." + nonQualifiedName;
            } else {
                fullQualifiedName = getSource().getAlias().getName() + "." + nonQualifiedName;
            }
        }
    }

    public FromItem getSource() {
        return source;
    }

    public String getFullQualifiedName() {
        return fullQualifiedName;
    }

    public String getNonQualifiedName() {
        return nonQualifiedName;
    }

    public String getUserInputName() {
        return userInputName;
    }

    public abstract boolean equals(Object o);

    public abstract int hashCode();
}
