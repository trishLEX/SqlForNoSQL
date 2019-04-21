package ru.bmstu.sqlfornosql.adapters.sql.selectfield;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import ru.bmstu.sqlfornosql.adapters.mongo.MongoUtils;

public abstract class SelectField {
    public abstract String getQualifiedContent();
    public abstract String getNonQualifiedContent();
    public abstract String getQualifiedIdent();
    public abstract String getNonQualifiedIdent();
    public abstract String getUserInputIdent();
    public abstract String getQuotedFullQualifiedContent();

    protected FromItem source;
    protected String fullQualifiedName;
    protected String userInputName;
    protected String nonQualifiedName;
    protected String nativeInDbName;

    public SelectField(String userInputName) {
        this.userInputName = userInputName;
        this.nonQualifiedName = MongoUtils.makeMongoColName(userInputName);
    }

    public void setSource(FromItem source) {
        this.source = source;
        updateQualifiedName();
    }

    public SelectField withSource(FromItem source) {
        this.source = source;
        updateQualifiedName();
        return this;
    }

    public void setFromItemAlias(Alias alias) {
        this.source.setAlias(alias);
        updateQualifiedName();
    }

    protected void updateQualifiedName() {
        if (getSource() instanceof SubSelect) {
            //TODO проверить обязателен ли subselect'у alias
            if (getSource().getAlias() == null){
                throw new IllegalStateException("SubSelects must have alias");
            } else {
                fullQualifiedName = getSource().getAlias().getName() + "." + nonQualifiedName;
                nativeInDbName = fullQualifiedName.substring(fullQualifiedName.indexOf('.') + 1);
            }
        } else {
            if (getSource().getAlias() == null) {
                fullQualifiedName = getSource().toString() + "." + nonQualifiedName;
            } else {
                fullQualifiedName = getSource().getAlias().getName() + "." + nonQualifiedName;
            }
            nativeInDbName = fullQualifiedName.substring(fullQualifiedName.indexOf('.') + 1);
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

    public String getNativeInDbName() {
        return nativeInDbName;
    }

    public abstract boolean equals(Object o);

    public abstract int hashCode();

    @Override
    public String toString() {
        return userInputName;
    }
}
