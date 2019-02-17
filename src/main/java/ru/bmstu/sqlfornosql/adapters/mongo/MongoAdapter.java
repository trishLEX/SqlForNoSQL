package ru.bmstu.sqlfornosql.adapters.mongo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import ru.bmstu.sqlfornosql.SqlUtils;

import java.util.List;
import java.util.stream.Collectors;

public class MongoAdapter {
    private SqlHolder sqlHolder;

    public MongoHolder translate(String sql) {
        MongoHolder mongoHolder;
        sqlHolder = fillSqlMeta(sql);

        mongoHolder = MongoHolder.createBySql(sqlHolder);
        validate();

        return mongoHolder;
    }

    private SqlHolder fillSqlMeta(String sql) {
        boolean isDistinct;
        boolean isCountAll;
        FromItem table;
        long limit;
        Expression whereClause;
        List<SelectItem> selectItems;
        List<Join> joins;
        List<String> groupBys;
        Expression havingClause;
        List<OrderByElement> orderByElements;

        try {
            Statement statement = CCJSqlParserUtil.parse(sql);

            if (Select.class.isAssignableFrom(statement.getClass())) {
                PlainSelect plainSelect = (PlainSelect)(((Select)statement).getSelectBody());
                Preconditions.checkArgument(plainSelect != null, "Can't parse statement");

                isDistinct = plainSelect.getDistinct() != null;
                isCountAll = SqlUtils.isCountAll(plainSelect.getSelectItems());
                table = plainSelect.getFromItem();
                limit = SqlUtils.getLimit(plainSelect.getLimit());
                whereClause = plainSelect.getWhere();
                selectItems = plainSelect.getSelectItems();
                joins = plainSelect.getJoins();
                groupBys = SqlUtils.getGroupBy(plainSelect.getGroupByColumnReferences());
                havingClause = plainSelect.getHaving();
                orderByElements = plainSelect.getOrderByElements();

                return new SqlHolder()
                        .withDistinct(isDistinct)
                        .withCountAll(isCountAll)
                        .withTable(table)
                        .withLimit(limit)
                        .withWhere(whereClause)
                        .withSelectItems(selectItems)
                        .withJoins(joins)
                        .withGroupBy(groupBys)
                        .withHaving(havingClause)
                        .withOrderBy(orderByElements);
            } else {
                throw new IllegalArgumentException("Only select statements are supported");
            }
        } catch (JSQLParserException e) {
            throw new IllegalArgumentException("Can't parse statement", e);
        }
    }

    private void validate() {
        List<SelectItem> selectItems = sqlHolder.getSelectItems();
        List<SelectItem> filteredItems = Lists.newArrayList(
                selectItems.stream()
                        .filter(selectItem -> {
                            try {
                                if (selectItem instanceof SelectExpressionItem
                                        && ((SelectExpressionItem) selectItem).getExpression() instanceof Column) {
                                    return true;
                                }
                            } catch (NullPointerException e) {
                                return false;
                            }
                            return false;
                        })
                        .collect(Collectors.toList()));

//        Preconditions.checkArgument(!((selectItems.size() > 1 || SqlUtils.isSelectAll(selectItems))) && sqlHolder.isDistinct(),
//                "cannot run distinct one more than one column");
//        Preconditions.checkArgument(!(sqlHolder.getGroupBys().isEmpty()
//                        && selectItems.size() != filteredItems.size()
//                        && !SqlUtils.isSelectAll(selectItems)
//                        && !SqlUtils.isCountAll(selectItems)),
//                "illegal expression(s) found in select clause.  Only column names supported");
        Preconditions.checkArgument(sqlHolder.getJoins() == null || sqlHolder.getJoins().isEmpty(),
                "Joins are not supported.  Only one simple table name is supported.");
    }
}
