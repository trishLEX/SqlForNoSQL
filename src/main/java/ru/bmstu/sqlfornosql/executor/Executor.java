package ru.bmstu.sqlfornosql.executor;

import ru.bmstu.sqlfornosql.SqlUtils;
import ru.bmstu.sqlfornosql.adapters.mongo.SqlHolder;

public class Executor {
    public void execute(String sql) {
        SqlHolder sqlHolder = SqlUtils.fillSqlMeta(sql);

    }
}
