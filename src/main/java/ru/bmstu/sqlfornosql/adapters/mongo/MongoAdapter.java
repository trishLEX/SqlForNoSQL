package ru.bmstu.sqlfornosql.adapters.mongo;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;

import static ru.bmstu.sqlfornosql.adapters.sql.SqlUtils.fillSqlMeta;

public class MongoAdapter {
    private static final Logger logger = LogManager.getLogger(MongoAdapter.class);

    public MongoHolder translate(String sql) {
        logger.info("translating sql: " + sql);

        MongoHolder mongoHolder;
        SqlHolder sqlHolder = fillSqlMeta(sql);

        mongoHolder = MongoHolder.createBySql(sqlHolder);

        validate(sqlHolder);

        return mongoHolder;
    }

    public MongoHolder translate(SqlHolder sqlHolder) {
        logger.info("translating sql: " + sqlHolder.getSqlQuery());

        validate(sqlHolder);

        return MongoHolder.createBySql(sqlHolder);
    }

    private void validate(SqlHolder sqlHolder) {
        Preconditions.checkArgument(sqlHolder.getJoin() == null,
                "Joins are not supported.  Only one simple table name is supported.");
    }
}
