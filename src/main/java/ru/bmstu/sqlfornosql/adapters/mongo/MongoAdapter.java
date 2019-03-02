package ru.bmstu.sqlfornosql.adapters.mongo;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ru.bmstu.sqlfornosql.SqlUtils.fillSqlMeta;

public class MongoAdapter {
    private static final Logger logger = LogManager.getLogger(MongoAdapter.class);

    private SqlHolder sqlHolder;

    public MongoHolder translate(String sql) {
        logger.info("translating sql: " + sql);

        MongoHolder mongoHolder;
        sqlHolder = fillSqlMeta(sql);

        mongoHolder = MongoHolder.createBySql(sqlHolder);

        validate();

        return mongoHolder;
    }

    public MongoHolder translate(SqlHolder sqlHolder) {
        logger.info("translating sql: " + sqlHolder.toString());

        this.sqlHolder = sqlHolder;
        validate();

        return MongoHolder.createBySql(sqlHolder);
    }

    private void validate() {
        Preconditions.checkArgument(sqlHolder.getJoins() == null || sqlHolder.getJoins().isEmpty(),
                "Joins are not supported.  Only one simple table name is supported.");
    }
}
