package ru.bmstu.sqlfornosql.adapters;

import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.executor.Executor;
import ru.bmstu.sqlfornosql.model.Table;
import ru.bmstu.sqlfornosql.model.TableIterator;

import java.util.concurrent.CompletableFuture;

public abstract class AbstractClient implements AutoCloseable {
    public abstract TableIterator executeQuery(SqlHolder holder);

    public CompletableFuture<TableIterator> executeQueryLazy(SqlHolder holder) {
        return CompletableFuture.supplyAsync(() -> executeQuery(holder), Executor.EXECUTOR);
    }

//    public abstract void open();
    public abstract void close();
}
