package ru.bmstu.sqlfornosql.model;

import ru.bmstu.sqlfornosql.adapters.AbstractClient;
import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class TableIterator implements Iterator<Table>, Iterable<Table> {
    public static final long BATCH_SIZE = 1;
    private long lastBatchSize;
    private AbstractClient client;
    private SqlHolder holder;
    private int offsetIndex;
    private Table lastTable;
    private Runnable afterAll;

    public static TableIterator ofTable(Table table) {
        return new TableIterator() {
            private boolean wasIterated = false;

            @Nonnull
            @Override
            public Iterator<Table> iterator() {
                return ofTable(table);
            }

            @Override
            public Table next() {
                if (hasNext()) {
                    wasIterated = true;
                    return table;
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public boolean hasNext() {
                return !wasIterated;
            }
        };
    }

    //TODO поддержать пользовательские limit и offset
    public TableIterator(AbstractClient client, SqlHolder holder) {
        this.client = client;
        this.holder = holder;
        this.holder.setLimit(BATCH_SIZE);
        this.lastBatchSize = BATCH_SIZE;
        this.offsetIndex = 0;
    }

    public TableIterator() {
        this.lastBatchSize = BATCH_SIZE;
        this.offsetIndex = 0;
    }

    public void setAfterAll(Runnable afterAll) {
        this.afterAll = afterAll;
    }

    @Override
    @Nonnull
    public Iterator<Table> iterator() {
        client.open();
        return new TableIterator(client, holder);
    }

    @Override
    public boolean hasNext() {
        return lastBatchSize >= BATCH_SIZE;
    }

    @Override
    public Table next() {
        if (hasNext()) {
            if (offsetIndex != 0) {
                lastTable.clear();
                holder.setOffset(BATCH_SIZE * offsetIndex);
                holder.setLimit(BATCH_SIZE);
            }
            offsetIndex++;

            holder.setLimit(BATCH_SIZE);
            lastTable = client.executeQuery(holder);
            lastBatchSize = lastTable.size();
            holder.setLimit(-1);
            holder.setOffset(-1);

            if (!hasNext()) {
                if (afterAll != null) {
                    afterAll.run();
                }
                client.close();
            }
            return lastTable;
        }

        throw new NoSuchElementException("There are no more rows");
    }
}
