package ru.bmstu.sqlfornosql.model;

import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public abstract class TableIterator implements Iterator<Table>, Iterable<Table> {
    public static final long BATCH_SIZE = 1;
    protected long lastBatchSize;
    private SqlHolder holder;

    protected List<Runnable> afterAll = new ArrayList<>();

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
                    afterAll.forEach(Runnable::run);
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
    public TableIterator(SqlHolder holder) {
        this.holder = holder;
        this.holder.setLimit(BATCH_SIZE);
        this.lastBatchSize = BATCH_SIZE;
    }

    public TableIterator() {
        this.lastBatchSize = BATCH_SIZE;
    }

    public void addAfterAll(Runnable afterAll) {
        this.afterAll.add(afterAll);
    }

    @Override
    @Nonnull
    public abstract Iterator<Table> iterator();

    @Override
    public boolean hasNext() {
        return lastBatchSize >= BATCH_SIZE;
    }

    @Override
    public abstract Table next();
}
