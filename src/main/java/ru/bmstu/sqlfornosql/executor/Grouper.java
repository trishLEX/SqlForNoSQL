package ru.bmstu.sqlfornosql.executor;

import java.util.Collection;

import ru.bmstu.sqlfornosql.model.Row;
import ru.bmstu.sqlfornosql.model.Table;

public class Grouper {
    public static Table group(Table table, Collection<String> groupBys, Collection<String> columns) {
        Table result = new Table();
        for (Row row : table.getRows()) {

        }

        throw new UnsupportedOperationException();
    }
}
