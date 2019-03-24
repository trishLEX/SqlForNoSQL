package ru.bmstu.sqlfornosql.model;

import java.util.*;

public class Table {
    private List<Row> rows;
    private Set<String> columns;

    public Table() {
        rows = new ArrayList<>();
        columns = new HashSet<>();
    }

    public Table add(String key, Object value, RowType type) {
        columns.add(key);
        rows.add(new Row().add(key, value, type));
        return this;
    }

    public Table add(Row row) {
        rows.add(row);
        columns.addAll(row.getColumns());
        return this;
    }

    public List<Row> getRows() {
        return rows;
    }

    public Set<String> getColumns() {
        return columns;
    }

    public boolean isEmpty() {
        for (Row row : rows) {
            if (!row.isEmpty()) {
                return false;
            }
        }

        return true;
    }

    public void remove(Collection<String> keys) {
        columns.removeAll(keys);
        for (Row row : rows) {
            row.remove(keys);
        }
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder("ROWS:\n");
        for (Row row : rows) {
            res.append(row.toString()).append("\n");
        }
        res.append("\n");
        return res.toString();
    }
}
