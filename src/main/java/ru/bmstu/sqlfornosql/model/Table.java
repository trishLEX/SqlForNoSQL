package ru.bmstu.sqlfornosql.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
