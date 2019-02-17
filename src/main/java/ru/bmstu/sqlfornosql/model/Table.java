package ru.bmstu.sqlfornosql.model;

import java.util.ArrayList;
import java.util.List;

public class Table {
    private List<Row> rows;

    public Table() {
        rows = new ArrayList<>();
    }

    public Table add(String key, Object value, RowType type) {
        rows.add(new Row().add(key, value, type));
        return this;
    }

    public Table add(Row row) {
        rows.add(row);
        return this;
    }

    public List<Row> getRows() {
        return rows;
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
