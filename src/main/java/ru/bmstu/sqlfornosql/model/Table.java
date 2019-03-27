package ru.bmstu.sqlfornosql.model;

import java.util.*;

import static ru.bmstu.sqlfornosql.model.RowType.NULL;

public class Table {
    private Map<String, RowType> typeMap;
    private List<Row> rows;
    private Set<String> columns;

    public Table() {
        typeMap = new LinkedHashMap<>();
        rows = new ArrayList<>();
        columns = new LinkedHashSet<>();
    }

    public Table add(String key, Object value, RowType type) {
        columns.add(key);
        typeMap.put(key, type);
        rows.add(new Row(this).add(key, value));
        return this;
    }

    private Table add(Row row) {
        rows.add(row);
        columns.addAll(row.getColumns());
        return this;
    }

    public void setType(String column, RowType type) {
        if (type != NULL || typeMap.get(column) == NULL || typeMap.get(column) == null) {
            typeMap.put(column, type);
        }
    }

    public Table add(Row row, Map<String, RowType> types) {
        add(row);

        for (Map.Entry<String, RowType> typeEntry : types.entrySet()) {
            setType(typeEntry.getKey(), typeEntry.getValue());
        }

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

    public RowType getType(String key) {
        if (typeMap.containsKey(key)) {
            return typeMap.get(key);
        } else {
            throw new IllegalArgumentException("No column with name: " + key);
        }
    }

    public Map<String, RowType> getTypeMap() {
        return typeMap;
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
