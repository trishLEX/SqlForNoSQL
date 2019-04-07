package ru.bmstu.sqlfornosql.model;

import java.time.LocalDateTime;
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

    //TODO make private, сделать API для нормального добавления строк
    public Table add(Row row) {
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
        for (String key : keys) {
            boolean removed = columns.remove(key);
            typeMap.remove(key);
            if (!removed) {
                columns.remove(key.toLowerCase());
                typeMap.remove(key.toLowerCase());
            }
        }
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

    public void sort(LinkedHashMap<String, Boolean> orderByMap) {
        rows.sort((o1, o2) -> {
            for (Map.Entry<String, Boolean> orderEntry : orderByMap.entrySet()) {
                int compare = compareValues(o1.getObject(orderEntry.getKey()), o2.getObject(orderEntry.getKey()), getType(orderEntry.getKey()));
                if (compare != 0) {
                    return orderEntry.getValue() ? compare : -compare;
                }
            }

            return 0;
        });
    }

    private int compareValues(Object a, Object b, RowType type) {
        if (a == null && b == null) {
            return 0;
        } else if (a == null) {
            return 1;
        } else if (b == null) {
            return -1;
        }
        switch (type) {
            case NULL:
                //TODO LOG IT
                System.out.println("LOG HERE");
                return 0;
            case INT: {
                Integer o1 = (Integer) a;
                Integer o2 = (Integer) b;
                return o1.compareTo(o2);
            }
            case DOUBLE: {
                Double o1 = (Double) a;
                Double o2 = (Double) b;
                return o1.compareTo(o2);
            }
            case BOOLEAN: {
                throw new IllegalArgumentException("Trying to sort by boolean");
            }
            case DATE: {
                LocalDateTime o1 = (LocalDateTime) a;
                LocalDateTime o2 = (LocalDateTime) b;
                return o1.compareTo(o2);
            }
            case STRING:
                String o1 = (String) a;
                String o2 = (String) b;
                return o1.compareTo(o2);
            default:
                throw new IllegalStateException("Unknown type: " + type);
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
