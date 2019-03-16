package ru.bmstu.sqlfornosql.model;

import com.google.common.base.Joiner;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class Row {
    private Map<String, RowType> typeMap;
    private Map<String, Object> values;

    public Row() {
        typeMap = new LinkedHashMap<>();
        values = new LinkedHashMap<>();
    }

    public Row add(String key, Object value, RowType type) {
        typeMap.put(key, type);
        values.put(key, value);
        return this;
    }

    public Set<String> getColumns() {
        return values.keySet();
    }

    public Boolean getBool(String key) {
        if (typeMap.containsKey(key)) {
            if (typeMap.get(key) == RowType.BOOLEAN) {
                return (Boolean) values.get(key);
            } else {
                throw new IllegalArgumentException("Column '" + key + "' has type: " + typeMap.get(key));
            }
        } else {
            throw new IllegalArgumentException("No column with name: " + key);
        }
    }

    public LocalDateTime getDate(String key) {
        if (typeMap.containsKey(key)) {
            if (typeMap.get(key) == RowType.DATE) {
                return (LocalDateTime) values.get(key);
            } else {
                throw new IllegalArgumentException("Column '" + key + "' has type: " + typeMap.get(key));
            }
        } else {
            throw new IllegalArgumentException("No column with name: " + key);
        }
    }

    public Double getDouble(String key) {
        if (typeMap.containsKey(key)) {
            if (typeMap.get(key) == RowType.DOUBLE) {
                return (Double) values.get(key);
            } else {
                throw new IllegalArgumentException("Column '" + key + "' has type: " + typeMap.get(key));
            }
        } else {
            throw new IllegalArgumentException("No column with name: " + key);
        }
    }

    public Integer getInt(String key) {
        if (typeMap.containsKey(key)) {
            if (typeMap.get(key) == RowType.INT) {
                return (Integer) values.get(key);
            } else {
                throw new IllegalArgumentException("Column '" + key + "' has type: " + typeMap.get(key));
            }
        } else {
            throw new IllegalArgumentException("No column with name: " + key);
        }
    }

    public String getString(String key) {
        if (typeMap.containsKey(key)) {
            if (typeMap.get(key) == RowType.STRING) {
                return (String) values.get(key);
            } else {
                throw new IllegalArgumentException("Column '" + key + "' has type: " + typeMap.get(key));
            }
        } else {
            throw new IllegalArgumentException("No column with name: " + key);
        }
    }

    public Object getObject(String key) {
        if (values.containsKey(key)) {
            return values.get(key);
        } else {
            throw new IllegalArgumentException("No column with name: " + key);
        }
    }

    public RowType getType(String key) {
        if (typeMap.containsKey(key)) {
            return typeMap.get(key);
        } else {
            throw new IllegalArgumentException("No column with name: " + key);
        }
    }

    public boolean contains(String key) {
        return values.containsKey(key);
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public String toString() {
        return "{" + Joiner.on(", ").withKeyValueSeparator(": ").join(values) + "}";
    }
}
