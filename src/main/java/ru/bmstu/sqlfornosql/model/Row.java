package ru.bmstu.sqlfornosql.model;

import com.google.common.base.Joiner;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class Row {
    private Table table;
    private Map<String, Object> values;

    public Row(Table table) {
        this.table = table;
        this.values = new LinkedHashMap<>();
    }

    public Row add(String key, Object value) {
        values.put(key, value);
        return this;
    }

    public Set<String> getColumns() {
        return values.keySet();
    }

    public Boolean getBool(String key) {
        if (table.getType(key) != null) {
            if (table.getType(key) == RowType.BOOLEAN) {
                return (Boolean) values.get(key);
            } else {
                throw new IllegalArgumentException("Column '" + key + "' has type: " + table.getType(key));
            }
        } else {
            throw new IllegalArgumentException("No column with name: " + key);
        }
    }

    public LocalDateTime getDate(String key) {
        if (table.getType(key) != null) {
            if (table.getType(key) == RowType.DATE) {
                return (LocalDateTime) values.get(key);
            } else {
                throw new IllegalArgumentException("Column '" + key + "' has type: " + table.getType(key));
            }
        } else {
            throw new IllegalArgumentException("No column with name: " + key);
        }
    }

    public Double getDouble(String key) {
        if (table.getType(key) != null) {
            if (table.getType(key) == RowType.DOUBLE) {
                return (Double) values.get(key);
            } else {
                throw new IllegalArgumentException("Column '" + key + "' has type: " + table.getType(key));
            }
        } else {
            throw new IllegalArgumentException("No column with name: " + key);
        }
    }

    public Integer getInt(String key) {
        if (table.getType(key) != null) {
            if (table.getType(key) == RowType.INT) {
                return (Integer) values.get(key);
            } else {
                throw new IllegalArgumentException("Column '" + key + "' has type: " + table.getType(key));
            }
        } else {
            throw new IllegalArgumentException("No column with name: " + key);
        }
    }

    public String getString(String key) {
        if (table.getType(key) != null) {
            if (table.getType(key) == RowType.STRING) {
                return (String) values.get(key);
            } else {
                throw new IllegalArgumentException("Column '" + key + "' has type: " + table.getType(key));
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

    public void remove(Collection<String> keys) {
        for (String key : keys) {
            values.remove(key);
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
        return "{" + Joiner.on(", ").withKeyValueSeparator(": ").useForNull("null").join(values) + "}";
    }
}
