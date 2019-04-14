package ru.bmstu.sqlfornosql.model;

import com.google.common.base.Joiner;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectField;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class Row {
    private Table table;
    private Map<SelectField, Object> values;

    public Row(Table table) {
        this.table = table;
        this.values = new LinkedHashMap<>();
    }

    public Row add(SelectField key, Object value) {
        values.put(key, value);
        return this;
    }

    public Set<SelectField> getColumns() {
        return values.keySet();
    }

    public Boolean getBool(SelectField key) {
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

    public LocalDateTime getDate(SelectField key) {
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

    public Double getDouble(SelectField key) {
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

    public Integer getInt(SelectField key) {
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

    public String getString(SelectField key) {
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

    public Object getObject(SelectField key) {
        if (values.containsKey(key)) {
            return values.get(key);
        } else {
            throw new IllegalArgumentException("No column with name: " + key);
        }
    }

    public Object getObject(String key) {
        for (Map.Entry<SelectField, Object> rowEntry : values.entrySet()) {
            if (rowEntry.getKey().getUserInputName().equalsIgnoreCase(key)) {
                return rowEntry.getValue();
            }
        }

        throw new IllegalArgumentException("No column with name: " + key);
    }

    public void remove(Collection<SelectField> keys) {
        for (SelectField key : keys) {
            Object removed = values.remove(key);
            if (removed == null) {
                values.remove(key);
            }
        }
    }

    public boolean contains(SelectField key) {
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
