package ru.bmstu.sqlfornosql.adapters.postgres;

import ru.bmstu.sqlfornosql.adapters.sql.SqlHolder;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.Column;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectField;
import ru.bmstu.sqlfornosql.model.Row;
import ru.bmstu.sqlfornosql.model.RowType;
import ru.bmstu.sqlfornosql.model.Table;
import ru.bmstu.sqlfornosql.model.TableIterator;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.sql.Types.*;

@ParametersAreNonnullByDefault
public class PostgresMapper {
    public TableIterator mapResultSet(Iterable<ResultSet> resultSetIterator, SqlHolder query) {
        return new TableIterator() {
            private Iterator<ResultSet> iterator = resultSetIterator.iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Table next() {
                return mapResultSet(iterator.next(), query);
            }

            @Nonnull
            @Override
            public Iterator<Table> iterator() {
                Iterator<ResultSet> rsIterator = resultSetIterator.iterator();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return rsIterator.hasNext();
                    }

                    @Override
                    public Table next() {
                        return mapResultSet(rsIterator.next(), query);
                    }
                };
            }
        };
    }

    public Table mapResultSet(ResultSet resultSet, SqlHolder query) {
        Table table = new Table();
        try (resultSet){
            ResultSetMetaData metaData = resultSet.getMetaData();
            Map<String, SelectField> columns = new HashMap<>();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                columns.put(metaData.getColumnName(i), new Column(metaData.getColumnName(i)).withSource(query.getFromItem()));
            }

            while (resultSet.next()) {
                Row row = new Row(table);
                Map<SelectField, RowType> typeMap = new LinkedHashMap<>();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    RowType type = getTypeFromSqlType(metaData.getColumnType(i));
                    if (!query.isSelectAll()) {
                        //String column = query.getFieldByNonQualifiedName(metaData.getColumnName(i)).getQualifiedContent();
                        SelectField column = query.getSelectFields().get(i - 1);
                        row.add(column, getPostgresValue(resultSet, i, type));
                        typeMap.put(column, type);
                    } else {
                        SelectField column = columns.get(metaData.getColumnName(i));
                        row.add(column, getPostgresValue(resultSet, i, type));
                        typeMap.put(column, type);
                    }
                }
                table.add(row, typeMap);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Can't get result of query", e);
        }

        return table;
    }

    private RowType getTypeFromSqlType(int type) {
        switch (type) {
            case BIT:
            case TINYINT:
            case BIGINT:
            case INTEGER:
            case SMALLINT:
                return RowType.INT;
            case LONGNVARCHAR:
            case CHAR:
            case VARCHAR:
                return RowType.STRING;
            case NULL:
                return RowType.NULL;
            case NUMERIC:
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
            case REAL:
                return RowType.DOUBLE;
            case DATE:
            case TIME:
            case TIMESTAMP:
                return RowType.DATE;
            default:
                throw new IllegalArgumentException("Type unsupported");
        }
    }

    private Object getPostgresValue(ResultSet resultSet, int column, RowType type) {
        try {
            if (resultSet.getObject(column) == null) {
                return null;
            }
            switch (type) {
                case INT:
                    return resultSet.getInt(column);
                case DOUBLE:
                    return resultSet.getDouble(column);
                case DATE:
                    Timestamp ts = resultSet.getTimestamp(column);
                    return ts == null ? null : resultSet.getTimestamp(column).toLocalDateTime();
                case BOOLEAN:
                    return resultSet.getBoolean(column);
                case STRING:
                    return resultSet.getString(column);
                case NULL:
                    return null;
                default:
                    throw new IllegalStateException("Wrong type: " + type);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Can't get value", e);
        }
    }
}
