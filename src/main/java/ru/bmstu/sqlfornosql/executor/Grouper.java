package ru.bmstu.sqlfornosql.executor;

import ru.bmstu.sqlfornosql.model.Row;
import ru.bmstu.sqlfornosql.model.RowType;
import ru.bmstu.sqlfornosql.model.Table;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Grouper {
    //TODO Check that collections are sets
    public static Table group(Table table, Collection<String> groupBys, Collection<String> columns) {
        Table result = new Table();
        Map<Map<String, Object>, Row> index = new HashMap<>();
        for (Row row : table.getRows()) {
            Map<String, Object> indexEntry = new HashMap<>();
            for (String column : groupBys) {
                indexEntry.put(column, row.getObject(column));
            }

            if (index.containsKey(indexEntry)) {
                Row resRow = index.get(indexEntry);
                index.put(indexEntry, mergeRows(row, resRow, columns, table.getTypeMap(), result));
            } else {
                index.put(indexEntry, row);
            }
        }

        throw new UnsupportedOperationException();
    }

    private static Row mergeRows(Row a, Row b, Collection<String> columns, Map<String, RowType> typeMap, Table table) {
        Row row = new Row(table);
        for (String column : columns) {
            //TODO type должен определяться динамически (вдруг null и int были)
            row.add(column, mergeValues(a, b, column, typeMap));
            table.setType(column, typeMap.get(column));
        }

        return row;
    }

    //TODO фикс тип данных NULL, хотелось бы иметь все таки Int и занчение null, а не тип данных null
    //TODO не поддерживается если столбец был в group by и он без аггрегационной функции
    private static Object mergeValues(Row a, Row b, String column, Map<String, RowType> typeMap) {
//        if (typeMap.get(column) != RowType.NULL && typeMap.get(column) != RowType.NULL && a.getType(column) != b.getType(column)) {
//            throw new IllegalStateException("Types of values are not equal:" + a.getObject(column) + " and " + b.getObject(column));
//        }

        switch (typeMap.get(column)) {
            case NULL:
//                if (b.getType(column) == RowType.NULL) {
//                    return null;
//                } else {
//                    return b.getObject(column);
//                }
                return b.getObject(column);
            case STRING:
//                if (b.getType(column) == RowType.NULL) {
//                    return a.getObject(column);
//                } else {
//                    return mergeStrings(a.getObject(column), b.getObject(column), column);
//                }
                mergeStrings(a.getObject(column), b.getObject(column), column);
            case BOOLEAN:
//                if (b.getType(column) == RowType.NULL) {
//                    return a.getObject(column);
//                } else {
//                    return mergeBools(a.getObject(column), b.getObject(column), column);
//                }
                mergeBools(a.getObject(column), b.getObject(column), column);
            case DATE:
//                if (b.getType(column) == RowType.NULL) {
//                    return a.getObject(column);
//                } else {
//                    return mergeDates(a, b, column);
//                }
                return mergeDates(a.getObject(column), b.getObject(column), column);
            case DOUBLE:
//                if (b.getType(column) == RowType.NULL) {
//                    return a.getObject(column);
//                } else {
//                    return mergeDoubles(a, b, column);
//                }
                mergeDoubles(a.getObject(column), b.getObject(column), column);
            case INT:
//                if (b.getType(column) == RowType.NULL) {
//                    return a.getObject(column);
//                } else {
//                    return mergeInts(a, b, column);
//                }
                mergeInts(a.getObject(column), b.getObject(column), column);
            default:
                throw new IllegalStateException("Unsupported type of column: " + column);
        }
    }

    private static Object mergeInts(Object a, Object b, String column) {
        if (column.toLowerCase().startsWith("min(")) {
            return minInts((Integer) a, (Integer) b);
        } else if (column.toLowerCase().startsWith("max(")) {
            return maxInts((Integer) a, (Integer) b);
        } else if (column.toLowerCase().startsWith("sum(")) {
            return sumInts((Integer) a, (Integer) b);
        } else if (column.toLowerCase().startsWith("count(")) {
            return countObjects((Integer) a, (Integer) b);
        } else if (column.toLowerCase().startsWith("avg(")) {
            //TODO непонятно, что делать с avg
            throw new UnsupportedOperationException();
        } else {
            throw new IllegalStateException("Unsupported aggregation function in column: " + column);
        }
    }

    private static Object maxInts(int a, int b) {
        return Integer.max(a, b);
    }

    private static Object minInts(int a, int b) {
        return Integer.min(a, b);
    }

    private static Object sumInts(int a, int b) {
        return a + b;
    }

    private static Object mergeDoubles(Object a, Object b, String column) {
        if (column.toLowerCase().startsWith("min(")) {
            return minDoubles((Double) a, (Double) b);
        } else if (column.toLowerCase().startsWith("max(")) {
            return maxDoubles((Double) a, (Double) b);
        } else if (column.toLowerCase().startsWith("sum(")) {
            return sumDoubles((Double) a, (Double) b);
        } else if (column.toLowerCase().startsWith("count(")) {
            return countObjects((Integer) a, (Integer) b);
        } else if (column.toLowerCase().startsWith("avg(")) {
            //TODO непонятно, что делать с avg
            throw new UnsupportedOperationException();
        } else {
            throw new IllegalStateException("Unsupported aggregation function in column: " + column);
        }
    }

    private static Object maxDoubles(double a, double b) {
        return Double.max(a, b);
    }

    private static Object minDoubles(double a, double b) {
        return Double.min(a, b);
    }

    private static Object sumDoubles(double a, double b) {
        return a + b;
    }

    private static Object mergeDates(Object a, Object b, String column) {
        if (column.toLowerCase().startsWith("min(")) {
            return minDates((LocalDateTime) a, (LocalDateTime) b);
        } else if (column.toLowerCase().startsWith("max(")) {
            return maxDates((LocalDateTime) a, (LocalDateTime) b);
        } else if (column.toLowerCase().startsWith("count(")) {
            return countObjects((Integer) a, (Integer) b);
        } else {
            throw new IllegalArgumentException("Can't aggregate dates in column: " + column);
        }
    }

    private static Object maxDates(LocalDateTime a, LocalDateTime b) {
        int res = a.compareTo(b);
        if (res > 0) {
            return a;
        } else if (res < 0) {
            return b;
        } else {
            return a;
        }
    }

    private static Object minDates(LocalDateTime a, LocalDateTime b) {
        int res = a.compareTo(b);
        if (res > 0) {
            return b;
        } else if (res < 0) {
            return a;
        } else {
            return b;
        }
    }

    private static Object mergeBools(Object a, Object b, String column) {
        if (column.toLowerCase().startsWith("count(")) {
            return countObjects((Integer) a, (Integer) b);
        } else {
            throw new IllegalArgumentException("Can't aggregate bools in column: " + column);
        }
    }

    //TODO check column names
    private static Object mergeStrings(Object a, Object b, String column) {
        if (column.toLowerCase().startsWith("min(")) {
            return minStrings((String) a, (String) b);
        } else if (column.toLowerCase().startsWith("max(")) {
            return maxStrings((String) a, (String) b);
        } else if (column.toLowerCase().startsWith("count(")) {
            return countObjects((Integer) a, (Integer) b);
        } else {
            throw new IllegalArgumentException("Can't aggregate strings in column: " + column);
        }
    }

    private static String maxStrings(String a, String b) {
        int res = a.compareToIgnoreCase(b);
        if (res > 0) {
            return a;
        } else if (res < 0){
            return b;
        } else {
            return a;
        }
    }

    private static String minStrings(String a, String b) {
        int res = a.compareToIgnoreCase(b);
        if (res > 0) {
            return b;
        } else if (res < 0) {
            return a;
        } else {
            return b;
        }
    }

    private static int countObjects(int a, int b) {
        return a + b;
    }
}
