package ru.bmstu.sqlfornosql.executor;

import net.sf.jsqlparser.expression.Expression;
import org.medfoster.sqljep.ParseException;
import org.medfoster.sqljep.RowJEP;
import ru.bmstu.sqlfornosql.adapters.sql.SqlUtils;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectField;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SelectFieldExpression;
import ru.bmstu.sqlfornosql.adapters.sql.selectfield.SqlFunction;
import ru.bmstu.sqlfornosql.model.Row;
import ru.bmstu.sqlfornosql.model.RowType;
import ru.bmstu.sqlfornosql.model.Table;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static ru.bmstu.sqlfornosql.executor.ExecutorUtils.*;

public class Grouper {
    //TODO Check that collections are sets
    //TODO having unsupported
    public static Table group(Table table, Collection<String> groupBys, Collection<SelectField> columns, @Nullable Expression havingClause) {
        Table result = new Table();
        Map<Map<String, Object>, Row> index = new HashMap<>();
        for (Row row : table.getRows()) {
            Map<String, Object> indexEntry = new HashMap<>();
            for (String column : groupBys) {
                indexEntry.put(column, row.getObject(column.toLowerCase()));
            }

            if (index.containsKey(indexEntry)) {
                Row resRow = index.get(indexEntry);
                index.put(indexEntry, mergeRows(row, resRow, columns, groupBys, table.getTypeMap(), result));
            } else {
                index.put(indexEntry, defaultRow(row, columns, table.getTypeMap(), result));
            }
        }

        for (Row row : index.values()) {
            if (havingClause != null) {
                HashMap<String, Integer> colMapping = getIdentMapping(havingClause.toString());
                RowJEP sqljep = prepareSqlJEP(havingClause, colMapping);
                Comparable[] values = new Comparable[colMapping.size()];

                for (Map.Entry<String, Integer> colMappingEntry : colMapping.entrySet()) {
                    values[colMappingEntry.getValue()] = getValue(row, colMappingEntry.getKey());
                }

                try {
                    Boolean expressionValue = (Boolean) sqljep.getValue(values);
                    if (expressionValue) {
                        result.add(row);
                    }
                } catch (ParseException e) {
                    throw new IllegalStateException("Can't execute expression: " + havingClause, e);
                }
            } else {
                result.add(row);
            }
        }

        return result;
    }

    private static Row mergeRows(Row a, Row b, Collection<SelectField> columns, Collection<String> groupBys, Map<String, RowType> typeMap, Table table) {
        Row row = new Row(table);
        for (SelectField field : columns) {
            //TODO type должен определяться динамически (вдруг null и int были)
            String column = field.getNonQualifiedContent();
            if (groupBys.contains(column)) {
                System.out.println("should be equals: " + a.getObject(column) + " " + b.getObject(column));
                row.add(column, typeMap.get(column));
            } else {
                if (column.toLowerCase().startsWith("avg(")) {
                    String countColumn = "count(" + column.substring(4);
                    row.add(column, avg(a.getDouble(column), a.getInt(countColumn), b.getDouble(column), b.getInt(countColumn)));
                    row.add(countColumn, a.getInt(countColumn) + b.getInt(countColumn));
                    table.setType(column, typeMap.get(column));
                    table.setType(countColumn, RowType.INT);
                } else {
                    row.add(column, mergeValues(a, b, column, typeMap));
                    table.setType(column, typeMap.get(column));
                }
            }
        }

        return row;
    }

    private static Row defaultRow(Row row, Collection<SelectField> columns, Map<String, RowType> typeMap, Table table) {
        Row resRow = new Row(table);
        for (SelectField column : columns) {
            if (column instanceof SelectFieldExpression) {
                SelectFieldExpression expression = (SelectFieldExpression) column;
                if (expression.getFunction() == SqlFunction.COUNT) {
                    resRow.add(column.getNonQualifiedContent(), 1);
                    table.setType(column.getNonQualifiedContent(), RowType.INT);
                } else if (expression.getFunction() == SqlFunction.AVG) {
                    resRow.add(column.getNonQualifiedContent(), row.getObject(expression.getColumn().getNonQualifiedContent()));
                    table.setType(column.getNonQualifiedContent(), typeMap.get(expression.getColumn().getNonQualifiedContent()));

                    resRow.add("count(" + expression.getColumn().getNonQualifiedContent() + ")", 1);
                    table.setType("count(" + expression.getColumn().getNonQualifiedContent() + ")", RowType.INT);
                } else {
                    resRow.add(column.getNonQualifiedContent(), row.getObject(expression.getColumn().getNonQualifiedContent()));
                    table.setType(column.getNonQualifiedContent(), typeMap.get(expression.getColumn().getNonQualifiedContent()));
                }
            } else {
                resRow.add(column.getNonQualifiedContent(), row.getObject(column.getNonQualifiedContent()));
                table.setType(column.getNonQualifiedContent(), typeMap.get(column.getNonQualifiedContent()));
            }


//            column = column.toLowerCase();
//            if (column.toLowerCase().startsWith("min(")) {
//                resRow.add(column, row.getObject(SqlUtils.getIdentFromSelectItem(column)));
//                table.setType(column, typeMap.get(SqlUtils.getIdentFromSelectItem(column)));
//            } else if (column.toLowerCase().startsWith("max(")) {
//                resRow.add(column, row.getObject(SqlUtils.getIdentFromSelectItem(column)));
//                table.setType(column, typeMap.get(SqlUtils.getIdentFromSelectItem(column)));
//            } else if (column.toLowerCase().startsWith("sum(")) {
//                resRow.add(column, row.getObject(SqlUtils.getIdentFromSelectItem(column)));
//                table.setType(column, typeMap.get(SqlUtils.getIdentFromSelectItem(column)));
//            } else if (column.toLowerCase().startsWith("count(")) {
//                resRow.add(column, 1);
//                table.setType(column, RowType.INT);
//            } else if (column.toLowerCase().startsWith("avg(")) {
//                String colName = column.substring(4, column.lastIndexOf(')'));
//                resRow.add(column, row.getObject(colName));
//                resRow.add("count(" + colName + ")", 1);
//                table.setType(column, typeMap.get(column));
//                table.setType("count(" + colName + ")", RowType.INT);
//            } else {
//                resRow.add(column, row.getObject(column));
//                table.setType(column, typeMap.get(column));
//            }
        }

        return resRow;
    }

    //TODO фикс тип данных NULL, хотелось бы иметь все таки Int и занчение null, а не тип данных null
    //TODO не поддерживается если столбец был в group by и он без аггрегационной функции
    private static Object mergeValues(Row a, Row b, String column, Map<String, RowType> typeMap) {
        String columnA = SqlUtils.getIdentFromSelectItem(column.toLowerCase());
        column = column.toLowerCase();
        switch (typeMap.get(columnA)) {
            case NULL:
                return b.getObject(column);
            case STRING:
                return mergeStrings(a.getObject(columnA), b.getObject(column), column);
            case BOOLEAN:
                return mergeBools(a.getObject(columnA), b.getObject(column), column);
            case DATE:
                return mergeDates(a.getObject(columnA), b.getObject(column), column);
            case DOUBLE:
                return mergeDoubles(a.getObject(columnA), b.getObject(column), column);
            case INT:
                return mergeInts(a.getObject(columnA), b.getObject(column), column);
            default:
                throw new IllegalStateException("Unsupported type of column: " + column);
        }
    }

    //TODO протестировать, когда в таблице нет занчений (чтобы не было деления на ноль)
    private static Double avg(Double avgA, Integer countA, Double avgB, Integer countB) {
        if (avgA == null) {
            avgA = 0.0;
            countA = 1;
        }

        if (avgB == null) {
            avgB = 0.0;
            countB = 1;
        }

        return (avgA * countA + avgB * countB) / (countA + countB);
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
