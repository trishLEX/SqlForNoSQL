package ru.bmstu.sqlfornosql.adapters.mongo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoIterable;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import ru.bmstu.sqlfornosql.model.Row;
import ru.bmstu.sqlfornosql.model.RowType;
import ru.bmstu.sqlfornosql.model.Table;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;

import static ru.bmstu.sqlfornosql.adapters.mongo.MongoHolder.MONGO_ID;

public class MongoMapper {
    public Table mapGroupBy(MongoIterable<BsonDocument> mongoResult, MongoHolder query) {
        Table table = new Table();
        for (BsonDocument element : mongoResult) {
            System.out.println(element);
            Row row = new Row(table);
            Map<String, RowType> typeMap = new LinkedHashMap<>();

            if (!query.hasAggregateFunctions()) {
                fillRowFromDocument(element, row, typeMap, query);
            } else {
                if (element.get(MONGO_ID).isDocument()) {
                    fillRowFromDocument(element, row, typeMap, query);
                } else {
                    String field = MongoUtils.normalizeColumnName(query.getProjection().getString(MONGO_ID));
                    if (query.getSelectFields().contains(field)) {
                        addValueToRow(row, typeMap, query, element, MONGO_ID);
                    }
                }

                for (String column : element.keySet()) {
                    if (!column.equals(MONGO_ID) && query.getSelectFields().contains(column)) {
                        addValueToRow(row, typeMap, query, element, column);
                    }
                }
            }

            table.add(row, typeMap);
        }

        return table;
    }

    public Table mapCountAll(long count, MongoHolder query) {
        return new Table().add("count", count, RowType.INT);
    }

    public Table mapFind(FindIterable<BsonDocument> mongoResult, MongoHolder query) {
        Table table = new Table();
        for (BsonDocument mongoRow : mongoResult) {
            System.out.println(mongoRow);
            Row row = new Row(table);
            Map<String, RowType> typeMap = new LinkedHashMap<>();

            for (Map.Entry<String, BsonValue> pair : mongoRow.entrySet()) {
                if (!query.isSelectAll()) {
                    addValueToRow(row, typeMap, query.getQualifiedNameMap().get(pair.getKey()), pair.getValue());
                } else {
                    addValueToRow(row, typeMap, pair.getKey(), pair.getValue());
                }
            }

            table.add(row, typeMap);
        }

        return table;
    }

    private void fillRowFromDocument(BsonDocument element, Row row, Map<String, RowType> typeMap, MongoHolder query) {
        if (element.get(MONGO_ID).isDocument()) {
            BsonDocument idDocument = element.getDocument(MONGO_ID);
            for (String column : idDocument.keySet()) {
                addValueToRow(row, typeMap, query, idDocument, column);
            }
        } else {
            if (!query.isSelectAll()) {
                addValueToRow(
                        row,
                        typeMap,
                        query.getQualifiedNameMap().get(MongoUtils.normalizeColumnName(query.getProjection().getString(MONGO_ID))),
                        element.get(MONGO_ID));
            } else {
                addValueToRow(row, typeMap, MongoUtils.normalizeColumnName(query.getProjection().getString(MONGO_ID)), element.get(MONGO_ID));
            }
        }
    }

    private void addValueToRow(Row row, Map<String, RowType> typeMap, MongoHolder query, BsonDocument document, String column) {
        BsonValue value = document.get(column);
        if (!query.isSelectAll()) {
            addValueToRow(row, typeMap, query.getQualifiedNameMap().get(column), value);
        } else {
            addValueToRow(row, typeMap, column, value);
        }
    }

    //TODO этот метод должен быть внутри класса Row
    private void addValueToRow(Row row, Map<String, RowType> typeMap, String key, BsonValue value) {
        if (value.isBoolean()) {
            row.add(key, value.asBoolean().getValue());
            typeMap.put(key, RowType.BOOLEAN);
        } else if (value.isDateTime()) {
            row.add(
                    key,
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(value.asDateTime().getValue()), ZoneId.systemDefault())
            );
            typeMap.put(key, RowType.DATE);
        } else if (value.isDouble()) {
            row.add(key, value.asDouble().doubleValue());
            typeMap.put(key, RowType.DOUBLE);
        } else if (value.isInt64()) {
            //TODO сделать поддержку разницы между int32 и int64
            row.add(key, value.asInt64().getValue());
            typeMap.put(key, RowType.INT);
        } else if (value.isInt32()) {
            row.add(key, value.asInt32().getValue());
            typeMap.put(key, RowType.INT);
        } else if (value.isString()) {
            row.add(key, value.asString().getValue());
            typeMap.put(key, RowType.STRING);
        } else if (value.isNull()) {
            row.add(key, null);
            typeMap.put(key, RowType.NULL);
        } else if (value.isObjectId()) {
            row.add(key, value.asObjectId().getValue().toString());
            typeMap.put(key, RowType.STRING);
        } else {
            throw new IllegalArgumentException("Unsupported type of value");
        }
    }
}
