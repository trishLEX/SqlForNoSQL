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
import java.util.Map;

import static ru.bmstu.sqlfornosql.adapters.mongo.MongoHolder.MONGO_ID;

public class MongoMapper {
    public Table mapGroupBy(MongoIterable<BsonDocument> mongoResult, MongoHolder query) {
        Table table = new Table();
        for (BsonDocument element : mongoResult) {
            System.out.println(element);
            Row row = new Row();

            if (!query.hasAggregateFunctions()) {
                fillRowFromDocument(element, row, query);
            } else {
                if (element.get(MONGO_ID).isDocument()) {
                    fillRowFromDocument(element, row, query);
                } else {
                    String field = MongoUtils.normalizeColumnName(query.getProjection().getString(MONGO_ID));
                    if (query.getSelectFields().contains(field)) {
                        addValueToRow(row, query, element, MONGO_ID);
                    }
                }

                for (String column : element.keySet()) {
                    if (!column.equals(MONGO_ID) && query.getSelectFields().contains(column)) {
                        addValueToRow(row, query, element, column);
                    }
                }
            }

            table.add(row);
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
            Row row = new Row();

            for (Map.Entry<String, BsonValue> pair : mongoRow.entrySet()) {
                if (!query.isSelectAll()) {
                    addValueToRow(row, query.getQualifiedNameMap().get(pair.getKey()), pair.getValue());
                } else {
                    addValueToRow(row, pair.getKey(), pair.getValue());
                }
            }

            table.add(row);
        }

        return table;
    }

    private void fillRowFromDocument(BsonDocument element, Row row, MongoHolder query) {
        if (element.get(MONGO_ID).isDocument()) {
            BsonDocument idDocument = element.getDocument(MONGO_ID);
            for (String column : idDocument.keySet()) {
                addValueToRow(row, query, idDocument, column);
            }
        } else {
            if (!query.isSelectAll()) {
                addValueToRow(row, query.getQualifiedNameMap().get(MongoUtils.normalizeColumnName(query.getProjection().getString(MONGO_ID))), element.get(MONGO_ID));
            } else {
                addValueToRow(row, MongoUtils.normalizeColumnName(query.getProjection().getString(MONGO_ID)), element.get(MONGO_ID));
            }
        }
    }

    private void addValueToRow(Row row, MongoHolder query, BsonDocument document, String column) {
        BsonValue value = document.get(column);
        if (!query.isSelectAll()) {
            addValueToRow(row, query.getQualifiedNameMap().get(column), value);
        } else {
            addValueToRow(row, column, value);
        }
    }

    //TODO этот метод должен быть внутри класса Row
    private void addValueToRow(Row row, String key, BsonValue value) {
        if (value.isBoolean()) {
            row.add(key, value.asBoolean().getValue(), RowType.BOOLEAN);
        } else if (value.isDateTime()) {
            row.add(
                    key,
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(value.asDateTime().getValue()), ZoneId.systemDefault()),
                    RowType.DATE
            );
        } else if (value.isDouble()) {
            row.add(key, value.asDouble().doubleValue(), RowType.DOUBLE);
        } else if (value.isInt64()) {
            //TODO сделать поддержку разницы между int32 и int64
            row.add(key, value.asInt64().getValue(), RowType.INT);
        } else if (value.isInt32()) {
            row.add(key, value.asInt32().getValue(), RowType.INT);
        } else if (value.isString()) {
            row.add(key, value.asString().getValue(), RowType.STRING);
        } else if (value.isNull()) {
            row.add(key, null, RowType.NULL);
        } else if (value.isObjectId()) {
            row.add(key, value.asObjectId().getValue().toString(), RowType.STRING);
        } else {
            throw new IllegalArgumentException("Unsupported type of value");
        }
    }
}
