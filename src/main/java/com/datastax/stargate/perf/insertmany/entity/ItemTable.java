package com.datastax.stargate.perf.insertmany.entity;

import com.datastax.astra.client.collections.documents.Document;
import com.datastax.astra.client.collections.options.CollectionInsertManyOptions;
import com.datastax.astra.client.exception.DataAPIException;
import com.datastax.astra.client.tables.Table;
import com.datastax.astra.client.tables.options.TableInsertManyOptions;
import com.datastax.astra.client.tables.results.TableInsertManyResult;
import com.datastax.astra.client.tables.results.TableInsertOneResult;
import com.datastax.astra.client.tables.row.Row;

import java.util.List;

/**
 * Wrapper around an API Table.
 */
public record ItemTable(String name, Table<Row> table,
                        int vectorSize, boolean orderedInserts)
    implements ItemContainer
{
    @Override
    public void validateIsEmpty() {
        final int maxCount = 100;
        long count = countItems(maxCount);
        if (count > 0) {
            throw new IllegalStateException("Table '" + name + "' not empty; has " + count + " documents");
        }
    }

    @Override
    public long countItems(int maxCount) {
        /*
        try {
            return table.countDocuments(maxCount);
        } catch (TooManyDocumentsToCountException e) {
            return maxCount+1;
        }
         */
        return -1L;
    }

    @Override
    public void insertItem(ContainerItem item) throws DataAPIException {
        TableInsertOneResult result = table.insertOne(item.toTableRow());
        List<Object> primaryKeys = result.getInsertedId();
        if (primaryKeys.size() != 1) {
            throw new IllegalStateException(String.format(
                    "Unexpected id response for inserted document: expected List<1>, got List<%d>",
                    primaryKeys.size()));
        }
        Object resultId = primaryKeys.get(0);
        if (!item.idAsString().equals(resultId)) {
            throw new IllegalStateException(String.format(
                    "Unexpected id for inserted document: expected %s, got %s",
                    _str(item.idAsString()), _str(resultId)));
        }
    }

    @Override
    public boolean insertItems(List<ContainerItem> items) throws DataAPIException {
        // Special case: 1 item, simply use "insertOne()" instead
        if (items.size() == 1) {
            insertItem(items.get(0));
            return true;
        }
        List<Row> rows = items.stream()
                .map(item -> item.toTableRow())
                .toList();
        TableInsertManyOptions options = new TableInsertManyOptions()
                .ordered(orderedInserts);
        if (items.size() > options.chunkSize()) {
            options = options.chunkSize(items.size());
        }

        TableInsertManyResult result = table.insertMany(rows, options);
        List<?> ids = result.getInsertedIds();
        if (ids == null || ids.size() != items.size()) {
            return false;
        }

        return true;
    }

    @Override
    public ContainerItem findItem(String idAsSring) {
        /*
        Optional<Document> doc = collection.findOne(Filter.findById(idAsSring));
        return CollectionItem.fromDocument(doc);
         */
        return null;
    }

    @Override
    public long deleteAll() {
        /*
        CollectionDeleteResult dr = collection.deleteAll();
        return dr.getDeletedCount();
         */
        return -1L;
    }

    private static String _str(Object ob) {
        if (ob == null) {
            return "`null`";
        }
        if (ob instanceof String) {
            return "'"+ob+"'";
        }
        return String.format("(%s)'%s'", ob.getClass().getName(), ob);
    }
}
