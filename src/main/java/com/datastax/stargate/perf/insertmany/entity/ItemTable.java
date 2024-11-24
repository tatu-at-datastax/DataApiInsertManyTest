package com.datastax.stargate.perf.insertmany.entity;

import com.datastax.astra.client.collections.documents.Document;
import com.datastax.astra.client.collections.options.CollectionInsertManyOptions;
import com.datastax.astra.client.exception.DataAPIException;
import com.datastax.astra.client.tables.Table;

import java.util.List;

/**
 * Wrapper around an API Table.
 */
public record ItemTable(String name, Table<ContainerItem> table,
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
        /*
        CollectionInsertOneResult result = table.insertOne(item.toDocument());
        if (!item.idAsString().equals(result.getInsertedId())) {
            throw new IllegalStateException(String.format(
                    "Unexpected id for inserted document: expected %s, got %s",
                    _str(item.idAsString()), _str(result.getInsertedId())));
        }
         */
    }

    @Override
    public boolean insertItems(List<ContainerItem> items) throws DataAPIException {
        // Special case: 1 item, simply use "insertOne()" instead
        if (items.size() == 1) {
            insertItem(items.get(0));
            return true;
        }
        List<Document> itemList = items.stream().map(ContainerItem::toDocument).toList();
        CollectionInsertManyOptions options = new CollectionInsertManyOptions()
                .ordered(orderedInserts);
        if (items.size() > options.chunkSize()) {
            options = options.chunkSize(items.size());
        }

        /*
        CollectionInsertManyResult result = collection.insertMany(itemList, options);
        List<?> ids = result.getInsertedIds();
        if (ids == null || ids.size() != items.size()) {
            return false;
        }

         */
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
