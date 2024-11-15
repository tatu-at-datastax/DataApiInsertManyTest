package com.datastax.stargate.perf.insertmany.entity;

import java.util.List;
import java.util.Optional;

import com.datastax.astra.client.collections.Collection;
import com.datastax.astra.client.collections.documents.Document;
import com.datastax.astra.client.collections.exceptions.TooManyDocumentsToCountException;
import com.datastax.astra.client.collections.options.CollectionInsertManyOptions;
import com.datastax.astra.client.collections.results.CollectionDeleteResult;
import com.datastax.astra.client.collections.results.CollectionInsertManyResult;
import com.datastax.astra.client.collections.results.CollectionInsertOneResult;
import com.datastax.astra.client.core.query.Filter;
import com.datastax.astra.client.exception.DataAPIException;

/**
 * Wrapper around a Collection of Documents.
 */
public record ItemCollection(String name, Collection<Document> collection,
                             int vectorSize, boolean orderedInserts)
{
    private static final CollectionInsertManyOptions OPTIONS_ORDERED = new CollectionInsertManyOptions()
            .ordered(true);

    private static final CollectionInsertManyOptions OPTIONS_UNORDERED = new CollectionInsertManyOptions()
            .ordered(false);

    public void validateIsEmpty() {
        final int maxCount = 100;
        long count = countItems(maxCount);
        if (count > 0) {
            throw new IllegalStateException("Collection '" + name + "' not empty; has " + count + " documents");
        }
    }

    public long countItems(int maxCount) {
        try {
            return collection.countDocuments(maxCount);
        } catch (TooManyDocumentsToCountException e) {
            return maxCount+1;
        }
    }

    public void insertItem(CollectionItem item) throws DataAPIException {
        CollectionInsertOneResult result = collection.insertOne(item.toDocument());
        if (!item.idAsString().equals(result.getInsertedId())) {
            throw new IllegalStateException(String.format(
                    "Unexpected id for inserted document: expected %s, got %s",
                    _str(item.idAsString()), _str(result.getInsertedId())));
        }
    }

    public boolean insertItems(List<CollectionItem> items) throws DataAPIException {
        // Special case: 1 item, simply use "insertOne()" instead
        if (items.size() == 1) {
            insertItem(items.get(0));
            return true;
        }
        List<Document> itemList = items.stream().map(CollectionItem::toDocument).toList();
        CollectionInsertManyOptions options = new CollectionInsertManyOptions()
                .ordered(orderedInserts);
        if (items.size() > options.chunkSize()) {
            options = options.chunkSize(items.size());
        }

        CollectionInsertManyResult result = collection.insertMany(itemList, options);
        List<?> ids = result.getInsertedIds();
        if (ids == null || ids.size() != items.size()) {
            return false;
        }
        return true;
    }

    public CollectionItem findItem(String idAsSring) {
        Optional<Document> doc = collection.findOne(Filter.findById(idAsSring));
        return CollectionItem.fromDocument(doc);
    }

    public long deleteAll() {
        CollectionDeleteResult dr = collection.deleteAll();
        return dr.getDeletedCount();
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
