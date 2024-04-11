package com.datastax.stargate.perf.insertmany.entity;

import java.util.List;
import java.util.Optional;

import com.datastax.astra.client.Collection;
import com.datastax.astra.client.exception.DataApiException;
import com.datastax.astra.client.exception.TooManyDocumentsToCountException;
import com.datastax.astra.client.model.DeleteResult;
import com.datastax.astra.client.model.Document;
import com.datastax.astra.client.model.Filter;
import com.datastax.astra.client.model.InsertManyOptions;
import com.datastax.astra.client.model.InsertManyResult;
import com.datastax.astra.client.model.InsertOneResult;

/**
 * Wrapper around a Collection of Documents.
 */
public record ItemCollection(String name, Collection<Document> collection,
                             int vectorSize, boolean orderedInserts)
{
    private static final InsertManyOptions OPTIONS_ORDERED = new InsertManyOptions()
            .ordered(true);

    private static final InsertManyOptions OPTIONS_UNORDERED = new InsertManyOptions()
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

    public void insertItem(CollectionItem item) throws DataApiException {
        InsertOneResult result = collection.insertOne(item.toDocument());
        if (!item.idAsString().equals(result.getInsertedId())) {
            throw new IllegalStateException(String.format(
                    "Unexpected id for inserted document: expected %s, got %s",
                    _str(item.idAsString()), _str(result.getInsertedId())));
        }
    }

    public boolean insertItems(List<CollectionItem> items) throws DataApiException {
        InsertManyResult result = collection.insertMany(items.stream().map(CollectionItem::toDocument).toList(),
                orderedInserts ? OPTIONS_ORDERED : OPTIONS_UNORDERED);
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
        DeleteResult dr = collection.deleteAll();
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
