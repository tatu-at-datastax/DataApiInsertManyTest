package com.datastax.stargate.perf.insertmany.entity;

import com.datastax.astra.client.Collection;
import com.datastax.astra.client.exception.TooManyDocumentsToCountException;
import com.datastax.astra.client.model.Document;
import com.datastax.astra.client.model.Filter;
import com.datastax.astra.client.model.InsertOneResult;

import java.util.Optional;

/**
 * Wrapper around a Collection of Documents.
 */
public record ItemCollection(String name, Collection<Document> collection, int vectorSize)
{
    public void validateIsEmpty() {
        final int maxCount = 100;
        try {
            long count = collection.countDocuments(maxCount);
            if (count > 0) {
                throw new IllegalStateException("Collection '" + name + "' not empty; has " + count + " documents");
            }
        } catch (TooManyDocumentsToCountException e) {
            throw new IllegalStateException("Collection '" + name + "' not empty; has over " + maxCount + " documents");
        }
    }

    public void insertItem(CollectionItem item) {
        InsertOneResult result = collection.insertOne(item.toDocument());
        if (!item.idAsString().equals(result.getInsertedId())) {
            throw new IllegalStateException(String.format(
                    "Unexpected id for inserted document: expected %s, got %s",
                    _str(item.idAsString()), _str(result.getInsertedId())));
        }
    }

    public CollectionItem findItem(String idAsSring) {
        Optional<Document> doc = collection.findOne(Filter.findById(idAsSring));
        return CollectionItem.fromDocument(doc);
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
