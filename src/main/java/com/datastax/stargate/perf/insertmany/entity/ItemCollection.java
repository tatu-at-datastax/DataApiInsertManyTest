package com.datastax.stargate.perf.insertmany.entity;

import com.datastax.astra.client.Collection;
import com.datastax.astra.client.exception.TooManyDocumentsToCountException;
import com.datastax.astra.client.model.Document;

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
}
