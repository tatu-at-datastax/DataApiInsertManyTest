package com.datastax.stargate.perf.insertmany.entity;

import java.util.List;
import java.util.Optional;

import com.datastax.astra.client.collections.Collection;
import com.datastax.astra.client.collections.commands.options.CollectionInsertManyOptions;
import com.datastax.astra.client.collections.commands.results.CollectionDeleteResult;
import com.datastax.astra.client.collections.commands.results.CollectionInsertManyResult;
import com.datastax.astra.client.collections.commands.results.CollectionInsertOneResult;
import com.datastax.astra.client.collections.definition.documents.Document;
import com.datastax.astra.client.collections.exceptions.TooManyDocumentsToCountException;
import com.datastax.astra.client.core.query.Filter;
import com.datastax.astra.client.exceptions.DataAPIException;

/**
 * Wrapper around a Collection of Documents.
 */
public record ItemCollection(String name, Collection<Document> collection,
                             int vectorSize, boolean orderedInserts)
    implements ItemContainer
{
    @Override
    public void validateIsEmpty() {
        long count = countItems(100);
        if (count > 0) {
            throw new IllegalStateException("Collection '" + name + "' not empty; has " + count + " documents");
        }
    }

    @Override
    public long countItems(int maxCount) {
        try {
            return collection.countDocuments(maxCount);
        } catch (TooManyDocumentsToCountException e) {
            return maxCount+1;
        }
    }

    @Override
    public void insertItem(ContainerItem item) throws DataAPIException {
        CollectionInsertOneResult result = collection.insertOne(item.toDocument());
        if (!item.idAsString().equals(result.getInsertedId())) {
            throw new IllegalStateException(String.format(
                    "Unexpected id for inserted document: expected %s, got %s",
                    _str(item.idAsString()), _str(result.getInsertedId())));
        }
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
        if (items.size() > options.getChunkSize()) {
            options = options.chunkSize(items.size());
        }

        CollectionInsertManyResult result = collection.insertMany(itemList, options);
        List<?> ids = result.getInsertedIds();
        if (ids == null || ids.size() != items.size()) {
            return false;
        }
        return true;
    }

    @Override
    public ContainerItem findItem(String idAsSring) {
        Optional<Document> doc = collection.findOne(Filter.findById(idAsSring));
        return ContainerItem.fromDocument(doc);
    }

    @Override
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
