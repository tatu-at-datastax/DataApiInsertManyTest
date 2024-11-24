package com.datastax.stargate.perf.insertmany.entity;

import com.datastax.astra.client.collections.documents.Document;
import com.datastax.astra.client.collections.exceptions.TooManyDocumentsToCountException;
import com.datastax.astra.client.collections.options.CollectionInsertManyOptions;
import com.datastax.astra.client.collections.results.CollectionDeleteResult;
import com.datastax.astra.client.collections.results.CollectionInsertManyResult;
import com.datastax.astra.client.collections.results.CollectionInsertOneResult;
import com.datastax.astra.client.core.query.Filter;
import com.datastax.astra.client.exception.DataAPIException;

import java.util.List;
import java.util.Optional;

public interface ItemContainer {
    public int vectorSize();

    public boolean orderedInserts();

    public void validateIsEmpty();

    public long countItems(int maxCount);

    public void insertItem(CollectionItem item) throws DataAPIException;

    public boolean insertItems(List<CollectionItem> items) throws DataAPIException;

    public CollectionItem findItem(String idAsSring);

    public long deleteAll();
}
