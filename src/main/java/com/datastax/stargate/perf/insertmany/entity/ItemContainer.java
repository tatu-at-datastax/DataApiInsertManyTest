package com.datastax.stargate.perf.insertmany.entity;

import com.datastax.astra.client.exceptions.DataAPIException;

import java.util.List;

public interface ItemContainer {
    int vectorSize();

    boolean orderedInserts();

    void validateIsEmpty();

    long countItems(int maxCount);

    void insertItem(ContainerItem item) throws DataAPIException;

    boolean insertItems(List<ContainerItem> items) throws DataAPIException;

    ContainerItem findItem(String idAsSring);

    long deleteAll();
}
