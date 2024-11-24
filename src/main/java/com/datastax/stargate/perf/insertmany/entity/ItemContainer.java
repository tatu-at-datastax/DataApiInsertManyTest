package com.datastax.stargate.perf.insertmany.entity;

import com.datastax.astra.client.exception.DataAPIException;

import java.util.List;

public interface ItemContainer {
    public int vectorSize();

    public boolean orderedInserts();

    public void validateIsEmpty();

    public long countItems(int maxCount);

    public void insertItem(ContainerItem item) throws DataAPIException;

    public boolean insertItems(List<ContainerItem> items) throws DataAPIException;

    public ContainerItem findItem(String idAsSring);

    public long deleteAll();
}
