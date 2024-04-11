package com.datastax.stargate.perf.insertmany.entity;

import java.util.ArrayList;
import java.util.List;

public class CollectionItemGenerator {
    private final CollectionItemIdGenerator idGenerator;

    private final int vectorLength;

    public CollectionItemGenerator(CollectionItemIdGenerator idGenerator, int vectorLength) {
        this.idGenerator = idGenerator;
        this.vectorLength = vectorLength;
    }

    public CollectionItem generateSingle() {
        return CollectionItem.create(idGenerator.nextId(), vectorLength);
    }

    public List<CollectionItem> generate(int count) {
        CollectionItemId[] ids = idGenerator.nextIds(count);
        List<CollectionItem> result = new ArrayList<>(count);
        for (int i = 0; i < count; ++i) {
            result.add(CollectionItem.create(ids[i], vectorLength));
        }
        return result;
    }
}
