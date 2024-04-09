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
        idGenerator.nextCycle();
        return _generate();
    }

    public List<CollectionItem> generate(int count) {
        idGenerator.nextCycle();
        List<CollectionItem> result = new ArrayList<>(count);
        for (int i = 0; i < count; ++i) {
            result.add(_generate());
        }
        return result;
    }

    private CollectionItem _generate() {
        final CollectionItemId itemId = idGenerator.nextId();
        return CollectionItem.create(itemId, vectorLength);
    }
}
