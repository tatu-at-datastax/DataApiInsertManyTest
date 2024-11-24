package com.datastax.stargate.perf.insertmany.entity;

import java.util.ArrayList;
import java.util.List;

public class ContainerItemGenerator {
    private final ContainerItemIdGenerator idGenerator;

    private final int vectorLength;

    public ContainerItemGenerator(ContainerItemIdGenerator idGenerator, int vectorLength) {
        this.idGenerator = idGenerator;
        this.vectorLength = vectorLength;
    }

    public ContainerItem generateSingle() {
        return ContainerItem.create(idGenerator.nextId(), vectorLength);
    }

    public List<ContainerItem> generate(int count) {
        ContainerItemId[] ids = idGenerator.nextIds(count);
        List<ContainerItem> result = new ArrayList<>(count);
        for (int i = 0; i < count; ++i) {
            result.add(ContainerItem.create(ids[i], vectorLength));
        }
        return result;
    }
}
