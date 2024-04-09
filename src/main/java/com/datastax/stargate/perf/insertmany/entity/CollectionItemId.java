package com.datastax.stargate.perf.insertmany.entity;

/**
 * Immutable id for a collection item; used both as generating Document key and
 * for generating Document content in a reproducible manner.
 */
public record CollectionItemId(int cycle, int step) {
    public long seedForRandom() {
        return ((long) cycle << 32) | step;
    }

    @Override
    public String toString() {
        return new StringBuilder(16).append(cycle).append('-').append(step).toString();
    }
}
