package com.datastax.stargate.perf.insertmany.entity;

/**
 * Generator for {@link CollectionItemId} which is based on concept of two-level
 * sequences: first, a cycle that is monotonically increasing or decreasing;
 * and then a step within that cycle (monotonically increasing).
 * Cycle starting value is configurable; steps start from 0.
 */
public class CollectionItemIdGenerator {
    /**
     * Whether {@link #cycle} value will be increasing or decreasing.
     */
    private final boolean increasing;

    private int cycle;

    private CollectionItemIdGenerator(boolean increasing, int cycle) {
        this.increasing = increasing;
        this.cycle = cycle;
    }

    public static CollectionItemIdGenerator increasingCycleGenerator(int startCycle) {
        return new CollectionItemIdGenerator(true, startCycle);
    }

    public static CollectionItemIdGenerator decreasingCycleGenerator(int startCycle) {
        return new CollectionItemIdGenerator(false, startCycle);
    }

    public synchronized CollectionItemId nextId()
    {
        nextCycle();
        return new CollectionItemId(cycle, 0);
    }

    public synchronized CollectionItemId[] nextIds(int count)
    {
        nextCycle();
        CollectionItemId[] ids = new CollectionItemId[count];
        for (int i = 0; i < count; ++i) {
            ids[i] = new CollectionItemId(cycle, i);
        }
        return ids;
    }

    private void nextCycle() {
        if (increasing) {
            ++cycle;
        } else {
            --cycle;
        }
    }
}
