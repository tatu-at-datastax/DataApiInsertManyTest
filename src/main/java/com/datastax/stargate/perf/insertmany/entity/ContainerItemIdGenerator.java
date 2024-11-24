package com.datastax.stargate.perf.insertmany.entity;

/**
 * Generator for {@link ContainerItemId} which is based on concept of two-level
 * sequences: first, a cycle that is monotonically increasing or decreasing;
 * and then a step within that cycle (monotonically increasing).
 * Cycle starting value is configurable; steps start from 0.
 */
public class ContainerItemIdGenerator {
    /**
     * Whether {@link #cycle} value will be increasing or decreasing.
     */
    private final boolean increasing;

    private int cycle;

    private ContainerItemIdGenerator(boolean increasing, int cycle) {
        this.increasing = increasing;
        this.cycle = cycle;
    }

    public static ContainerItemIdGenerator increasingCycleGenerator(int startCycle) {
        return new ContainerItemIdGenerator(true, startCycle);
    }

    public static ContainerItemIdGenerator decreasingCycleGenerator(int startCycle) {
        return new ContainerItemIdGenerator(false, startCycle);
    }

    public synchronized ContainerItemId nextId()
    {
        nextCycle();
        return new ContainerItemId(cycle, 0);
    }

    public synchronized ContainerItemId[] nextIds(int count)
    {
        nextCycle();
        ContainerItemId[] ids = new ContainerItemId[count];
        for (int i = 0; i < count; ++i) {
            ids[i] = new ContainerItemId(cycle, i);
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
