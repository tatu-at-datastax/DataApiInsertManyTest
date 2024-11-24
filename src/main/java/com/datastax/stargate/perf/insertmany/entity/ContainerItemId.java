package com.datastax.stargate.perf.insertmany.entity;

import java.util.Random;

/**
 * Immutable id for a collection item; used both as generating Document key and
 * for generating Document content in a reproducible manner.
 */
public record ContainerItemId(int cycle, int step) {
    public long seedForRandom() {
        return ((long) cycle << 32) | step;
    }

    public int generateTestInt() {
        return cycle << 4 + step;
    }

    public String generateString(int length) {
        final StringBuilder sb = new StringBuilder(length);
        final Random rnd = new Random(seedForRandom());

        for (int i = 0; i < length; ++i) {
            if ((i & 7) == 7) {
                sb.append(' ');
            } else {
                sb.append((char) ('a' + rnd.nextInt(25)));
            }
        }
        return sb.toString();
    }

    public float[] generateVector(int vectorLength) {
        Random rnd = new Random(seedForRandom());
        float[] result = new float[vectorLength];
        // TODO: pre-generate chunks/blocks, compose
        for (int i = 0; i < vectorLength; ++i) {
            // Scale from [0.0, 1.0) to [-1.0, 1.0)
            result[i] = rnd.nextFloat() * 2 - 1;
        }
        return result;

    }

    @Override
    public String toString() {
        return new StringBuilder(16).append("id#").append(cycle).append('_').append(step).toString();
    }
}
