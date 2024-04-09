package com.datastax.stargate.perf.insertmany.entity;

import com.datastax.stargate.perf.insertmany.entity.CollectionItemId;

import java.util.Random;

public record VectorGenerator(int vectorLength)
{
    public float[] generate(CollectionItemId id) {
        Random rnd = new Random(id.seedForRandom());
        float[] result = new float[vectorLength];
        // TODO: pre-generate chunks/blocks, compose
        for (int i = 0; i < vectorLength; ++i) {
            // Scale from [0.0, 1.0) to [-1.0, 1.0)
            result[i] = rnd.nextFloat() * 2 - 1;
        }
        return result;
    }
}
