package com.datastax.stargate.perf.insertmany.entity;

/**
 * Lightweight wrapper for information needed to create a Document to insert
 * into a Collection.
 */
public record CollectionItem(CollectionItemId id, float[] vector) {
}
