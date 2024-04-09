package com.datastax.stargate.perf.insertmany.entity;

import java.util.Objects;

import com.datastax.astra.client.model.Document;

/**
 * Lightweight wrapper for information needed to create a Document to insert
 * into a Collection.
 */
public class CollectionItem
{
    private final String idAsString;
    private final float[] vector;

    private final int value;

    private final String description;

    private CollectionItem(CollectionItemId id,
                           int value, String description, float[] vector) {
        this.idAsString = id.toString();
        this.vector = vector;
        this.value = value;
        this.description = description;
    }

    public static CollectionItem create(CollectionItemId id, int vectorLength) {
        return new CollectionItem(Objects.requireNonNull(id),
                id.generateTestInt(), id.generateString(100),
                (vectorLength < 1) ? null : id.generateVector(vectorLength));
    }

    public Document toDocument() {
        Document doc = new Document(idAsString);
        if (vector != null) {
            doc = doc.vector(vector);
        }
        doc.put("value", value);
        doc.put("description", description);
        return doc;
    }

    public String idAsString() {
        return idAsString;
    }
}
