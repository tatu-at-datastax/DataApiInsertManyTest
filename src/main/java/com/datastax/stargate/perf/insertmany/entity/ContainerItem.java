package com.datastax.stargate.perf.insertmany.entity;

import java.util.Objects;
import java.util.Optional;

import com.datastax.astra.client.collections.documents.Document;
import com.datastax.astra.client.core.vector.DataAPIVector;
import com.datastax.astra.client.tables.columns.ColumnTypes;
import com.datastax.astra.client.tables.mapping.Column;
import com.datastax.astra.client.tables.row.Row;

/**
 * Lightweight wrapper for information needed to create a Document or Row to insert
 * into a Collection/Table.
 */
public class ContainerItem
{
    @Column(value = "id", type = ColumnTypes.TEXT)
    public final String idAsString;

    @Column(value = "value", type = ColumnTypes.BIGINT)
    public final long value;

    @Column(value = "description", type = ColumnTypes.TEXT)
    public final String description;

    @Column(value = "vector", type = ColumnTypes.VECTOR)
    public final float[] vector;

    private ContainerItem(String idAsString,
                          long value, String description, float[] vector) {
        this.idAsString = idAsString;
        this.vector = vector;
        this.value = value;
        this.description = description;
    }

    public static ContainerItem create(ContainerItemId id, int vectorLength) {
        return new ContainerItem(Objects.requireNonNull(id).toString(),
                id.generateTestInt(), id.generateString(100),
                (vectorLength < 1) ? null : id.generateVector(vectorLength));
    }

    public static ContainerItem fromDocument(Optional<Document> maybeDoc) {
        return maybeDoc.isPresent() ? fromDocument(maybeDoc.get()) : null;
    }

    public static ContainerItem fromDocument(Document doc) {
        String id = String.valueOf(doc.getId(Object.class));
        Number num = doc.get("value", Number.class);
        return new ContainerItem(id,
                (num == null) ? 0L : num.longValue(),
                String.valueOf(doc.get("description")),
                null);
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

    public Row toTableRow() {
        Row row = Row.create()
                .addText("id", idAsString)
                .addBigInt("value", value)
                .addText("description", description);
        if (vector != null) {
            row = row.addVector("vector",
                    new DataAPIVector(vector));
        }
        return row;
    }

    public String idAsString() {
        return idAsString;
    }

    public static void verifySimilarity(ContainerItem exp, ContainerItem actual) {
        if (!Objects.equals(exp.idAsString, actual.idAsString)) {
            throw new IllegalStateException(String.format(
                    "Unexpected 'id': expected '%s', got '%s'",
                    exp.idAsString, actual.idAsString));
        }
        if (!Objects.equals(exp.value, actual.value)) {
            throw new IllegalStateException(String.format(
                    "Unexpected 'value': expected %s, got %s",
                    exp.value, actual.value));
        }
        if (!Objects.equals(exp.description, actual.description)) {
            throw new IllegalStateException(String.format(
                    "Unexpected 'description': expected %s, got %s",
                    exp.description, actual.description));
        }
        // Leave out $vector, not populated when fetching
    }
}
