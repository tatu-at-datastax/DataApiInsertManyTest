package com.datastax.stargate.perf.insertmany.entity;

import com.datastax.astra.client.collections.documents.Document;
import com.datastax.astra.client.collections.options.CollectionInsertManyOptions;
import com.datastax.astra.client.core.query.Filter;
import com.datastax.astra.client.core.query.FilterOperator;
import com.datastax.astra.client.core.vector.SimilarityMetric;
import com.datastax.astra.client.exception.DataAPIException;
import com.datastax.astra.client.tables.Table;
import com.datastax.astra.client.tables.index.VectorIndexDefinition;
import com.datastax.astra.client.tables.index.VectorIndexDefinitionOptions;
import com.datastax.astra.client.tables.options.TableInsertManyOptions;
import com.datastax.astra.client.tables.results.TableInsertManyResult;
import com.datastax.astra.client.tables.results.TableInsertOneResult;
import com.datastax.astra.client.tables.row.Row;

import java.util.List;
import java.util.Optional;

/**
 * Wrapper around an API Table.
 */
public record ItemTable(String name, Table<Row> table,
                        int vectorSize, boolean orderedInserts)
    implements ItemContainer
{
    @Override
    public void validateIsEmpty() {
        // If "countDocuments()" was supported, we would do:
        if (false) {
            long count = countItems(100);
            if (count > 0) {
                throw new IllegalStateException("Table '" + name + "' not empty; has " + count + " rows");
            }
        }
        // ... but since not, try to fetch a single item
        Optional<Row> row = table.findOne(new Filter());
        if (row.isPresent()) {
            throw new IllegalStateException("Table '" + name + "' not empty; has at least one row");
        }
    }

    @Override
    public long countItems(int maxCount) {
        // Not yet supported by Data API for Tables, so:
        //return table.countRows(maxCount);
        return -1L;
    }

    @Override
    public void insertItem(ContainerItem item) throws DataAPIException {
        TableInsertOneResult result = table.insertOne(item.toTableRow());
        List<Object> primaryKeys = result.getInsertedId();
        if (primaryKeys.size() != 1) {
            throw new IllegalStateException(String.format(
                    "Unexpected id response for inserted document: expected List<1>, got List<%d>",
                    primaryKeys.size()));
        }
        Object resultId = primaryKeys.get(0);
        if (!item.idAsString().equals(resultId)) {
            throw new IllegalStateException(String.format(
                    "Unexpected id for inserted document: expected %s, got %s",
                    _str(item.idAsString()), _str(resultId)));
        }
    }

    @Override
    public boolean insertItems(List<ContainerItem> items) throws DataAPIException {
        // Special case: 1 item, simply use "insertOne()" instead
        if (items.size() == 1) {
            insertItem(items.get(0));
            return true;
        }
        List<Row> rows = items.stream()
                .map(item -> item.toTableRow())
                .toList();
        TableInsertManyOptions options = new TableInsertManyOptions()
                .ordered(orderedInserts);
        if (items.size() > options.chunkSize()) {
            options = options.chunkSize(items.size());
        }

        TableInsertManyResult result = table.insertMany(rows, options);
        List<?> ids = result.getInsertedIds();
        if (ids == null || ids.size() != items.size()) {
            return false;
        }

        return true;
    }

    @Override
    public ContainerItem findItem(String idAsSring) {
        // NOTE: don't use "findById" as that assumes "_id" key
        Filter idFilter = new Filter("id", FilterOperator.EQUALS_TO, idAsSring);
        Optional<Row> row = table.findOne(idFilter);
        return ContainerItem.fromTableRow(row);
    }

    @Override
    public long deleteAll() {
        table.deleteAll();
        return -1L;
    }

    public void createVectorIndex(String idxName, int dimension) {
        table.createVectorIndex(idxName, new VectorIndexDefinition()
                .column("vector")
                .options(new VectorIndexDefinitionOptions()
                        .metric(SimilarityMetric.COSINE)));
    }

    private static String _str(Object ob) {
        if (ob == null) {
            return "`null`";
        }
        if (ob instanceof String) {
            return "'"+ob+"'";
        }
        return String.format("(%s)'%s'", ob.getClass().getName(), ob);
    }
}
