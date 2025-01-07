package com.datastax.stargate.perf.base;

import picocli.CommandLine;

public abstract class DataApiCollectionTestBase extends DataApiTestBase
{
    @CommandLine.Option(names = {"-c", "--collection-name"},
            defaultValue = "insert_many_test_collection",
            description = "Collection name (default: 'insert_many_test_collection')")
    protected String collectionName;

    @CommandLine.Option(names = {"-x", "--index", "--add-indexes"}, arity="1",
            description = "Whether to Index (all) Fields or not (default: true)")
    protected boolean addIndexes = true;

    protected DataApiCollectionTestBase() {
        super(ContainerType.COLLECTION);
    }

    @Override
    protected String containerName() {
        return collectionName;
    }

    @Override
    protected boolean createIndexes() {
        return addIndexes;
    }
}
