package com.datastax.stargate.perf.base;

import picocli.CommandLine;

public abstract class DataApiTableTestBase extends DataApiTestBase
{
    @CommandLine.Option(names = {"-T", "--table-name"},
            defaultValue = "insert_many_test_api_table",
            description = "Table name (default: 'insert_many_test_api_table')")
    protected String tableName;

    protected DataApiTableTestBase() {
        super(ContainerType.API_TABLE);
    }

    @Override
    protected String containerName() {
        return tableName;
    }

    @Override
    protected boolean createIndexes() {
        return true;
    }
}
