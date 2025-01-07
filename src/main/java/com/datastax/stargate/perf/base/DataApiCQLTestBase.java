package com.datastax.stargate.perf.base;

import picocli.CommandLine;

public abstract class DataApiCQLTestBase extends DataApiTestBase {
    @CommandLine.Option(names = {"-T", "--table-name"},
            defaultValue = "insert_many_test_cql_table",
            description = "Table name (default: 'insert_many_test_cql_table')")
    protected String tableName;

    protected DataApiCQLTestBase() {
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

