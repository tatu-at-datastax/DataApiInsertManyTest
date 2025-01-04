package com.datastax.stargate.perf.base;

import com.datastax.astra.client.core.options.DataAPIClientOptions;
import picocli.CommandLine;

public abstract class DataApiCQLTestBase extends DataApiTestBase {
    @CommandLine.Option(names = {"-T", "--table-name"},
            defaultValue = "insert_many_test_cql_table",
            description = "Table name (default: 'insert_many_test_cql_table')")
    protected String tableName;

    @Override
    protected final DataAPIClientOptions dataApiOptions(
            DataAPIClientOptions builder) {
        throw new IllegalStateException("Should never get called on " + getClass().getName());
    }
}

