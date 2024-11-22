package com.datastax.stargate.perf.base;

import picocli.CommandLine;

public abstract class DataApiTableTestBase extends DataApiTestBase
{
    @CommandLine.Option(names = {"-T", "--table-name"},
            defaultValue = "insert_many_test_table",
            description = "Collection name (default: 'insert_many_test_table')")
    protected String tableName;
}
