package com.datastax.stargate.perf.insertmany;

import com.datastax.astra.client.core.options.DataAPIClientOptions;
import com.datastax.astra.client.databases.Database;
import com.datastax.stargate.perf.base.DataApiCQLTestBase;
import com.datastax.stargate.perf.base.ContainerType;
import com.datastax.stargate.perf.base.DataApiTestClient;
import picocli.CommandLine;

@CommandLine.Command(name = "InsertManyCQLTest", mixinStandardHelpOptions=true)
public class InsertManyCQLTest
    extends DataApiCQLTestBase
{
    @Override
    protected InsertManyTestClient createTestClient(Database db) {
        return new InsertManyTestClient(db, containerType,
                tableName, vectorLength, orderedInserts, batchSize);
    }

    @Override
    protected DataAPIClientOptions dataApiOptions(DataAPIClientOptions opts) {
        return opts;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new InsertManyCQLTest()).execute(args));
    }
}
