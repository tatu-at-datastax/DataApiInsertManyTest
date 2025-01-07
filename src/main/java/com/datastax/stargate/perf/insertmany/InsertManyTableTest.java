package com.datastax.stargate.perf.insertmany;

import com.datastax.astra.client.core.options.DataAPIClientOptions;
import com.datastax.astra.client.databases.Database;
import com.datastax.stargate.perf.base.DataApiTableTestBase;
import com.datastax.stargate.perf.base.ContainerType;
import com.datastax.stargate.perf.base.DataApiTestClient;
import picocli.CommandLine;

@CommandLine.Command(name = "InsertManyTableTest", mixinStandardHelpOptions=true)
public class InsertManyTableTest
    extends DataApiTableTestBase
{
    @Override
    protected InsertManyTestClient createTestClient(Database db) {
        return new InsertManyTestClient(db, containerType, tableName,
                vectorLength, orderedInserts, batchSize);
    }

    @Override
    protected DataAPIClientOptions dataApiOptions(DataAPIClientOptions opts) {
        // 02-Dec-2024, tatu: Used to have this option, but no longer?
        // opts.maxDocumentsInInsert(maxDocsToInsert);
        return opts;
    }

    public static void main(String[] args) {
        // Should default to true anyway but just in case...
        // This looks weird tho.
        DataAPIClientOptions.getSerdesOptions().encodeDataApiVectorsAsBase64(true);

        System.exit(new CommandLine(new InsertManyTableTest()).execute(args));
    }
}
