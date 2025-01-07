package com.datastax.stargate.perf.insertmany;

import com.datastax.stargate.perf.base.DataApiCollectionTestBase;
import com.datastax.stargate.perf.base.DataApiTestClient;
import picocli.CommandLine;

import com.datastax.astra.client.core.options.DataAPIClientOptions;
import com.datastax.astra.client.databases.Database;

@CommandLine.Command(name = "InsertManyCollectionTest", mixinStandardHelpOptions=true)
public class InsertManyCollectionTest
    extends DataApiCollectionTestBase
{
    protected InsertManyCollectionTest() { }

    @Override
    protected DataApiTestClient createTestClient(Database db) {
        return new InsertManyTestClient(db, containerType,
                collectionName, vectorLength, orderedInserts, batchSize);
    }

    @Override
    protected DataAPIClientOptions dataApiOptions(
            DataAPIClientOptions opts) {
        // 02-Dec-2024, tatu: Used to have this option, but no longer?
        // opts.maxDocumentsInInsert(maxDocsToInsert);
        return opts;
    }

    public static void main(String[] args) {
        // Should default to true anyway but just in case...
        // This looks weird tho.
        DataAPIClientOptions.getSerdesOptions().encodeDataApiVectorsAsBase64(true);
        System.exit(new CommandLine(new InsertManyCollectionTest()).execute(args));
    }
}
