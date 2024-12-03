package com.datastax.stargate.perf.insertmany;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;

import com.datastax.stargate.perf.base.DataApiCollectionTestBase;
import com.datastax.stargate.perf.insertmany.entity.ContainerType;
import picocli.CommandLine;

import com.datastax.astra.client.core.options.DataAPIClientOptions;
import com.datastax.astra.client.databases.Database;

@CommandLine.Command(name = "InsertManyCollectionTest", mixinStandardHelpOptions=true)
public class InsertManyCollectionTest
    extends DataApiCollectionTestBase
    implements Callable<Integer>
{
    // protected int maxDocsToInsert;

    @Override
    public Integer call()
    {
        // maxDocsToInsert = Math.max(batchSize, DataAPIClientOptions.DEFAULT_MAX_CHUNK_SIZE);

        final AtomicInteger exitCode = new AtomicInteger(-1);
        Database db = initializeDB(exitCode);

        if (db == null) {
            return exitCode.get();
        }


        System.out.printf("Fetch names of existing Collections in the database: ");
        List<String> collectionNames;

        try {
            collectionNames	= db.listCollectionNames();
            System.out.println(collectionNames);
        } catch (Exception e) {
            System.err.printf("\n  FAIL/c1: (%s) %s\n", e.getClass().getSimpleName(),
                    e);
            return 3;
        }

        InsertManyClient testClient = new InsertManyClient(db, ContainerType.COLLECTION,
                collectionName, vectorLength, orderedInserts, batchSize);
        System.out.printf("Initialize test client (collection '%s'):\n", collectionName);
        try {
            testClient.initialize(skipInit, addIndexes);
        } catch (Exception e) {
            System.err.printf("\n  FAIL/c2: (%s) %s\n", e.getClass().getSimpleName(),
                    e);
            return 3;
        }
        System.out.printf("Ok: Initialization of '%s' successful.\n", collectionName);
        System.out.printf("Validate that inserts to '%s' work.\n", collectionName);
        try {
            testClient.validate();
        } catch (Exception e) {
            System.err.printf("\n  FAIL/c3: (%s) %s\n", e.getClass().getSimpleName(),
                    e);
            e.printStackTrace();
            return 4;
        }
        System.out.printf("Ok: Validation of '%s' successful.\n", collectionName);

        System.out.printf("Start warm-up, run test against '%s'.\n", collectionName);
        try {
            testClient.runWarmupAndTest(agentCount, rateLimitRPS);
        } catch (Exception e) {
            System.err.printf("\n  FAIL/c4: (%s) %s\n", e.getClass().getSimpleName(),
                    e);
            e.printStackTrace();
            return 5;
        }

        System.out.println();
        System.out.println("DONE!");
        return 0;
    }

    @Override
    protected DataAPIClientOptions dataApiOptions(
            DataAPIClientOptions opts) {
        // 02-Dec-2024, tatu: Used to have this option, but no longer?
        // opts.maxDocumentsInInsert(maxDocsToInsert);
        return opts;
    }

    public static void main(String[] args)
    {
        // Should default to true anyway but just in case...
        // This looks weird tho.
        DataAPIClientOptions.getSerdesOptions().encodeDataApiVectorsAsBase64(true);

        int exitCode = new CommandLine(new InsertManyCollectionTest()).execute(args);
        System.exit(exitCode);
    }
}
