package com.datastax.stargate.perf.insertmany;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import picocli.CommandLine;
import picocli.CommandLine.Option;

import com.datastax.astra.client.core.options.DataAPIOptions;
import com.datastax.astra.client.databases.Database;

@CommandLine.Command(name = "DataApiInsertManyTest", mixinStandardHelpOptions=true)
public class DataApiInsertManyTest
    extends DataApiTestBase
    implements Callable<Integer>
{
    @Option(names = {"-b", "--batch-size"},
            description = "Batch size for inserts (default: 20)")
    int batchSize = 20;

    @Option(names = {"-r", "--rate", "--rate-limit"},
            description = "Rate limit as RPS (default: 100)")
    int rateLimitRPS = 100;

    @Option(names = {"-a", "--agent-count"},
            description = "Agent count (also: thread count) (default: 10)")
    int agentCount = 10;

    int maxDocsToInsert;

    @Override
    public Integer call()
    {
        maxDocsToInsert = Math.max(batchSize, DataAPIOptions.DEFAULT_MAX_CHUNK_SIZE);

        final AtomicInteger exitCode = new AtomicInteger(-1);
        Database db = initializeDB(exitCode);

        if (db == null) {
            return exitCode.get();
        }


        System.out.printf("Fetch names of existing Collections in the database: ");
        Stream<String> collectionNames;

        try {
            collectionNames	= db.listCollectionNames();
            System.out.println(collectionNames.toList());
        } catch (Exception e) {
            System.err.printf("\n  FAIL: (%s) %s\n", e.getClass().getSimpleName(),
                    e.getMessage());
            return 3;
        }

        InsertManyClient testClient = new InsertManyClient(db, collectionName,
                vectorLength, orderedInserts, batchSize);
        System.out.printf("Initialize test client (collection '%s'):\n", collectionName);
        try {
            testClient.initialize(skipInit, addIndexes);
        } catch (Exception e) {
            System.err.printf("\n  FAIL: (%s) %s\n", e.getClass().getSimpleName(),
                    e.getMessage());
            return 3;
        }
        System.out.printf("Ok: Initialization of '%s' successful.\n", collectionName);
        System.out.printf("Validate that inserts to '%s' work.\n", collectionName);
        try {
            testClient.validate();
        } catch (Exception e) {
            System.err.printf("\n  FAIL: (%s) %s\n", e.getClass().getSimpleName(),
                    e.getMessage());
            return 4;
        }
        System.out.printf("Ok: Validation of '%s' successful.\n", collectionName);

        System.out.printf("Start warm-up, run test against '%s'.\n", collectionName);
        try {
            testClient.runWarmupAndTest(agentCount, rateLimitRPS);
        } catch (Exception e) {
            System.err.printf("\n  FAIL: (%s) %s\n", e.getClass().getSimpleName(),
                    e.getMessage());
            return 5;
        }

        System.out.println();
        System.out.println("DONE!");
        return 0;
    }

    @Override
    protected DataAPIOptions.DataAPIClientOptionsBuilder dataApiOptions(
            DataAPIOptions.DataAPIClientOptionsBuilder builder) {
        return builder.withMaxDocumentsInInsert(maxDocsToInsert);
    }

    public static void main(String[] args)
    {
        // Should default to true anyway but just in case...
        DataAPIOptions.encodeDataApiVectorsAsBase64 = true;

        int exitCode = new CommandLine(new DataApiInsertManyTest()).execute(args);
        System.exit(exitCode);
    }
}
