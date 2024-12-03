package com.datastax.stargate.perf.insertmany;

import com.datastax.astra.client.core.options.DataAPIClientOptions;
import com.datastax.astra.client.databases.Database;
import com.datastax.stargate.perf.base.DataApiTableTestBase;
import com.datastax.stargate.perf.insertmany.entity.ContainerType;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

@CommandLine.Command(name = "InsertManyTableTest", mixinStandardHelpOptions=true)
public class InsertManyTableTest
    extends DataApiTableTestBase
    implements Callable<Integer>
{
    // protected int maxDocsToInsert;

    @Override
    public Integer call()
    {
        //maxDocsToInsert = Math.max(batchSize, DataAPIClientOptions.DEFAULT_MAX_CHUNK_SIZE);

        final AtomicInteger exitCode = new AtomicInteger(-1);
        Database db = initializeDB(exitCode);

        if (db == null) {
            return exitCode.get();
        }


        System.out.printf("Fetch names of existing Tables in the database: ");
        List<String> tableNames;

        try {
            tableNames	= db.listTableNames();
            System.out.println(tableNames);
        } catch (Exception e) {
            System.err.printf("\n  FAIL/0: (%s) %s\n", e.getClass().getSimpleName(),
                    e);
            return 3;
        }

        InsertManyClient testClient = new InsertManyClient(db, ContainerType.TABLE,
                tableName, vectorLength, orderedInserts, batchSize);

        System.out.printf("Initialize test client (table '%s'):\n", tableName);
        try {
            testClient.initialize(skipInit, true);
        } catch (Exception e) {
            System.err.printf("\n  FAIL/1: (%s) %s\n", e.getClass().getName(),
                    e);
            e.printStackTrace();
            return 3;
        }
        System.out.printf("Ok: Initialization of '%s' successful.\n", tableName);
        System.out.printf("Validate that inserts to '%s' work.\n", tableName);
        try {
            testClient.validate();
        } catch (Exception e) {
            System.err.printf("\n  FAIL/2: (%s) %s\n", e.getClass().getName(),
                    e);
            return 4;
        }
        System.out.printf("Ok: Validation of '%s' successful.\n", tableName);

        System.out.printf("Start warm-up, run test against '%s'.\n", tableName);
        try {
            testClient.runWarmupAndTest(agentCount, rateLimitRPS);
        } catch (Exception e) {
            System.err.printf("\n  FAIL/3: (%s) %s\n", e.getClass().getName(),
                    e);
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

        int exitCode = new CommandLine(new InsertManyTableTest()).execute(args);
        System.exit(exitCode);
    }
}
