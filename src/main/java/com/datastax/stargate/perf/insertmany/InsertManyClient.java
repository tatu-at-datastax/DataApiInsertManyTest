package com.datastax.stargate.perf.insertmany;

import java.util.List;
import java.util.Objects;

import com.datastax.astra.client.collections.Collection;
import com.datastax.astra.client.collections.definition.CollectionDefinition;
import com.datastax.astra.client.collections.definition.documents.Document;
import com.datastax.astra.client.databases.Database;
import com.datastax.astra.client.core.vector.SimilarityMetric;
import com.datastax.astra.client.tables.Table;
import com.datastax.astra.client.tables.commands.options.CreateTableOptions;
import com.datastax.astra.client.tables.definition.TableDefinition;
import com.datastax.astra.client.tables.definition.columns.ColumnDefinitionVector;
import com.datastax.astra.client.tables.definition.columns.ColumnTypes;
import com.datastax.astra.client.tables.definition.rows.Row;
import com.datastax.stargate.perf.insertmany.entity.ContainerItem;
import com.datastax.stargate.perf.insertmany.entity.ContainerItemGenerator;
import com.datastax.stargate.perf.insertmany.entity.ContainerItemIdGenerator;
import com.datastax.stargate.perf.insertmany.entity.ContainerType;
import com.datastax.stargate.perf.insertmany.entity.ItemCollection;
import com.datastax.stargate.perf.insertmany.entity.ItemContainer;
import com.datastax.stargate.perf.insertmany.entity.ItemTable;

/**
 * Wrapper around access to test Collections for Data API
 */
public class InsertManyClient
{
    final private static int VALIDATE_SINGLE_ITEMS_TO_INSERT = 8;

    final private static int VALIDATE_BATCHES_TO_INSERT = 5;

    private final Database db;
    private final ContainerType containerType;
    private final String containerName;
    private final int vectorSize;
    private final boolean orderedInserts;
    private final int batchSize;

    private ItemContainer itemContainer;

    public InsertManyClient(Database db, ContainerType containerType,
                            String containerName,
                            int vectorSize, boolean orderedInserts,
                            int batchSize) {
        this.db = db;
        this.containerType = Objects.requireNonNull(containerType);
        this.containerName = Objects.requireNonNull(containerName);
        this.vectorSize = vectorSize;
        this.orderedInserts = orderedInserts;
        this.batchSize = batchSize;
    }

    /**
     * Method that will (re)create Collection as necessary; clear (if not deleted).
     * Fails with exception if there are problems with collection access.
     */
    public void initialize(boolean skipCollectionRecreate,
                           boolean addIndexes) throws Exception
    {
        System.out.printf("  checking if %s exists: ", containerDesc());
        ItemContainer container = null;

        if (containerExists()) {
            if (skipCollectionRecreate) {
                System.out.println("it does -- and since '--skipInit' specified, will skip recreation");
                System.out.printf("  but need to truncate its contents, if any...");
                container = fetchContainer();
                long deleted = container.deleteAll();
                System.out.printf(" deleted %d documents\n", deleted);
            } else {
                System.out.println("it does -- need to delete first");
                dropContainer();
                System.out.printf("%s deleted: will wait for 3 seconds...\n", containerDesc());
                try {
                    Thread.sleep(3000L);
                } catch (InterruptedException e) { }
            }
        } else {
            System.out.println("does not -- no need to delete");
        }

        if (container == null) {
            container = createContainer(addIndexes);
        }
        itemContainer = container;
        // And let's verify Collection does exist; do by checking it is empty
        itemContainer.validateIsEmpty();
    }

    private String containerDesc() {
        return containerType.desc(containerName);
    }

    private boolean containerExists() {
        return switch (containerType) {
            case COLLECTION -> db.collectionExists(containerName);
            case TABLE -> db.tableExists(containerName);
        };
    }

    private void dropContainer() {
        switch (containerType) {
            case COLLECTION:
                db.dropCollection(containerName);
                break;
            case TABLE:
            default:
                db.dropTable(containerName);
        }
    }

    private ItemContainer fetchContainer() {
        return switch (containerType) {
            case COLLECTION -> new ItemCollection(containerName,
                    db.getCollection(containerName),
                    vectorSize, orderedInserts);
            case TABLE -> /*
                new ItemTable(containerName,
                    db.getCollection(containerName),
                    vectorSize, orderedInserts);
                    */
                    null;
        };
    }

    private ItemContainer createContainer(boolean addIndexes) {
        return switch (containerType) {
            case COLLECTION -> createCollection(addIndexes);
            case TABLE -> createTable();
        };
    }

    private ItemCollection createCollection(boolean addIndexes) {
        CollectionDefinition def = new CollectionDefinition();
        String desc;
        if (vectorSize > 0) {
            def = def.vector(vectorSize, SimilarityMetric.COSINE);
            desc = "vector: "+vectorSize+"/"+SimilarityMetric.COSINE;
        } else {
            desc = "vector: NONE";
        }
        if (addIndexes) {
            desc += ", index: ALL";
        } else {
            def = def.indexingDeny("*");
            desc += ", index: NONE";
        }
        System.out.printf("Will (re)create %s (%s): ",
                containerDesc(), desc);

        final long start = System.currentTimeMillis();
        Collection<Document> coll = db.createCollection(containerName, def);
        System.out.printf("created (in %s)); definition = %s\n",
                _secs(System.currentTimeMillis() - start),
                def);
        return new ItemCollection(containerName, coll, vectorSize, orderedInserts);
    }

    private ItemTable createTable() {
        CreateTableOptions options = new CreateTableOptions()
                .ifNotExists(false);
        TableDefinition tableDef = new TableDefinition();
        tableDef = tableDef.addColumnText("id");
        tableDef = tableDef.addColumnText("description");
        tableDef = tableDef.addColumn("value", ColumnTypes.BIGINT);

        String desc;
        boolean hasVector = vectorSize > 0;
        if (hasVector) {
            tableDef = tableDef.addColumnVector("vector",
                    new ColumnDefinitionVector().dimension(vectorSize)
                            .metric(SimilarityMetric.COSINE));
            desc = "vector: "+vectorSize+"/"+SimilarityMetric.COSINE;
        } else {
            desc = "vector: NONE";
        }
        tableDef = tableDef.partitionKey("id");

        System.out.printf("Will (re)create %s (%s): ",
                containerDesc(), desc);

        long start = System.currentTimeMillis();
        Table<Row> rawTable = db.createTable(containerName, tableDef,
                options);
        System.out.printf("created (in %s))\n",
                _secs(System.currentTimeMillis() - start));
        ItemTable table = new ItemTable(containerName, rawTable, vectorSize, orderedInserts);
        if (hasVector) {
            final String indexName = "idx_vector_" + containerName;
            start = System.currentTimeMillis();
            System.out.printf("  will (re)create Vector index '%s': ", indexName);
            table.createVectorIndex(indexName, vectorSize);
            System.out.printf("created (in %s))\n",
                    _secs(System.currentTimeMillis() - start));
        } else {
            System.out.println("  will NOT (re)create index for Vector:");
        }
        return table;
    }

    /**
     * Method to call after {@link #initialize} to validate that Items can be inserted;
     * first individually, then in bulk; verifying each insertion and finally deleting
     * all Items before returning.
     */
    public void validate() {
        ContainerItemIdGenerator idGenerator = ContainerItemIdGenerator.decreasingCycleGenerator(0);
        ContainerItemGenerator itemGen = new ContainerItemGenerator(idGenerator, vectorSize);

        System.out.printf("  will insert %d documents, one by one:\n", VALIDATE_SINGLE_ITEMS_TO_INSERT);
        for (int i = 0; i < VALIDATE_SINGLE_ITEMS_TO_INSERT; ++i) {
            final long start = System.currentTimeMillis();
            ContainerItem item = itemGen.generateSingle();
            itemContainer.insertItem(item);
            System.out.printf("    inserted item #%d/%d: %s (in %s)",
                    i+1, VALIDATE_SINGLE_ITEMS_TO_INSERT, item.idAsString(),
                    _secs(System.currentTimeMillis() - start));
            // fetch to validate
            ContainerItem result = itemContainer.findItem(item.idAsString());
            verifyItem(item, result);
            System.out.println("(verified: OK)");
        }

        // Let's use half the batch size
        final int testBatchSize = batchSize / 2;
        System.out.printf("  will now insert %d batches of %d documents (ordered: %s):\n",
                VALIDATE_BATCHES_TO_INSERT, testBatchSize,
                orderedInserts);

        for (int i = 0; i < VALIDATE_BATCHES_TO_INSERT; ++i) {
            final long start = System.currentTimeMillis();
            List<ContainerItem> items = itemGen.generate(testBatchSize);
            itemContainer.insertItems(items);
            System.out.printf("    inserted Batch #%d/%d (in %s)",
                    i+1, VALIDATE_BATCHES_TO_INSERT,
                    _secs(System.currentTimeMillis() - start));
            // Validate one by one
            for (ContainerItem item : items) {
                ContainerItem result = itemContainer.findItem(item.idAsString());
                verifyItem(item, result);
            }
            System.out.println("(verified: OK)");
        }

        // Should now have certain number of Docs:
        final int expCount = VALIDATE_SINGLE_ITEMS_TO_INSERT
                + (VALIDATE_BATCHES_TO_INSERT * testBatchSize);

        System.out.printf("  all inserted and verified: should now have %d documents, verify: ", expCount);

        final long actCount = itemContainer.countItems(expCount + 100);
        if (expCount == actCount) {
            System.out.println("OK (had expected number)");
        // 25-Nov-2024, tatu: Tables do not support count yet, so let's just warn
        } else if (actCount == -1) {
            System.out.println("MAYBE-OK (Tables do not support count yet)");
        } else {
            throw new IllegalStateException("Expected to have " + expCount + " documents, had " + actCount);
        }

        // And all this being done, let's delete all items
        System.out.printf("  and now let's delete all items: ");
        long count = itemContainer.deleteAll();
        System.out.printf(" deleted %d documents; validate: ", count);
        itemContainer.validateIsEmpty();
        System.out.println("OK, now empty");
    }

    public void runWarmupAndTest(int threadCount, int testMaxRPS)
        throws InterruptedException
    {
        final ContainerItemGenerator itemGenerator = new ContainerItemGenerator(
                ContainerItemIdGenerator.increasingCycleGenerator(0),
                vectorSize);
        final TestPhaseRunner testRunner = new TestPhaseRunner(threadCount,
                itemContainer, itemGenerator, batchSize);

        // Warm-up with only 25% of full RPS; for 5 seconds
        testRunner.runPhase("Warm-up", 5, java.util.concurrent.TimeUnit.SECONDS,
                testMaxRPS / 4);

        // Actual test with full RPS; for 1 minute
        testRunner.runPhase("Main Test", 60, java.util.concurrent.TimeUnit.SECONDS,
                testMaxRPS);
    }

    private static void verifyItem(ContainerItem expected, ContainerItem actual) {
        if (actual == null) {
            throw new IllegalStateException("Failed to find inserted document with key '"
                    +expected.idAsString()+"'");
        }
        // Otherwise verify fields.
        ContainerItem.verifySimilarity(expected, actual);
    }

    private static String _secs(long msecs) {
        return "%.3f sec".formatted(msecs / 1000.0);
    }
}
