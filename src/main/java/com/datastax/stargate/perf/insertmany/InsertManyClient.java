package com.datastax.stargate.perf.insertmany;

import java.util.List;

import com.datastax.astra.client.Collection;
import com.datastax.astra.client.Database;
import com.datastax.astra.client.model.DeleteResult;
import com.datastax.astra.client.model.Document;
import com.datastax.astra.client.model.SimilarityMetric;
import com.datastax.stargate.perf.insertmany.entity.CollectionItem;
import com.datastax.stargate.perf.insertmany.entity.CollectionItemGenerator;
import com.datastax.stargate.perf.insertmany.entity.CollectionItemIdGenerator;
import com.datastax.stargate.perf.insertmany.entity.ItemCollection;

/**
 * Wrapper around access to test Collections for Data API
 */
public class InsertManyClient
{
    final private static int VALIDATE_SINGLE_ITEMS_TO_INSERT = 8;

    final private static int VALIDATE_BATCHES_TO_INSERT = 5;

    private final Database db;
    private final String collectionName;
    private final int vectorSize;
    private final boolean orderedInserts;
    private final int batchSize;

    private ItemCollection itemCollection;

    public InsertManyClient(Database db, String collectionName,
                            int vectorSize, boolean orderedInserts,
                            int batchSize) {
        this.db = db;
        this.collectionName = collectionName;
        this.vectorSize = vectorSize;
        this.orderedInserts = orderedInserts;
        this.batchSize = batchSize;
    }

    /**
     * Method that will (re)create Collection as necessary; clear (if not deleted).
     * Fails with exception if there are problems with collection access.
     */
    public void initialize(boolean skipCollectionRecreate) throws Exception
    {
        System.out.printf("  checking if collection '%s' exists: ", collectionName);
        Collection<Document> coll = null;

        if (db.collectionExists(collectionName)) {
            if (skipCollectionRecreate) {
                System.out.println("it does -- and since '--skipInit' specified, will skip recreation");
                System.out.printf("  but need to truncate its contents, if any...");
                coll = db.getCollection(collectionName);
                DeleteResult dr = coll.deleteAll();
                System.out.printf(" deleted %d documents\n", dr.getDeletedCount());
            } else {
                System.out.println("it does -- need to delete first");
                db.dropCollection(collectionName);
                System.out.printf("Collection '%s' deleted: will wait for 3 seconds...\n", collectionName);
                try {
                    Thread.sleep(3000L);
                } catch (InterruptedException e) { }
            }
        } else {
            System.out.println("does not -- no need to delete");
        }

        if (coll == null) {
            System.out.printf("Will (re)create collection '%s': ", collectionName);
            final long start = System.currentTimeMillis();
            coll = (vectorSize > 0)
                    ? db.createCollection(collectionName, vectorSize, SimilarityMetric.COSINE)
                    : db.createCollection(collectionName);
            System.out.printf("created (in %s)); options = %s\n",
                    _secs(System.currentTimeMillis() - start),
                    coll.getDefinition().getOptions());
        }
        itemCollection = new ItemCollection(collectionName, coll, vectorSize, orderedInserts);
        // And let's verify Collection does exist; do by checking it's empty
        itemCollection.validateIsEmpty();
    }

    /**
     * Method to call after {@link #initialize} to validate that Items can be inserted;
     * first individually, then in bulk; verifying each insertion and finally deleting
     * all Items before returning.
     */
    public void validate() {
        CollectionItemIdGenerator idGenerator = CollectionItemIdGenerator.decreasingCycleGenerator(0);
        CollectionItemGenerator itemGen = new CollectionItemGenerator(idGenerator, vectorSize);

        System.out.printf("  will insert %d documents, one by one:\n", VALIDATE_SINGLE_ITEMS_TO_INSERT);
        for (int i = 0; i < VALIDATE_SINGLE_ITEMS_TO_INSERT; ++i) {
            final long start = System.currentTimeMillis();
            CollectionItem item = itemGen.generateSingle();
            itemCollection.insertItem(item);
            System.out.printf("    inserted item #%d/%d: %s (in %s)",
                    i+1, VALIDATE_SINGLE_ITEMS_TO_INSERT, item.idAsString(),
                    _secs(System.currentTimeMillis() - start));
            // fetch to validate
            CollectionItem result = itemCollection.findItem(item.idAsString());
            verifyItem(item, result);
            System.out.println("(verified: OK)");
        }

        System.out.printf("  will now insert %d batches of %d documents (ordered: %s):\n",
                VALIDATE_BATCHES_TO_INSERT, batchSize,
                orderedInserts);

        for (int i = 0; i < VALIDATE_BATCHES_TO_INSERT; ++i) {
            final long start = System.currentTimeMillis();
            List<CollectionItem> items = itemGen.generate(batchSize);
            itemCollection.insertItems(items);
            System.out.printf("    inserted Batch #%d/%d (in %s)",
                    i+1, VALIDATE_BATCHES_TO_INSERT,
                    _secs(System.currentTimeMillis() - start));
            // Validate one by one
            for (CollectionItem item : items) {
                CollectionItem result = itemCollection.findItem(item.idAsString());
                verifyItem(item, result);
            }
            System.out.println("(verified: OK)");
        }

        // Should now have certain number of Docs:
        final int expCount = VALIDATE_SINGLE_ITEMS_TO_INSERT + (VALIDATE_BATCHES_TO_INSERT * batchSize);

        System.out.printf("  all inserted and verified: should now have %d documents, verify: ", expCount);

        final long actCount = itemCollection.countItems(expCount + 100);
        if (expCount != actCount) {
            throw new IllegalStateException("Expected to have "+expCount+" documents, had "+actCount);
        }
        System.out.println("OK (had expected number)");

        // And all this being done, let's delete all items
        System.out.printf("  and now let's delete all items: ");
        long count = itemCollection.deleteAll();
        System.out.printf(" deleted %d documents; validate: ", count);
        itemCollection.validateIsEmpty();
        System.out.println("OK, now empty");
    }

    public void runWarmupAndTest(int threadCount, int testMaxRPS)
        throws InterruptedException
    {
        final CollectionItemGenerator itemGenerator = new CollectionItemGenerator(
                CollectionItemIdGenerator.increasingCycleGenerator(0),
                vectorSize);
        final TestPhaseRunner testRunner = new TestPhaseRunner(threadCount,
                itemCollection, itemGenerator, batchSize);

        // Warm-up with only 25% of full RPS; for 20 seconds
        testRunner.runPhase("Warm-up", 20, java.util.concurrent.TimeUnit.SECONDS,
                testMaxRPS / 4);

        // Actual test with full RPS; for 1 minute
        testRunner.runPhase("Main Test", 60, java.util.concurrent.TimeUnit.SECONDS,
                testMaxRPS);
    }

    private static void verifyItem(CollectionItem expected, CollectionItem actual) {
        if (actual == null) {
            throw new IllegalStateException("Failed to find inserted document with key '"
                    +expected.idAsString()+"'");
        }
        // Otherwise verify fields.
        CollectionItem.verifySimilarity(expected, actual);
    }

    private static String _secs(long msecs) {
        return "%.3f sec".formatted(msecs / 1000.0);
    }
}
