package com.datastax.stargate.perf.insertmany;

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
public class CollectionTestClient
{
    final private static int VALIDATE_SINGLE_ITEMS_TO_INSERT = 10;

    final private static int VALIDATE_BATCHES_TO_INSERT = 10;

    final private static int VALIDATE_BATCH_SIZE = 10;

    private final Database db;
    private final String collectionName;
    private final int vectorSize;

    private ItemCollection itemCollection;

    public CollectionTestClient(Database db, String collectionName, int vectorSize) {
        this.db = db;
        this.collectionName = collectionName;
        this.vectorSize = vectorSize;
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
        itemCollection = new ItemCollection(collectionName, coll, vectorSize);
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
            System.out.printf(" created #%d: %s (in %s)\n",
                    i, item.idAsString(), _secs(System.currentTimeMillis() - start));
            // TODO: validate
        }

        System.err.println("Not implemented yet!");
    }

    private static String _secs(long msecs) {
        return "%.2f sec".formatted(msecs / 1000.0);
    }
}
