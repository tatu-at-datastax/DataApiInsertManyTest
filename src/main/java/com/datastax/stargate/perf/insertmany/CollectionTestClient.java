package com.datastax.stargate.perf.insertmany;

import java.time.Duration;
import java.time.Instant;

import com.datastax.astra.client.Collection;
import com.datastax.astra.client.Database;
import com.datastax.astra.client.model.DeleteResult;
import com.datastax.astra.client.model.Document;
import com.datastax.astra.client.model.SimilarityMetric;

/**
 * Wrapper around access to test Collections for Data API
 */
public class CollectionTestClient
{
    private final Database db;
    private final String collectionName;
    private final int vectorSize;

    public CollectionTestClient(Database db, String collectionName, int vectorSize) {
        this.db = db;
        this.collectionName = collectionName;
        this.vectorSize = vectorSize;
    }

    /**
     * Method that will (re)create Collection as necessary
     */
    public Collection<Document> initialize(boolean skipCollectionRecreate) throws Exception
    {
        System.out.printf("Initialize test client (collection '%s'):\n", collectionName);

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
            long elapsedMsecs = System.currentTimeMillis() - start;
            System.out.printf("created (in %.2f sec)); options = %s\n",
                    elapsedMsecs / 1000.0,
                    coll.getDefinition().getOptions());

            // And let's verify Collection does exist; do by checking it's empty
            long count = coll.countDocuments(10);
            if (count > 0) {
                throw new IllegalStateException("Collection '" + count + "' not empty; has " + count + " documents");
            }
        }
        return coll;
    }
}
