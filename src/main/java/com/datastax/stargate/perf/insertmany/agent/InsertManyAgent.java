package com.datastax.stargate.perf.insertmany.agent;

import com.datastax.astra.client.exception.DataApiException;
import com.datastax.stargate.perf.insertmany.entity.CollectionItem;
import com.datastax.stargate.perf.insertmany.entity.CollectionItemGenerator;
import com.datastax.stargate.perf.insertmany.entity.ItemCollection;
import io.github.bucket4j.Bucket;

import java.util.List;

/**
 * Object that represents a single concurrent agent that will insert data into the database;
 * instances matching active threads.
 */
public class InsertManyAgent
{
    public final int id;

    private final ItemCollection items;

    private final CollectionItemGenerator itemGenerator;

    private final int batchSize;

    public InsertManyAgent(int id, ItemCollection items, CollectionItemGenerator itemGenerator,
                           int batchSize) {
        this.id = id;
        this.items = items;
        this.itemGenerator = itemGenerator;
        this.batchSize = batchSize;
    }

    public void runPhase(final String phaseName, final long endTime,
                         Bucket rateLimiter, MetricsCollector metrics)
    {
        while ((System.currentTimeMillis()) < endTime) {
            // First: check throttling of the current phase
            if (!rateLimiter.tryConsume(1)) {
                // Throttled, wait a bit, retry
                waitMsecs(10L);
                continue;
            }
            List<CollectionItem> batch = itemGenerator.generate(batchSize);
            final long startTime = System.currentTimeMillis();
            Boolean ok = null;
            try {
                ok = items.insertItems(batch);
                metrics.reportOkCall(this, System.currentTimeMillis() - startTime);
            } catch (DataApiException ex) {
                System.err.printf("WARN: exception for %s: (%s) %s\n",
                        this, ex.getErrorCode(), ex.getMessage());
                metrics.reportErrorCall(this, System.currentTimeMillis() - startTime);
            }
        }
    }

    private void waitMsecs(long msecs) {
        try {
            Thread.sleep(msecs);
        } catch (InterruptedException e) {
        }
    }

    @Override
    public String toString() {
        return "[Agent #"+id+"]";
    }
}
