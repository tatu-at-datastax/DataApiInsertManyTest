package com.datastax.stargate.perf.insertmany.agent;

import com.datastax.stargate.perf.insertmany.entity.CollectionItemGenerator;
import com.datastax.stargate.perf.insertmany.entity.ItemCollection;
import io.github.bucket4j.Bucket;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Object that represents a single concurrent agent that will insert data into the database;
 * instances matching active threads.
 */
public class InsertManyAgent
{
    public final int id;

    private final ItemCollection items;

    private final CollectionItemGenerator itemGenerator;

    public InsertManyAgent(int id, ItemCollection items, CollectionItemGenerator itemGenerator) {
        this.id = id;
        this.items = items;
        this.itemGenerator = itemGenerator;
    }

    public void runPhase(final String phaseName, final long endTime,
                         Bucket rateLimiter, MetricsCollector metrics)
    {
        long currTime;

        while ((currTime = System.currentTimeMillis()) < endTime) {
            // First: check throttling of the current phase
            if (!rateLimiter.tryConsume(1)) {
                // Throttled, wait a bit, retry
                waitMsecs(10L);
                continue;
            }
            final long startTime = System.currentTimeMillis();

            // TODO: Perform the insert
            waitMsecs(50L);

            metrics.reportOkCall(this, System.currentTimeMillis() - startTime);
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
