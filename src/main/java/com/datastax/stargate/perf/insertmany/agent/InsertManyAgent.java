package com.datastax.stargate.perf.insertmany.agent;

import com.datastax.astra.client.exceptions.DataAPIException;
import com.datastax.stargate.perf.insertmany.entity.ContainerItem;
import com.datastax.stargate.perf.insertmany.entity.ContainerItemGenerator;
import com.datastax.stargate.perf.insertmany.entity.ItemContainer;
import io.github.bucket4j.Bucket;

import java.util.List;

/**
 * Object that represents a single concurrent agent that will insert data into the database;
 * instances matching active threads.
 */
public class InsertManyAgent
{
    public final int id;

    private final ItemContainer items;

    private final ContainerItemGenerator itemGenerator;

    private final int batchSize;

    public InsertManyAgent(int id, ItemContainer items, ContainerItemGenerator itemGenerator,
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
            List<ContainerItem> batch = itemGenerator.generate(batchSize);
            final long startTime = System.currentTimeMillis();
            try {
                boolean ok = items.insertItems(batch);
                metrics.reportOkCall(this, System.currentTimeMillis() - startTime);
                if (!ok) {
                    System.err.printf("WARN: insertItems returned `false` for %s\n", this);

                }
            } catch (DataAPIException ex) {
                metrics.reportErrorCall(this, System.currentTimeMillis() - startTime);
                System.err.printf("WARN: exception for %s: (%s) %s\n",
                        this, ex.getErrorCode(), ex.getMessage());
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
