package com.datastax.stargate.perf.insertmany;

import com.datastax.stargate.perf.insertmany.agent.InsertManyAgent;
import com.datastax.stargate.perf.insertmany.agent.MetricsCollector;
import com.datastax.stargate.perf.insertmany.entity.CollectionItemGenerator;
import com.datastax.stargate.perf.insertmany.entity.ItemCollection;
import io.github.bucket4j.Bucket;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Entity that handles running of one test phases (warm up, test).
 */
public class TestPhaseRunner {
   private final int agentCount;
   private final ItemCollection items;
   private final CollectionItemGenerator itemGenerator;
   private final int batchSize;

   public TestPhaseRunner(int agentCount,
                          ItemCollection items, CollectionItemGenerator itemGenerator,
                          int batchSize) {
        this.agentCount = agentCount;
        this.items = items;
        this.itemGenerator = itemGenerator;
        this.batchSize = batchSize;
   }

   public MetricsCollector runPhase(final String phaseName,
                                    long duration, TimeUnit durationUnit,
                                    int maxRPS) throws InterruptedException
   {
       System.out.printf("runPhase('%s') for %d %s; maxRPS: %d\n",
               phaseName, duration, durationUnit, maxRPS);
       System.out.printf(" first, truncate collection: ");
       items.deleteAll();
       System.out.printf("ok.\n");
       Thread.sleep(1000L);

       final long phaseStartMsecs = System.currentTimeMillis();

       final InsertManyAgent[] agents = new InsertManyAgent[agentCount];
       for (int i = 0; i < agentCount; ++i) {
           agents[i] = new InsertManyAgent(i, items, itemGenerator, batchSize);
       }
       final ExecutorService exec = Executors.newFixedThreadPool(agentCount);
       final MetricsCollector metrics = MetricsCollector.create();
       final Bucket rateLimiter = Bucket.builder()
               // Allow +10% burst beyond MaxRPS
               .addLimit(limit -> limit.capacity(maxRPS + (int) (maxRPS * 0.1))
                       .refillGreedy(maxRPS, Duration.ofSeconds(1)))
               .build();
       // To start need all agents to be ready and parent thread to ack:
       final CountDownLatch startLatch = new CountDownLatch(agentCount + 1);
       // to end just all agents to be done:
       final CountDownLatch endLatch = new CountDownLatch(agentCount);

       final long endTime = System.currentTimeMillis() + durationUnit.toMillis(duration);

       for (InsertManyAgent agent : agents) {
           exec.execute(new Runnable() {
               @Override
               public void run() {
                   startLatch.countDown();
                   try {
                       startLatch.await();
                   } catch (InterruptedException e) {
                       System.err.printf("ERROR: failed to start %s: (%s) %s\n",
                               agent, e.getClass().getName(), e.getMessage());
                       endLatch.countDown();
                       return;
                   }

                   try {
                       agent.runPhase(phaseName, endTime, rateLimiter, metrics);
                   } catch (Exception e) {
                       System.err.printf("ERROR: failed runPhase on %s: (%s) %s\n",
                               agent, e.getClass().getName(), e.getMessage());
                   } finally {
                       endLatch.countDown();
                   }
               }
           });
       }

       // And then loop a bit, waiting for the end: delay between prints at least 1 second,
       // at most 10 seconds; aiming at 30 updates total.
       final long waitBetweenOutputSecs = Math.max(1L, Math.min(10L,
               durationUnit.toSeconds(duration) / 30));
       System.out.printf("  (output state every %d seconds)\n", waitBetweenOutputSecs);
       final long waitBetweenOutputMsecs = waitBetweenOutputSecs * 1000L;

       // Ok, start all agents
       startLatch.countDown();
       try {
           startLatch.await(3L, TimeUnit.SECONDS);
       } catch (InterruptedException e) {
           throw new IllegalStateException("Failed to start agents for phase '"+phaseName+"'");
       }

       long currTime;
       while ((endLatch.getCount() > 0) && (currTime = System.currentTimeMillis()) < endTime) {
           final long waitMsecs = Math.min(endTime - currTime, waitBetweenOutputMsecs);
           Thread.sleep(waitMsecs);

           System.out.printf(" %s: %.2f secs -> %s%s\n", phaseName,
                   (currTime - phaseStartMsecs) / 1000.0,
                   metrics.callCountsDesc(),
                   metrics.rateDesc());
       }

       try {
          final long waitMsecs = endTime - System.currentTimeMillis() + 1000L;
          endLatch.await(waitMsecs + 10L, TimeUnit.MILLISECONDS);
       } catch (InterruptedException e) {
          System.err.printf("ERROR: failed to wait for end of phase '%s': (%s) %s",
                 phaseName, e.getClass().getName(), e.getMessage());
       }

       final long phaseMsecs = System.currentTimeMillis() - phaseStartMsecs;
       System.out.printf("\nCompleted phase ('%s') in %.2f seconds: %s\n",
               phaseName, (phaseMsecs / 1000.0), metrics.callCountsDesc());

       return metrics;
   }
}
