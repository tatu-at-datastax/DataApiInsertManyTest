package com.datastax.stargate.perf.insertmany.agent;

import com.datastax.stargate.perf.insertmany.entity.ItemCollection;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Object that represents a single concurrent agent that will insert data into the database;
 * instances matching active threads.
 */
public class InsertManyAgent
    implements Runnable
{
    public final int id;

    private final AtomicReference<AgentState> config;
    private final ItemCollection items;

    public InsertManyAgent(int id, AtomicReference<AgentState> config, ItemCollection items) {
        this.id = id;
        this.config = config;
        this.items = items;
    }

    @Override
    public void run() {
        AgentState state = config.get();
        state.agentStarted(this);

        main_loop:
        while (state != null) {
            // First: check throttling of the current phase
            if (!state.rateLimiter().tryConsume(1)) {
                // Throttled, wait a bit, retry
                waitMsecs(10L);
                continue;
            }
            // But then check if we have anything left to send
            if (state.leftToSend().getAndDecrement() <= 0) {
                // if not, need to wait for new phase
                state.agentFinished(this);
                AgentState nextState;

                while ((nextState = config.get()) == state) {
                    waitMsecs(10L);
                }
                state = nextState;
                state.agentStarted(this);
                continue;
            }

            // TODO: Perform the insert
            System.out.print(" [" + id + "]");
            waitMsecs(50L);
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
