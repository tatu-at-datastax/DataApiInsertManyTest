package com.datastax.stargate.perf.insertmany.agent;

import io.github.bucket4j.Bucket;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Container for configuration and state for {@link InsertManyAgent}
 */
public record AgentState(String phase,
                         Bucket rateLimiter, MetricsCollector metrics,
                         AtomicInteger leftToSend)
{
    public void agentStarted(InsertManyAgent agent) {
        System.out.printf("%s started '%s.\n", agent, phase);
    }

    public void agentFinished(InsertManyAgent agent) {
        System.out.printf("%s finished '%s.\n", agent, phase);
    }
}
