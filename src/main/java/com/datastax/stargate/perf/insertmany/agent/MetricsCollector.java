package com.datastax.stargate.perf.insertmany.agent;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.concurrent.atomic.AtomicInteger;

public class MetricsCollector {
    private final AtomicInteger okCalls = new AtomicInteger();
    private final AtomicInteger failCalls = new AtomicInteger();
    private final AtomicInteger errorCalls = new AtomicInteger();

    public static MetricsCollector create() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        return new MetricsCollector();
    }

    public int okCalls() {
        return okCalls.get();
    }
    public int failCalls() {
        return failCalls.get();
    }
    public int errorCalls() {
        return errorCalls.get();
    }

    public int totalCalls() {
        return okCalls() + failCalls() + errorCalls();
    }

    public String callCountsDesc() {
        return String.format("[OK: %d, Fail: %d, Error: %d]",
                okCalls(), failCalls(), errorCalls());
    }

    public void reportOkCall(InsertManyAgent agent, long timeMsecs) {
        okCalls.incrementAndGet();
    }

    public void reportFailCall(InsertManyAgent agent, long timeMsecs) {
        failCalls.incrementAndGet();
    }

    public void reportErrorCall(InsertManyAgent agent, long timeMsecs) {
        errorCalls.incrementAndGet();
    }
}
