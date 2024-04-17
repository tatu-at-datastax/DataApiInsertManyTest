package com.datastax.stargate.perf.insertmany.agent;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricsCollector {
    private final long startTime;

    private final int batchSize;

    private final AtomicInteger okCalls = new AtomicInteger();
    private final AtomicInteger errorCalls = new AtomicInteger();

    private final Timer okCallTimer;

    private MetricsCollector(SimpleMeterRegistry registry, int batchSize) {
        this.batchSize = batchSize;
        okCallTimer = Timer.builder("okCallTimer")
                .publishPercentiles(0.5, 0.95)
                .register(registry);
        startTime = System.currentTimeMillis();
    }

    public static MetricsCollector create(int batchSize) {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        return new MetricsCollector(registry, batchSize);
    }

    public int okCalls() {
        return okCalls.get();
    }
    public int errorCalls() {
        return errorCalls.get();
    }

    public int totalCalls() {
        return okCalls() + errorCalls();
    }

    public String callCountsDesc() {
        HistogramSnapshot okSnapshot = okCallTimer.takeSnapshot();
        ValueAtPercentile[] pvalues = okSnapshot.percentileValues();
        pvalues[0].toString();
        return String.format("[Counts OK: %d (p50/p95: %.1f/%.1f ms) Error: %d]",
                okCalls(),
                pvalues[0].value(TimeUnit.MILLISECONDS), pvalues[1].value(TimeUnit.MILLISECONDS),
                errorCalls());
    }

    public String rateDesc() {
        double callRate = totalCalls() * 1000.0 / (System.currentTimeMillis() - startTime);
        return String.format("[Rate: %.1f calls (%.1f docs)/sec]",
                callRate, callRate * batchSize);
    }

    public String allStatsDesc() {
        return callCountsDesc() + rateDesc();
    }

    public void reportOkCall(InsertManyAgent agent, long timeMsecs) {
        okCalls.incrementAndGet();
        okCallTimer.record(timeMsecs, TimeUnit.MILLISECONDS);
    }

    public void reportErrorCall(InsertManyAgent agent, long timeMsecs) {
        errorCalls.incrementAndGet();
    }
}
