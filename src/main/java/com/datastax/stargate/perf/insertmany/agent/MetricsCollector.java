package com.datastax.stargate.perf.insertmany.agent;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricsCollector {
    private final AtomicInteger okCalls = new AtomicInteger();
    private final AtomicInteger failCalls = new AtomicInteger();
    private final AtomicInteger errorCalls = new AtomicInteger();

    private final Timer okCallTimer;

    private MetricsCollector(SimpleMeterRegistry registry) {
        okCallTimer = Timer.builder("okCallTimer")
                .publishPercentiles(0.5, 0.95)
                .register(registry);
    }

    public static MetricsCollector create() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        return new MetricsCollector(registry);
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
        HistogramSnapshot okSnapshot = okCallTimer.takeSnapshot();
        ValueAtPercentile[] pvalues = okSnapshot.percentileValues();
        pvalues[0].toString();
        return String.format("[OK: %d (p50/p95: %.1f/%.1f ms), Fail: %d, Error: %d]",
//        return String.format("[OK: %d (%s), Fail: %d, Error: %d]",
                okCalls(),
                pvalues[0].value(TimeUnit.MILLISECONDS), pvalues[1].value(TimeUnit.MILLISECONDS),
                failCalls(), errorCalls());
    }

    public void reportOkCall(InsertManyAgent agent, long timeMsecs) {
        okCalls.incrementAndGet();
        okCallTimer.record(timeMsecs, TimeUnit.MILLISECONDS);
    }

    public void reportFailCall(InsertManyAgent agent, long timeMsecs) {
        failCalls.incrementAndGet();
    }

    public void reportErrorCall(InsertManyAgent agent, long timeMsecs) {
        errorCalls.incrementAndGet();
    }
}
