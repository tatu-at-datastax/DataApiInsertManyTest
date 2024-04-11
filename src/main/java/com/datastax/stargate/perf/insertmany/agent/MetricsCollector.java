package com.datastax.stargate.perf.insertmany.agent;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public record MetricsCollector() {
    public static MetricsCollector create(SimpleMeterRegistry metrics) {
        return new MetricsCollector();
    }
}
