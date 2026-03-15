package com.bifrore;

import java.util.concurrent.TimeUnit;

public final class BifroREMetricsView {
    private final BifroRE engine;
    private final long cacheNanos;
    private volatile BifroRE.MetricsSnapshot snapshot;
    private volatile long lastRefreshNanos;

    public BifroREMetricsView(BifroRE engine) {
        this(engine, TimeUnit.SECONDS.toNanos(1));
    }

    public BifroREMetricsView(BifroRE engine, long cacheNanos) {
        this.engine = engine;
        this.cacheNanos = Math.max(0L, cacheNanos);
        this.snapshot = new BifroRE.MetricsSnapshot(0, 0, 0, 0);
        this.lastRefreshNanos = 0L;
    }

    public double evalCount() {
        return current().evalCount;
    }

    public double evalErrorCount() {
        return current().evalErrorCount;
    }

    public double evalTotalNanos() {
        return current().evalTotalNanos;
    }

    public double evalMaxNanos() {
        return current().evalMaxNanos;
    }

    public double evalAvgNanos() {
        BifroRE.MetricsSnapshot current = current();
        return current.evalCount == 0 ? 0.0 : (double) current.evalTotalNanos / current.evalCount;
    }

    private BifroRE.MetricsSnapshot current() {
        long now = System.nanoTime();
        BifroRE.MetricsSnapshot current = snapshot;
        if (now - lastRefreshNanos <= cacheNanos) {
            return current;
        }
        synchronized (this) {
            now = System.nanoTime();
            if (now - lastRefreshNanos <= cacheNanos) {
                return snapshot;
            }
            BifroRE.MetricsSnapshot refreshed = engine.metrics();
            snapshot = refreshed != null ? refreshed : new BifroRE.MetricsSnapshot(0, 0, 0, 0);
            lastRefreshNanos = now;
            return snapshot;
        }
    }
}
