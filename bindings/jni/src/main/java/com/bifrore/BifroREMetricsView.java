package com.bifrore;

import java.util.concurrent.TimeUnit;

public final class BifroREMetricsView {
    private final BifroRE engine;
    private final long cacheNanos;
    private volatile ViewSnapshot snapshot;
    private volatile long lastRefreshNanos;

    public BifroREMetricsView(BifroRE engine) {
        this(engine, TimeUnit.SECONDS.toNanos(1));
    }

    public BifroREMetricsView(BifroRE engine, long cacheNanos) {
        this.engine = engine;
        this.cacheNanos = Math.max(0L, cacheNanos);
        this.snapshot = ViewSnapshot.empty();
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
        ViewSnapshot current = current();
        return current.evalCount == 0 ? 0.0 : (double) current.evalTotalNanos / current.evalCount;
    }

    public double ingressMessageCount() {
        return current().ingressMessageCount;
    }

    public double coreQueueDepth() {
        return current().coreQueueDepth;
    }

    public double coreQueueDepthMax() {
        return current().coreQueueDepthMax;
    }

    public double coreQueueDropCount() {
        return current().coreQueueDropCount;
    }

    public double ffiQueueDepth() {
        return current().ffiQueueDepth;
    }

    public double ffiQueueDepthMax() {
        return current().ffiQueueDepthMax;
    }

    public double ffiQueueDropCount() {
        return current().ffiQueueDropCount;
    }

    public double callbackDroppedCount() {
        return current().callbackDroppedCount;
    }

    public double callbackPendingCount() {
        return current().callbackPendingCount;
    }

    public double callbackQueueDepth() {
        return current().callbackQueueDepth;
    }

    public double shutdownDroppedCount() {
        return current().shutdownDroppedCount;
    }

    public double pollerTimeoutPendingCount() {
        return current().pollerTimeoutPendingCount;
    }

    private ViewSnapshot current() {
        long now = System.nanoTime();
        ViewSnapshot current = snapshot;
        if (now - lastRefreshNanos <= cacheNanos) {
            return current;
        }
        synchronized (this) {
            now = System.nanoTime();
            if (now - lastRefreshNanos <= cacheNanos) {
                return snapshot;
            }
            BifroRE.MetricsSnapshot refreshed = engine.metrics();
            snapshot = ViewSnapshot.from(engine, refreshed);
            lastRefreshNanos = now;
            return snapshot;
        }
    }

    private static final class ViewSnapshot {
        final long evalCount;
        final long evalErrorCount;
        final long evalTotalNanos;
        final long evalMaxNanos;
        final long ingressMessageCount;
        final long coreQueueDepth;
        final long coreQueueDepthMax;
        final long coreQueueDropCount;
        final long ffiQueueDepth;
        final long ffiQueueDepthMax;
        final long ffiQueueDropCount;
        final long callbackDroppedCount;
        final long callbackPendingCount;
        final long callbackQueueDepth;
        final long shutdownDroppedCount;
        final long pollerTimeoutPendingCount;

        private ViewSnapshot(
            long evalCount,
            long evalErrorCount,
            long evalTotalNanos,
            long evalMaxNanos,
            long ingressMessageCount,
            long coreQueueDepth,
            long coreQueueDepthMax,
            long coreQueueDropCount,
            long ffiQueueDepth,
            long ffiQueueDepthMax,
            long ffiQueueDropCount,
            long callbackDroppedCount,
            long callbackPendingCount,
            long callbackQueueDepth,
            long shutdownDroppedCount,
            long pollerTimeoutPendingCount
        ) {
            this.evalCount = evalCount;
            this.evalErrorCount = evalErrorCount;
            this.evalTotalNanos = evalTotalNanos;
            this.evalMaxNanos = evalMaxNanos;
            this.ingressMessageCount = ingressMessageCount;
            this.coreQueueDepth = coreQueueDepth;
            this.coreQueueDepthMax = coreQueueDepthMax;
            this.coreQueueDropCount = coreQueueDropCount;
            this.ffiQueueDepth = ffiQueueDepth;
            this.ffiQueueDepthMax = ffiQueueDepthMax;
            this.ffiQueueDropCount = ffiQueueDropCount;
            this.callbackDroppedCount = callbackDroppedCount;
            this.callbackPendingCount = callbackPendingCount;
            this.callbackQueueDepth = callbackQueueDepth;
            this.shutdownDroppedCount = shutdownDroppedCount;
            this.pollerTimeoutPendingCount = pollerTimeoutPendingCount;
        }

        static ViewSnapshot empty() {
            return new ViewSnapshot(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        static ViewSnapshot from(BifroRE engine, BifroRE.MetricsSnapshot metrics) {
            BifroRE.MetricsSnapshot snapshot = metrics != null ? metrics : BifroRE.MetricsSnapshot.empty();
            return new ViewSnapshot(
                snapshot.evalCount,
                snapshot.evalErrorCount,
                snapshot.evalTotal.totalNanos,
                snapshot.evalTotal.maxNanos,
                snapshot.ingressMessageCount,
                snapshot.coreQueueDepth,
                snapshot.coreQueueDepthMax,
                snapshot.coreQueueDropCount,
                snapshot.ffiQueueDepth,
                snapshot.ffiQueueDepthMax,
                snapshot.ffiQueueDropCount,
                engine.callbackDroppedCount(),
                engine.callbackPendingCount(),
                engine.callbackQueueDepth(),
                engine.shutdownDroppedCount(),
                engine.pollerTimeoutPendingCount()
            );
        }
    }
}
