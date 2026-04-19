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

    public double messagePipelineTotalNanos() {
        return current().messagePipelineTotalNanos;
    }

    public double messagePipelineMaxNanos() {
        return current().messagePipelineMaxNanos;
    }

    public double execTotalNanos() {
        return current().execTotalNanos;
    }

    public double execMaxNanos() {
        return current().execMaxNanos;
    }

    public double topicMatchCount() {
        return current().topicMatchCount;
    }

    public double topicMatchTotalNanos() {
        return current().topicMatchTotalNanos;
    }

    public double topicMatchMaxNanos() {
        return current().topicMatchMaxNanos;
    }

    public double payloadDecodeCount() {
        return current().payloadDecodeCount;
    }

    public double payloadDecodeTotalNanos() {
        return current().payloadDecodeTotalNanos;
    }

    public double payloadDecodeMaxNanos() {
        return current().payloadDecodeMaxNanos;
    }

    public double msgIrBuildCount() {
        return current().msgIrBuildCount;
    }

    public double msgIrBuildTotalNanos() {
        return current().msgIrBuildTotalNanos;
    }

    public double msgIrBuildMaxNanos() {
        return current().msgIrBuildMaxNanos;
    }

    public double predicateCount() {
        return current().predicateCount;
    }

    public double predicateTotalNanos() {
        return current().predicateTotalNanos;
    }

    public double predicateMaxNanos() {
        return current().predicateMaxNanos;
    }

    public double projectionCount() {
        return current().projectionCount;
    }

    public double projectionTotalNanos() {
        return current().projectionTotalNanos;
    }

    public double projectionMaxNanos() {
        return current().projectionMaxNanos;
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

    public double heapPollErrorCount() {
        return current().heapPollErrorCount;
    }

    public double heapPollInvalidArgumentCount() {
        return current().heapPollInvalidArgumentCount;
    }

    public double heapPollInvalidStateCount() {
        return current().heapPollInvalidStateCount;
    }

    public double heapPollOperationFailedCount() {
        return current().heapPollOperationFailedCount;
    }

    public double heapPollUnknownErrorCount() {
        return current().heapPollUnknownErrorCount;
    }

    public double heapPollMessageCount() {
        return current().heapPollMessageCount;
    }

    public double heapPollPayloadBytes() {
        return current().heapPollPayloadBytes;
    }

    public double heapPollNoDataCount() {
        return current().heapPollNoDataCount;
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
        final long messagePipelineTotalNanos;
        final long messagePipelineMaxNanos;
        final long execTotalNanos;
        final long execMaxNanos;
        final long topicMatchCount;
        final long topicMatchTotalNanos;
        final long topicMatchMaxNanos;
        final long payloadDecodeCount;
        final long payloadDecodeTotalNanos;
        final long payloadDecodeMaxNanos;
        final long msgIrBuildCount;
        final long msgIrBuildTotalNanos;
        final long msgIrBuildMaxNanos;
        final long predicateCount;
        final long predicateTotalNanos;
        final long predicateMaxNanos;
        final long projectionCount;
        final long projectionTotalNanos;
        final long projectionMaxNanos;
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
        final long heapPollErrorCount;
        final long heapPollInvalidArgumentCount;
        final long heapPollInvalidStateCount;
        final long heapPollOperationFailedCount;
        final long heapPollUnknownErrorCount;
        final long heapPollMessageCount;
        final long heapPollPayloadBytes;
        final long heapPollNoDataCount;
        final long shutdownDroppedCount;
        final long pollerTimeoutPendingCount;

        private ViewSnapshot(
            long evalCount,
            long evalErrorCount,
            long messagePipelineTotalNanos,
            long messagePipelineMaxNanos,
            long execTotalNanos,
            long execMaxNanos,
            long topicMatchCount,
            long topicMatchTotalNanos,
            long topicMatchMaxNanos,
            long payloadDecodeCount,
            long payloadDecodeTotalNanos,
            long payloadDecodeMaxNanos,
            long msgIrBuildCount,
            long msgIrBuildTotalNanos,
            long msgIrBuildMaxNanos,
            long predicateCount,
            long predicateTotalNanos,
            long predicateMaxNanos,
            long projectionCount,
            long projectionTotalNanos,
            long projectionMaxNanos,
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
            long heapPollErrorCount,
            long heapPollInvalidArgumentCount,
            long heapPollInvalidStateCount,
            long heapPollOperationFailedCount,
            long heapPollUnknownErrorCount,
            long heapPollMessageCount,
            long heapPollPayloadBytes,
            long heapPollNoDataCount,
            long shutdownDroppedCount,
            long pollerTimeoutPendingCount
        ) {
            this.evalCount = evalCount;
            this.evalErrorCount = evalErrorCount;
            this.messagePipelineTotalNanos = messagePipelineTotalNanos;
            this.messagePipelineMaxNanos = messagePipelineMaxNanos;
            this.execTotalNanos = execTotalNanos;
            this.execMaxNanos = execMaxNanos;
            this.topicMatchCount = topicMatchCount;
            this.topicMatchTotalNanos = topicMatchTotalNanos;
            this.topicMatchMaxNanos = topicMatchMaxNanos;
            this.payloadDecodeCount = payloadDecodeCount;
            this.payloadDecodeTotalNanos = payloadDecodeTotalNanos;
            this.payloadDecodeMaxNanos = payloadDecodeMaxNanos;
            this.msgIrBuildCount = msgIrBuildCount;
            this.msgIrBuildTotalNanos = msgIrBuildTotalNanos;
            this.msgIrBuildMaxNanos = msgIrBuildMaxNanos;
            this.predicateCount = predicateCount;
            this.predicateTotalNanos = predicateTotalNanos;
            this.predicateMaxNanos = predicateMaxNanos;
            this.projectionCount = projectionCount;
            this.projectionTotalNanos = projectionTotalNanos;
            this.projectionMaxNanos = projectionMaxNanos;
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
            this.heapPollErrorCount = heapPollErrorCount;
            this.heapPollInvalidArgumentCount = heapPollInvalidArgumentCount;
            this.heapPollInvalidStateCount = heapPollInvalidStateCount;
            this.heapPollOperationFailedCount = heapPollOperationFailedCount;
            this.heapPollUnknownErrorCount = heapPollUnknownErrorCount;
            this.heapPollMessageCount = heapPollMessageCount;
            this.heapPollPayloadBytes = heapPollPayloadBytes;
            this.heapPollNoDataCount = heapPollNoDataCount;
            this.shutdownDroppedCount = shutdownDroppedCount;
            this.pollerTimeoutPendingCount = pollerTimeoutPendingCount;
        }

        static ViewSnapshot empty() {
            return new ViewSnapshot(
                0, // evalCount
                0, // evalErrorCount
                0, // messagePipelineTotalNanos
                0, // messagePipelineMaxNanos
                0, // execTotalNanos
                0, // execMaxNanos
                0, // topicMatchCount
                0, // topicMatchTotalNanos
                0, // topicMatchMaxNanos
                0, // payloadDecodeCount
                0, // payloadDecodeTotalNanos
                0, // payloadDecodeMaxNanos
                0, // msgIrBuildCount
                0, // msgIrBuildTotalNanos
                0, // msgIrBuildMaxNanos
                0, // predicateCount
                0, // predicateTotalNanos
                0, // predicateMaxNanos
                0, // projectionCount
                0, // projectionTotalNanos
                0, // projectionMaxNanos
                0, // ingressMessageCount
                0, // coreQueueDepth
                0, // coreQueueDepthMax
                0, // coreQueueDropCount
                0, // ffiQueueDepth
                0, // ffiQueueDepthMax
                0, // ffiQueueDropCount
                0, // callbackDroppedCount
                0, // callbackPendingCount
                0, // callbackQueueDepth
                0, // heapPollErrorCount
                0, // heapPollInvalidArgumentCount
                0, // heapPollInvalidStateCount
                0, // heapPollOperationFailedCount
                0, // heapPollUnknownErrorCount
                0, // heapPollMessageCount
                0, // heapPollPayloadBytes
                0, // heapPollNoDataCount
                0, // shutdownDroppedCount
                0  // pollerTimeoutPendingCount
            );
        }

        static ViewSnapshot from(BifroRE engine, BifroRE.MetricsSnapshot metrics) {
            BifroRE.MetricsSnapshot snapshot = metrics != null ? metrics : BifroRE.MetricsSnapshot.empty();
            return new ViewSnapshot(
                snapshot.evalCount,
                snapshot.evalErrorCount,
                snapshot.messagePipeline.totalNanos,
                snapshot.messagePipeline.maxNanos,
                snapshot.exec.totalNanos,
                snapshot.exec.maxNanos,
                snapshot.topicMatch.count,
                snapshot.topicMatch.totalNanos,
                snapshot.topicMatch.maxNanos,
                snapshot.payloadDecode.count,
                snapshot.payloadDecode.totalNanos,
                snapshot.payloadDecode.maxNanos,
                snapshot.msgIrBuild.count,
                snapshot.msgIrBuild.totalNanos,
                snapshot.msgIrBuild.maxNanos,
                snapshot.predicate.count,
                snapshot.predicate.totalNanos,
                snapshot.predicate.maxNanos,
                snapshot.projection.count,
                snapshot.projection.totalNanos,
                snapshot.projection.maxNanos,
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
                engine.heapPollErrorCount(),
                engine.heapPollInvalidArgumentCount(),
                engine.heapPollInvalidStateCount(),
                engine.heapPollOperationFailedCount(),
                engine.heapPollUnknownErrorCount(),
                engine.heapPollMessageCount(),
                engine.heapPollPayloadBytes(),
                engine.heapPollNoDataCount(),
                engine.shutdownDroppedCount(),
                engine.pollerTimeoutPendingCount()
            );
        }
    }
}
