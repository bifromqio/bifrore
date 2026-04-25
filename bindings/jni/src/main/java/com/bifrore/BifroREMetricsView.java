package com.bifrore;

import java.util.concurrent.TimeUnit;

final class BifroREMetricsView {
    private final BifroRE engine;
    private final long cacheNanos;
    private volatile ViewSnapshot snapshot;
    private volatile long lastRefreshNanos;

    BifroREMetricsView(BifroRE engine) {
        this(engine, TimeUnit.SECONDS.toNanos(1));
    }

    BifroREMetricsView(BifroRE engine, long cacheNanos) {
        this.engine = engine;
        this.cacheNanos = Math.max(0L, cacheNanos);
        this.snapshot = ViewSnapshot.empty();
        this.lastRefreshNanos = 0L;
    }

    double evalCount() {
        return current().evalCount;
    }

    double evalErrorCount() {
        return current().evalErrorCount;
    }

    double evalTypeErrorCount() {
        return current().evalTypeErrorCount;
    }

    double payloadSchemaErrorCount() {
        return current().payloadSchemaErrorCount;
    }

    double payloadDecodeErrorCount() {
        return current().payloadDecodeErrorCount;
    }

    double payloadBuildErrorCount() {
        return current().payloadBuildErrorCount;
    }

    double messagePipelineTotalNanos() {
        return current().messagePipelineTotalNanos;
    }

    double messagePipelineMaxNanos() {
        return current().messagePipelineMaxNanos;
    }

    double execTotalNanos() {
        return current().execTotalNanos;
    }

    double execMaxNanos() {
        return current().execMaxNanos;
    }

    double topicMatchCount() {
        return current().topicMatchCount;
    }

    double topicMatchTotalNanos() {
        return current().topicMatchTotalNanos;
    }

    double topicMatchMaxNanos() {
        return current().topicMatchMaxNanos;
    }

    double payloadDecodeCount() {
        return current().payloadDecodeCount;
    }

    double payloadDecodeTotalNanos() {
        return current().payloadDecodeTotalNanos;
    }

    double payloadDecodeMaxNanos() {
        return current().payloadDecodeMaxNanos;
    }

    double msgIrBuildCount() {
        return current().msgIrBuildCount;
    }

    double msgIrBuildTotalNanos() {
        return current().msgIrBuildTotalNanos;
    }

    double msgIrBuildMaxNanos() {
        return current().msgIrBuildMaxNanos;
    }

    double predicateCount() {
        return current().predicateCount;
    }

    double predicateTotalNanos() {
        return current().predicateTotalNanos;
    }

    double predicateMaxNanos() {
        return current().predicateMaxNanos;
    }

    double projectionCount() {
        return current().projectionCount;
    }

    double projectionTotalNanos() {
        return current().projectionTotalNanos;
    }

    double projectionMaxNanos() {
        return current().projectionMaxNanos;
    }

    double ingressMessageCount() {
        return current().ingressMessageCount;
    }

    double coreQueueDepth() {
        return current().coreQueueDepth;
    }

    double coreQueueDepthMax() {
        return current().coreQueueDepthMax;
    }

    double coreQueueDropCount() {
        return current().coreQueueDropCount;
    }

    double ffiQueueDepth() {
        return current().ffiQueueDepth;
    }

    double ffiQueueDepthMax() {
        return current().ffiQueueDepthMax;
    }

    double ffiQueueDropCount() {
        return current().ffiQueueDropCount;
    }

    double callbackDroppedCount() {
        return current().callbackDroppedCount;
    }

    double callbackPendingCount() {
        return current().callbackPendingCount;
    }

    double callbackQueueDepth() {
        return current().callbackQueueDepth;
    }

    double heapPollErrorCount() {
        return current().heapPollErrorCount;
    }

    double heapPollInvalidArgumentCount() {
        return current().heapPollInvalidArgumentCount;
    }

    double heapPollInvalidStateCount() {
        return current().heapPollInvalidStateCount;
    }

    double heapPollInternalQueueErrorCount() {
        return current().heapPollInternalQueueErrorCount;
    }

    double heapPollUnknownErrorCount() {
        return current().heapPollUnknownErrorCount;
    }

    double heapPollMessageCount() {
        return current().heapPollMessageCount;
    }

    double heapPollPayloadBytes() {
        return current().heapPollPayloadBytes;
    }

    double heapPollNoDataCount() {
        return current().heapPollNoDataCount;
    }

    double shutdownDroppedCount() {
        return current().shutdownDroppedCount;
    }

    double pollerTimeoutPendingCount() {
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
        final long evalTypeErrorCount;
        final long payloadSchemaErrorCount;
        final long payloadDecodeErrorCount;
        final long payloadBuildErrorCount;
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
        final long heapPollInternalQueueErrorCount;
        final long heapPollUnknownErrorCount;
        final long heapPollMessageCount;
        final long heapPollPayloadBytes;
        final long heapPollNoDataCount;
        final long shutdownDroppedCount;
        final long pollerTimeoutPendingCount;

        private ViewSnapshot(
            long evalCount,
            long evalErrorCount,
            long evalTypeErrorCount,
            long payloadSchemaErrorCount,
            long payloadDecodeErrorCount,
            long payloadBuildErrorCount,
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
            long heapPollInternalQueueErrorCount,
            long heapPollUnknownErrorCount,
            long heapPollMessageCount,
            long heapPollPayloadBytes,
            long heapPollNoDataCount,
            long shutdownDroppedCount,
            long pollerTimeoutPendingCount
        ) {
            this.evalCount = evalCount;
            this.evalErrorCount = evalErrorCount;
            this.evalTypeErrorCount = evalTypeErrorCount;
            this.payloadSchemaErrorCount = payloadSchemaErrorCount;
            this.payloadDecodeErrorCount = payloadDecodeErrorCount;
            this.payloadBuildErrorCount = payloadBuildErrorCount;
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
            this.heapPollInternalQueueErrorCount = heapPollInternalQueueErrorCount;
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
                0, // evalTypeErrorCount
                0, // payloadSchemaErrorCount
                0, // payloadDecodeErrorCount
                0, // payloadBuildErrorCount
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
                0, // heapPollInternalQueueErrorCount
                0, // heapPollUnknownErrorCount
                0, // heapPollMessageCount
                0, // heapPollPayloadBytes
                0, // heapPollNoDataCount
                0, // shutdownDroppedCount
                0  // pollerTimeoutPendingCount
            );
        }

        static ViewSnapshot from(BifroRE instance, BifroRE.MetricsSnapshot metrics) {
            BifroRE.MetricsSnapshot snapshot = metrics != null ? metrics : BifroRE.MetricsSnapshot.empty();
            return new ViewSnapshot(
                snapshot.evalCount,
                snapshot.evalErrorCount,
                snapshot.evalTypeErrorCount,
                snapshot.payloadSchemaErrorCount,
                snapshot.payloadDecodeErrorCount,
                snapshot.payloadBuildErrorCount,
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
                instance.callbackDroppedCount(),
                instance.callbackPendingCount(),
                instance.callbackQueueDepth(),
                instance.heapPollErrorCount(),
                instance.heapPollInvalidArgumentCount(),
                instance.heapPollInvalidStateCount(),
                instance.heapPollInternalQueueErrorCount(),
                instance.heapPollUnknownErrorCount(),
                instance.heapPollMessageCount(),
                instance.heapPollPayloadBytes(),
                instance.heapPollNoDataCount(),
                instance.shutdownDroppedCount(),
                instance.pollerTimeoutPendingCount()
            );
        }
    }
}
