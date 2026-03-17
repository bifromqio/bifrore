package com.bifrore;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Logger;

public final class BifroRE implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(BifroRE.class.getName());
    private static final long POLLER_JOIN_TIMEOUT_MILLIS = 5000L;
    private static final long EXECUTOR_SHUTDOWN_TIMEOUT_MILLIS = 5000L;
    private static final long DROP_WARN_EVERY = 100L;

    static {
        NativeLibraryLoader.load();
    }

    public interface LogHandler {
        void onLog(
            int level,
            String target,
            String message,
            long timestampMillis,
            String threadId,
            String modulePath,
            String file,
            int line
        );
    }

    public static final int PAYLOAD_JSON = 1;
    public static final int PAYLOAD_PROTOBUF = 2;

    public static final class EvalResult {
        public final int ruleIndex;
        public final byte[] payload;
        public final RuleMetadata metadata;

        EvalResult(int ruleIndex, byte[] payload, RuleMetadata metadata) {
            this.ruleIndex = ruleIndex;
            this.payload = payload;
            this.metadata = metadata;
        }
    }

    public static final class RuleMetadata {
        public final int ruleIndex;
        public final String destinationsJson;

        RuleMetadata(int ruleIndex, String destinationsJson) {
            this.ruleIndex = ruleIndex;
            this.destinationsJson = destinationsJson;
        }
    }

    public static final class MetricsSnapshot {
        public final long evalCount;
        public final long evalErrorCount;
        public final long evalTotalNanos;
        public final long evalMaxNanos;

        MetricsSnapshot(long evalCount, long evalErrorCount, long evalTotalNanos, long evalMaxNanos) {
            this.evalCount = evalCount;
            this.evalErrorCount = evalErrorCount;
            this.evalTotalNanos = evalTotalNanos;
            this.evalMaxNanos = evalMaxNanos;
        }
    }

    private long handle;
    private long logCallbackHandle;
    private final ThreadPoolExecutor defaultMessageExecutor;
    private final ExecutorService defaultLogExecutor;
    private final String host;
    private final int port;
    private final String nodeId;
    private final int clientCount;
    private final boolean multiNci;
    private final String clientIdsPath;
    private final int callbackQueueCapacity;
    private final RuleMetadata[] ruleMetadataTable;
    private final AtomicLong callbackSubmittedCount;
    private final AtomicLong callbackCompletedCount;
    private final AtomicLong callbackDroppedCount;
    private final AtomicLong pollerTimeoutPendingCount;
    private final AtomicLong shutdownDroppedCount;
    private volatile boolean mqttStarted;
    private volatile boolean pollRunning;
    private volatile boolean disconnecting;
    private volatile Thread pollThread;
    private volatile Consumer<EvalResult> nextHandler;
    private volatile Executor nextExecutor;

    public BifroRE(BifroREOptions options) {
        this(
            options.host,
            options.port,
            Objects.requireNonNull(options.ruleJsonPath, "ruleJsonPath"),
            options.nodeId,
            options.clientCount,
            options.multiNci,
            options.payloadFormat,
            options.clientIdsPath,
            options.callbackQueueCapacity,
            options.pollBatchLimit
        );
    }

    private BifroRE(
        String host,
        int port,
        String ruleJsonPath,
        String nodeId,
        int clientCount,
        boolean multiNci,
        int payloadFormat,
        String clientIdsPath,
        int callbackQueueCapacity,
        int pollBatchLimit
    ) {
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.nodeId = nodeId;
        this.clientCount = clientCount;
        this.multiNci = multiNci;
        this.callbackQueueCapacity = Math.max(1, callbackQueueCapacity);
        this.clientIdsPath =
            (clientIdsPath == null || clientIdsPath.isBlank()) ? "./client_ids" : clientIdsPath;
        this.handle = nativeCreateWithConfigAndPayloadFormatAndClientIdsPath(
            ruleJsonPath,
            payloadFormat,
            this.clientIdsPath
        );
        if (this.handle == 0) {
            throw new IllegalStateException("Failed to create engine with rule file");
        }
        if (nativeSetPollBatchLimit(this.handle, Math.max(1, pollBatchLimit)) != 0) {
            nativeDestroy(this.handle);
            this.handle = 0;
            throw new IllegalStateException("Failed to configure poll batch limit");
        }
        RuleMetadata[] metadataTable = nativeGetRuleMetadataTable(this.handle);
        if (metadataTable == null) {
            nativeDestroy(this.handle);
            this.handle = 0;
            throw new IllegalStateException("Failed to load rule metadata table");
        }
        this.ruleMetadataTable = metadataTable;
        this.logCallbackHandle = 0;
        this.callbackSubmittedCount = new AtomicLong();
        this.callbackCompletedCount = new AtomicLong();
        this.callbackDroppedCount = new AtomicLong();
        this.pollerTimeoutPendingCount = new AtomicLong();
        this.shutdownDroppedCount = new AtomicLong();
        this.defaultMessageExecutor = createDefaultMessageExecutor(this.callbackQueueCapacity);
        this.defaultLogExecutor = java.util.concurrent.Executors.newSingleThreadExecutor();
        this.mqttStarted = false;
        this.pollRunning = false;
        this.disconnecting = false;
        this.pollThread = null;
        this.nextHandler = null;
        this.nextExecutor = defaultMessageExecutor;
    }

    public synchronized int start() {
        if (mqttStarted) {
            return 0;
        }
        int rc = nativeStartMqtt(
            handle,
            host,
            port,
            nodeId,
            clientCount,
            null,
            null,
            true,
            3600,
            "bifrore-group",
            false,
            "",
            30,
            multiNci,
            0
        );
        if (rc == 0) {
            mqttStarted = true;
            ensurePollerRunning();
        }
        return rc;
    }

    public synchronized int stop() {
        return disconnect();
    }

    public synchronized int disconnect() {
        if (handle == 0) {
            return -1;
        }
        if (!mqttStarted && !disconnecting) {
            return 0;
        }
        disconnecting = true;
        mqttStarted = false;
        return nativeDisconnect(handle);
    }

    public void onNext(Consumer<EvalResult> handler) {
        onNext(handler, null);
    }

    public synchronized void onNext(Consumer<EvalResult> handler, Executor executor) {
        this.nextHandler = handler;
        this.nextExecutor = executor != null ? executor : defaultMessageExecutor;
        if (handler != null && mqttStarted) {
            ensurePollerRunning();
        }
    }

    public int onLog(LogHandler handler, int minLevel) {
        return onLog(handler, minLevel, null);
    }

    public int onLog(LogHandler handler, int minLevel, Executor executor) {
        if (logCallbackHandle != 0) {
            nativeSetLogCallback(0, 3);
            nativeFreeLogHandler(logCallbackHandle);
            logCallbackHandle = 0;
        }
        if (handler == null) {
            return nativeSetLogCallback(0, minLevel);
        }
        Executor targetExecutor = executor != null ? executor : defaultLogExecutor;
        LogHandler wrapped = (
            level,
            target,
            message,
            timestampMillis,
            threadId,
            modulePath,
            file,
            line
        ) -> targetExecutor.execute(() ->
            handler.onLog(level, target, message, timestampMillis, threadId, modulePath, file, line));
        logCallbackHandle = nativeRegisterLogHandler(wrapped);
        if (logCallbackHandle == 0) {
            return -1;
        }
        return nativeSetLogCallback(logCallbackHandle, minLevel);
    }

    public MetricsSnapshot metrics() {
        if (handle == 0) {
            return new MetricsSnapshot(0, 0, 0, 0);
        }
        return nativeMetricsSnapshot(handle);
    }

    public long callbackDroppedCount() {
        return callbackDroppedCount.get();
    }

    public long callbackPendingCount() {
        return pendingCallbackCount();
    }

    public long callbackQueueDepth() {
        return defaultMessageExecutor.getQueue().size();
    }

    public long shutdownDroppedCount() {
        return shutdownDroppedCount.get();
    }

    public long pollerTimeoutPendingCount() {
        return pollerTimeoutPendingCount.get();
    }

    public RuleMetadata[] ruleMetadataTable() {
        return ruleMetadataTable.clone();
    }

    private synchronized void ensurePollerRunning() {
        if (pollRunning || handle == 0 || (!mqttStarted && !disconnecting)) {
            return;
        }
        pollRunning = true;
        pollThread = new Thread(() -> {
            while (pollRunning && handle != 0) {
                EvalResult[] results = nativePollResultsBatch(handle, -1);
                if (results == null) {
                    break;
                }
                if (results.length == 0) {
                    continue;
                }
                Consumer<EvalResult> handler = nextHandler;
                Executor executor = nextExecutor;
                if (handler != null && executor != null) {
                    for (EvalResult result : results) {
                        if (result != null) {
                            RuleMetadata metadata = metadataFor(result.ruleIndex);
                            EvalResult enriched = new EvalResult(result.ruleIndex, result.payload, metadata);
                            Runnable task = () -> {
                                try {
                                    handler.accept(enriched);
                                } finally {
                                    callbackCompletedCount.incrementAndGet();
                                }
                            };
                            try {
                                executor.execute(task);
                                callbackSubmittedCount.incrementAndGet();
                            } catch (RejectedExecutionException ignored) {
                                // rejection is already logged and counted by the default executor handler
                            }
                        }
                    }
                }
            }
            pollRunning = false;
        }, "bifrore-msg-poller");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    private synchronized void stopPoller(boolean interrupt) {
        if (interrupt) {
            pollRunning = false;
        }
        Thread thread = pollThread;
        pollThread = null;
        if (thread != null) {
            if (interrupt) {
                thread.interrupt();
            }
            try {
                thread.join(POLLER_JOIN_TIMEOUT_MILLIS);
                if (thread.isAlive()) {
                    long pendingCallbacks = pendingCallbackCount();
                    pollerTimeoutPendingCount.addAndGet(pendingCallbacks);
                    LOGGER.warning(
                        "BifroRE poller did not stop within " + POLLER_JOIN_TIMEOUT_MILLIS
                            + " ms; close may drop undelivered results pendingCallbacks="
                            + pendingCallbacks
                    );
                    pollRunning = false;
                    thread.interrupt();
                    thread.join(1000);
                }
            } catch (InterruptedException interrupted) {
                LOGGER.warning(
                    "Interrupted while waiting for BifroRE poller shutdown; pendingCallbacks="
                        + pendingCallbackCount()
                        + " error="
                        + interrupted
                );
                Thread.currentThread().interrupt();
            }
        }
    }

    private long pendingCallbackCount() {
        return Math.max(0L, callbackSubmittedCount.get() - callbackCompletedCount.get());
    }

    private RuleMetadata metadataFor(int ruleIndex) {
        if (ruleIndex < 0 || ruleIndex >= ruleMetadataTable.length) {
            return null;
        }
        return ruleMetadataTable[ruleIndex];
    }

    @Override
    public synchronized void close() {
        if (handle == 0) {
            return;
        }
        disconnect();
        stopPoller(false);
        pollRunning = false;
        disconnecting = false;
        if (logCallbackHandle != 0) {
            nativeSetLogCallback(0, 3);
            nativeFreeLogHandler(logCallbackHandle);
            logCallbackHandle = 0;
        }
        if (handle != 0) {
            nativeDestroy(handle);
            handle = 0;
        }
        defaultMessageExecutor.shutdown();
        try {
            if (!defaultMessageExecutor.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                long dropped = defaultMessageExecutor.shutdownNow().size();
                shutdownDroppedCount.addAndGet(dropped);
                LOGGER.warning(
                    "BifroRE callback executor did not stop within " + EXECUTOR_SHUTDOWN_TIMEOUT_MILLIS
                        + " ms; dropped queued callbacks=" + dropped
                );
            }
        } catch (InterruptedException interrupted) {
            LOGGER.warning(
                "Interrupted while waiting for BifroRE callback executor shutdown; pendingCallbacks="
                    + pendingCallbackCount()
                    + " error="
                    + interrupted
            );
            Thread.currentThread().interrupt();
        }
        defaultLogExecutor.shutdown();
    }

    private ThreadPoolExecutor createDefaultMessageExecutor(int queueCapacity) {
        RejectedExecutionHandler handler = (task, executor) -> {
            long dropped = callbackDroppedCount.incrementAndGet();
            if (dropped <= 10 || dropped % DROP_WARN_EVERY == 0) {
                LOGGER.warning(
                    "BifroRE callback queue is full; dropping newest callback task droppedCount="
                        + dropped
                        + " queueDepth="
                        + executor.getQueue().size()
                );
            }
            throw new RejectedExecutionException("BifroRE callback queue is full");
        };
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, "bifrore-callback");
            thread.setDaemon(true);
            return thread;
        };
        return new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(queueCapacity),
            threadFactory,
            handler
        );
    }

    private static native long nativeCreateWithConfig(String path);
    private static native long nativeCreateWithConfigAndPayloadFormat(String path, int payloadFormat);
    private static native long nativeCreateWithConfigAndPayloadFormatAndClientIdsPath(
        String path,
        int payloadFormat,
        String clientIdsPath
    );
    private static native void nativeDestroy(long handle);
    private static native int nativeDisconnect(long handle);
    private static native int nativeStartMqtt(
        long handle,
        String host,
        int port,
        String nodeId,
        int clientCount,
        String username,
        String password,
        boolean cleanStart,
        int sessionExpiryInterval,
        String groupName,
        boolean ordered,
        String orderedPrefix,
        int keepAliveSecs,
        boolean multiNci,
        long cbHandle
    );
    private static native EvalResult[] nativePollResultsBatch(long handle, int timeoutMillis);
    private static native long nativeRegisterLogHandler(LogHandler handler);
    private static native void nativeFreeLogHandler(long cbHandle);
    private static native int nativeSetLogCallback(long cbHandle, int minLevel);
    private static native MetricsSnapshot nativeMetricsSnapshot(long handle);
    private static native int nativeSetPollBatchLimit(long handle, int limit);
    private static native RuleMetadata[] nativeGetRuleMetadataTable(long handle);
}
