package com.bifrore;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public final class BifroRE implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(BifroRE.class.getName());
    private static final Object INSTANCE_LOCK = new Object();
    private static final long POLLER_JOIN_TIMEOUT_MILLIS = 5000L;
    private static final long EXECUTOR_SHUTDOWN_TIMEOUT_MILLIS = 5000L;
    private static final long DROP_WARN_EVERY = 100L;
    private static final long DIRECT_SLOT_WAIT_MILLIS = 100L;
    private static final long RETRYABLE_POLL_BACKOFF_INITIAL_MILLIS = 10L;
    private static final long RETRYABLE_POLL_BACKOFF_MAX_MILLIS = 250L;

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
    private static final int NOTIFY_MODE_POLL = 0;
    public static final int BRE_OK = 0;
    public static final int BRE_ERR_INVALID_ARGUMENT = -1;
    public static final int BRE_ERR_INVALID_STATE = -2;
    public static final int BRE_ERR_OPERATION_FAILED = -3;
    public static final int BRE_ERR_START_FAILED = -4;
    public static final int BRE_ERR_ALREADY_STARTED = -5;
    public static final int BRE_ERR_WORKER_UNAVAILABLE = -6;

    private static final int JNI_DIRECT_ERR_HEADER_BUFFER_TOO_SMALL = -4;
    private static final int JNI_DIRECT_ERR_PAYLOAD_BUFFER_TOO_SMALL = -5;

    public interface MessageHandler {
        void onMessage(int ruleIndex, byte[] payloadBlob, int offset, int length, RuleMetadata metadata);
    }

    @Experimental("Async direct callback API is unstable and may change in future releases.")
    public interface AsyncDirectMessageHandler {
        CompletionStage<?> onMessage(
            int ruleIndex,
            ByteBuffer payloadBuffer,
            int offset,
            int length,
            RuleMetadata metadata
        );
    }

    public static final class RuleMetadata {
        public final int ruleIndex;
        public final String[] destinations;

        RuleMetadata(int ruleIndex, String[] destinations) {
            this.ruleIndex = ruleIndex;
            this.destinations = destinations;
        }
    }

    static final class PollBatch {
        final int[] headerTriples;
        final byte[] payloadData;

        PollBatch(int[] headerTriples, byte[] payloadData) {
            this.headerTriples = headerTriples;
            this.payloadData = payloadData;
        }
    }

    static final class PollResult {
        final int code;
        final PollBatch batch;

        PollResult(int code, PollBatch batch) {
            this.code = code;
            this.batch = batch;
        }
    }

    static final class PollSlot {
        final ByteBuffer headerBuffer;
        final int headerCapacityInts;
        final ByteBuffer payloadBuffer;
        final int payloadCapacityBytes;
        volatile int messageCount;

        PollSlot(int headerIntsCapacity, int payloadBufferBytes) {
            this.headerCapacityInts = headerIntsCapacity;
            this.headerBuffer = ByteBuffer
                .allocateDirect(headerIntsCapacity * Integer.BYTES)
                .order(ByteOrder.nativeOrder());
            this.payloadCapacityBytes = payloadBufferBytes;
            this.payloadBuffer = ByteBuffer
                .allocateDirect(payloadBufferBytes)
                .order(ByteOrder.nativeOrder());
            this.messageCount = 0;
        }
    }

    private enum PollLoopAction {
        CONTINUE,
        BACKOFF,
        STOP
    }

    public static final class MetricsSnapshot {
        public static final class StageLatencySnapshot {
            public final long count;
            public final long totalNanos;
            public final long maxNanos;

            StageLatencySnapshot(long count, long totalNanos, long maxNanos) {
                this.count = count;
                this.totalNanos = totalNanos;
                this.maxNanos = maxNanos;
            }
        }

        public final long ingressMessageCount;
        public final long coreQueueDepth;
        public final long coreQueueDepthMax;
        public final long coreQueueDropCount;
        public final long ffiQueueDepth;
        public final long ffiQueueDepthMax;
        public final long ffiQueueDropCount;
        public final StageLatencySnapshot messagePipeline;
        public final long evalCount;
        public final long evalErrorCount;
        public final StageLatencySnapshot exec;
        public final StageLatencySnapshot topicMatch;
        public final StageLatencySnapshot payloadDecode;
        public final StageLatencySnapshot msgIrBuild;
        public final StageLatencySnapshot predicate;
        public final StageLatencySnapshot projection;

        MetricsSnapshot(
            long ingressMessageCount,
            long coreQueueDepth,
            long coreQueueDepthMax,
            long coreQueueDropCount,
            long ffiQueueDepth,
            long ffiQueueDepthMax,
            long ffiQueueDropCount,
            StageLatencySnapshot messagePipeline,
            long evalCount,
            long evalErrorCount,
            StageLatencySnapshot exec,
            StageLatencySnapshot topicMatch,
            StageLatencySnapshot payloadDecode,
            StageLatencySnapshot msgIrBuild,
            StageLatencySnapshot predicate,
            StageLatencySnapshot projection
        ) {
            this.ingressMessageCount = ingressMessageCount;
            this.coreQueueDepth = coreQueueDepth;
            this.coreQueueDepthMax = coreQueueDepthMax;
            this.coreQueueDropCount = coreQueueDropCount;
            this.ffiQueueDepth = ffiQueueDepth;
            this.ffiQueueDepthMax = ffiQueueDepthMax;
            this.ffiQueueDropCount = ffiQueueDropCount;
            this.messagePipeline = messagePipeline;
            this.evalCount = evalCount;
            this.evalErrorCount = evalErrorCount;
            this.exec = exec;
            this.topicMatch = topicMatch;
            this.payloadDecode = payloadDecode;
            this.msgIrBuild = msgIrBuild;
            this.predicate = predicate;
            this.projection = projection;
        }

        static MetricsSnapshot empty() {
            StageLatencySnapshot emptyStage = new StageLatencySnapshot(0, 0, 0);
            return new MetricsSnapshot(
                0, 0, 0, 0, 0, 0, 0, emptyStage, 0, 0,
                emptyStage,
                emptyStage,
                emptyStage,
                emptyStage,
                emptyStage,
                emptyStage
            );
        }

        static MetricsSnapshot from(long[] values) {
            if (values == null || values.length < 30) {
                return empty();
            }
            int index = 0;
            long ingressMessageCount = values[index++];
            long coreQueueDepth = values[index++];
            long coreQueueDepthMax = values[index++];
            long coreQueueDropCount = values[index++];
            long ffiQueueDepth = values[index++];
            long ffiQueueDepthMax = values[index++];
            long ffiQueueDropCount = values[index++];
            StageLatencySnapshot messagePipeline = readStageWithCount(values, index);
            index += 3;
            long evalCount = values[index++];
            long evalErrorCount = values[index++];
            StageLatencySnapshot exec = readStageWithCount(values, index);
            index += 3;
            StageLatencySnapshot topicMatch = readStageWithCount(values, index);
            index += 3;
            StageLatencySnapshot payloadDecode = readStageWithCount(values, index);
            index += 3;
            StageLatencySnapshot msgIrBuild = readStageWithCount(values, index);
            index += 3;
            StageLatencySnapshot predicate = readStageWithCount(values, index);
            index += 3;
            StageLatencySnapshot projection = readStageWithCount(values, index);
            return new MetricsSnapshot(
                ingressMessageCount,
                coreQueueDepth,
                coreQueueDepthMax,
                coreQueueDropCount,
                ffiQueueDepth,
                ffiQueueDepthMax,
                ffiQueueDropCount,
                messagePipeline,
                evalCount,
                evalErrorCount,
                exec,
                topicMatch,
                payloadDecode,
                msgIrBuild,
                predicate,
                projection
            );
        }

        private static StageLatencySnapshot readStageWithCount(long[] values, int offset) {
            return new StageLatencySnapshot(
                values[offset],
                values[offset + 1],
                values[offset + 2]
            );
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
    private final String username;
    private final String password;
    private final String clientIdsPath;
    private final int callbackQueueCapacity;
    private final int directPollSlotCount;
    private final int directPayloadBufferBytes;
    private final boolean detailedLatencyMetrics;
    private final boolean cleanStart;
    private final int sessionExpiryInterval;
    private final String groupName;
    private final RuleMetadata[] ruleMetadataTable;
    private final ArrayBlockingQueue<PollSlot> freeDirectSlots;
    private final AtomicLong callbackDroppedCount;
    private volatile long heapPollMessageCount;
    private volatile long heapPollErrorCount;
    private volatile long heapPollInvalidArgumentCount;
    private volatile long heapPollInvalidStateCount;
    private volatile long heapPollOperationFailedCount;
    private volatile long heapPollUnknownErrorCount;
    private volatile long heapPollNoDataCount;
    private volatile long heapPollPayloadBytes;
    private final AtomicLong pollerTimeoutPendingCount;
    private final AtomicLong shutdownDroppedCount;
    private volatile boolean mqttStarted;
    private volatile boolean pollRunning;
    private volatile boolean disconnecting;
    private volatile Thread pollThread;
    private volatile MessageHandler nextHandler;
    private volatile AsyncDirectMessageHandler nextAsyncDirectHandler;
    private volatile Executor nextExecutor;
    private volatile boolean singletonHeld;

    public BifroRE(BifroREOptions options) {
        this(
            options.mqtt.host,
            options.mqtt.port,
            Objects.requireNonNull(options.ffi.ruleJsonPath, "ruleJsonPath"),
            options.mqtt.nodeId,
            options.mqtt.clientCount,
            options.mqtt.multiNci,
            options.mqtt.username,
            options.mqtt.password,
            options.ffi.payloadFormat,
            options.ffi.clientIdsPath,
            options.ffi.protobufDescriptorSetPath,
            options.ffi.protobufMessageName,
            options.jvm.callbackQueueCapacity,
            options.jvm.pollBatchLimit,
            options.jvm.directPollSlotCount,
            options.jvm.directPayloadBufferBytes,
            options.ffi.detailedLatencyMetrics,
            options.mqtt.cleanStart,
            options.mqtt.sessionExpiryInterval,
            options.mqtt.groupName
        );
    }

    private BifroRE(
        String host,
        int port,
        String ruleJsonPath,
        String nodeId,
        int clientCount,
        boolean multiNci,
        String username,
        String password,
        int payloadFormat,
        String clientIdsPath,
        String protobufDescriptorSetPath,
        String protobufMessageName,
        int callbackQueueCapacity,
        int pollBatchLimit,
        int directPollSlotCount,
        int directPayloadBufferBytes,
        boolean detailedLatencyMetrics,
        boolean cleanStart,
        int sessionExpiryInterval,
        String groupName
    ) {
        acquireSingleton();
        this.singletonHeld = true;
        boolean initialized = false;
        try {
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.nodeId = nodeId;
        this.clientCount = clientCount;
        this.multiNci = multiNci;
        this.username = username;
        this.password = password;
        this.callbackQueueCapacity = Math.max(1, callbackQueueCapacity);
        this.directPollSlotCount = Math.max(1, directPollSlotCount);
        this.directPayloadBufferBytes = Math.max(1, directPayloadBufferBytes);
        this.detailedLatencyMetrics = detailedLatencyMetrics;
        this.cleanStart = cleanStart;
        this.sessionExpiryInterval = Math.max(0, sessionExpiryInterval);
        this.groupName =
            (groupName == null || groupName.isBlank()) ? "bifrore-group" : groupName;
        this.clientIdsPath =
            (clientIdsPath == null || clientIdsPath.isBlank()) ? "./client_ids" : clientIdsPath;
        this.handle = nativeCreateEngine(
            ruleJsonPath,
            payloadFormat,
            this.clientIdsPath,
            NOTIFY_MODE_POLL,
            protobufDescriptorSetPath,
            protobufMessageName
        );
        if (this.handle == 0) {
            throw new IllegalStateException("Failed to create engine with rule file");
        }
        if (nativeSetPollBatchLimit(this.handle, Math.max(1, pollBatchLimit)) != BRE_OK) {
            nativeDestroy(this.handle);
            this.handle = 0;
            throw new IllegalStateException("Failed to configure poll batch limit");
        }
        if (nativeSetDetailedLatencyMetrics(this.handle, this.detailedLatencyMetrics) != BRE_OK) {
            nativeDestroy(this.handle);
            this.handle = 0;
            throw new IllegalStateException("Failed to configure detailed latency metrics");
        }
        RuleMetadata[] metadataTable = nativeGetRuleMetadataTable(this.handle);
        if (metadataTable == null) {
            nativeDestroy(this.handle);
            this.handle = 0;
            throw new IllegalStateException("Failed to load rule metadata table");
        }
        this.ruleMetadataTable = metadataTable;
        this.freeDirectSlots = createDirectSlotPool(this.directPollSlotCount, Math.max(1, pollBatchLimit), this.directPayloadBufferBytes);
        this.logCallbackHandle = 0;
        this.callbackDroppedCount = new AtomicLong();
        this.heapPollMessageCount = 0L;
        this.heapPollErrorCount = 0L;
        this.heapPollInvalidArgumentCount = 0L;
        this.heapPollInvalidStateCount = 0L;
        this.heapPollOperationFailedCount = 0L;
        this.heapPollUnknownErrorCount = 0L;
        this.heapPollNoDataCount = 0L;
        this.heapPollPayloadBytes = 0L;
        this.pollerTimeoutPendingCount = new AtomicLong();
        this.shutdownDroppedCount = new AtomicLong();
        this.defaultMessageExecutor = createDefaultMessageExecutor(this.callbackQueueCapacity);
        this.defaultLogExecutor = java.util.concurrent.Executors.newSingleThreadExecutor();
        this.mqttStarted = false;
        this.pollRunning = false;
        this.disconnecting = false;
        this.pollThread = null;
        this.nextHandler = null;
        this.nextAsyncDirectHandler = null;
        this.nextExecutor = defaultMessageExecutor;
        initialized = true;
        } finally {
            if (!initialized) {
                releaseSingleton();
                this.singletonHeld = false;
            }
        }
    }

    public synchronized int start() {
        if (mqttStarted) {
            return BRE_OK;
        }
        int rc = nativeStartMqtt(
            handle,
            host,
            port,
            nodeId,
            clientCount,
            username,
            password,
            cleanStart,
            sessionExpiryInterval,
            groupName,
            false,
            "",
            30,
            multiNci
        );
        if (rc == BRE_OK) {
            mqttStarted = true;
            reconcilePollerRegistration();
        }
        return rc;
    }

    public synchronized int stop() {
        return disconnect();
    }

    public synchronized int disconnect() {
        if (handle == 0) {
            return BRE_ERR_INVALID_ARGUMENT;
        }
        if (!mqttStarted && !disconnecting) {
            return BRE_OK;
        }
        disconnecting = true;
        mqttStarted = false;
        return nativeDisconnect(handle);
    }

    public void onNext(MessageHandler handler) {
        onNext(handler, null);
    }

    public void onNext(MessageHandler handler, Executor executor) {
        this.nextHandler = handler;
        this.nextAsyncDirectHandler = null;
        this.nextExecutor = executor != null ? executor : defaultMessageExecutor;
        reconcilePollerRegistration();
    }

    @Experimental("Async direct callback API is unstable and may change in future releases.")
    public void onNextAsyncDirect(AsyncDirectMessageHandler handler) {
        onNextAsyncDirect(handler, null);
    }

    @Experimental("Async direct callback API is unstable and may change in future releases.")
    public void onNextAsyncDirect(AsyncDirectMessageHandler handler, Executor executor) {
        this.nextAsyncDirectHandler = handler;
        this.nextHandler = null;
        this.nextExecutor = executor != null ? executor : defaultMessageExecutor;
        reconcilePollerRegistration();
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
            return BRE_ERR_INVALID_ARGUMENT;
        }
        return nativeSetLogCallback(logCallbackHandle, minLevel);
    }

    public MetricsSnapshot metrics() {
        if (handle == 0) {
            return MetricsSnapshot.empty();
        }
        return MetricsSnapshot.from(nativeMetricsSnapshotValues(handle));
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

    public long heapPollErrorCount() {
        return heapPollErrorCount;
    }

    public long heapPollInvalidArgumentCount() {
        return heapPollInvalidArgumentCount;
    }

    public long heapPollInvalidStateCount() {
        return heapPollInvalidStateCount;
    }

    public long heapPollOperationFailedCount() {
        return heapPollOperationFailedCount;
    }

    public long heapPollUnknownErrorCount() {
        return heapPollUnknownErrorCount;
    }

    public long heapPollMessageCount() {
        return heapPollMessageCount;
    }

    public long heapPollNoDataCount() {
        return heapPollNoDataCount;
    }

    public long heapPollPayloadBytes() {
        return heapPollPayloadBytes;
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
            long retryablePollBackoffMillis = 0L;
            while (pollRunning && handle != 0) {
                AsyncDirectMessageHandler asyncDirectHandler = nextAsyncDirectHandler;
                PollLoopAction action;
                if (asyncDirectHandler != null) {
                    action = pollDirectBatch(asyncDirectHandler, nextExecutor);
                    if (action == PollLoopAction.STOP) {
                        break;
                    }
                } else {
                    action = pollHeapBatch(nextHandler, nextExecutor);
                    if (action == PollLoopAction.STOP) {
                        break;
                    }
                }
                if (action == PollLoopAction.BACKOFF) {
                    retryablePollBackoffMillis = nextRetryablePollBackoffMillis(retryablePollBackoffMillis);
                    if (!sleepRetryablePollBackoff(retryablePollBackoffMillis)) {
                        break;
                    }
                } else {
                    retryablePollBackoffMillis = 0L;
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

    private void reconcilePollerRegistration() {
        if (!mqttStarted) {
            return;
        }
        if (nextHandler != null || nextAsyncDirectHandler != null) {
            ensurePollerRunning();
        } else {
            stopPoller(true);
        }
    }

    private long pendingCallbackCount() {
        return Math.max(0L, defaultMessageExecutor.getTaskCount() - defaultMessageExecutor.getCompletedTaskCount());
    }

    private boolean shouldStopPollerForHeapCode(int code) {
        return code == BRE_ERR_INVALID_ARGUMENT || code == BRE_ERR_INVALID_STATE;
    }

    private static long nextRetryablePollBackoffMillis(long currentBackoffMillis) {
        if (currentBackoffMillis <= 0L) {
            return RETRYABLE_POLL_BACKOFF_INITIAL_MILLIS;
        }
        return Math.min(RETRYABLE_POLL_BACKOFF_MAX_MILLIS, currentBackoffMillis * 2L);
    }

    private boolean sleepRetryablePollBackoff(long backoffMillis) {
        try {
            Thread.sleep(backoffMillis);
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void recordHeapPollErrorCode(int code) {
        heapPollErrorCount += 1;
        if (code == BRE_ERR_INVALID_ARGUMENT) {
            heapPollInvalidArgumentCount += 1;
        } else if (code == BRE_ERR_INVALID_STATE) {
            heapPollInvalidStateCount += 1;
        } else if (code == BRE_ERR_OPERATION_FAILED) {
            heapPollOperationFailedCount += 1;
        } else {
            heapPollUnknownErrorCount += 1;
        }
    }

    private PollLoopAction pollHeapBatch(MessageHandler handler, Executor executor) {
        if (handler == null) {
            return PollLoopAction.CONTINUE;
        }
        PollResult result = nativePollResultsBatch(handle, -1);
        if (result.code < BRE_OK) {
            recordHeapPollErrorCode(result.code);
            return shouldStopPollerForHeapCode(result.code) ? PollLoopAction.STOP : PollLoopAction.BACKOFF;
        }
        PollBatch batch = result.batch;
        if (batch == null || batch.headerTriples.length == 0) {
            heapPollNoDataCount += 1;
            return PollLoopAction.CONTINUE;
        }
        int messageCount = batch.headerTriples.length / 3;
        heapPollMessageCount += messageCount;
        heapPollPayloadBytes += batch.payloadData.length;
        dispatchHeapBatch(batch, ruleMetadataTable, handler, executor);
        return PollLoopAction.CONTINUE;
    }

    static void dispatchHeapBatch(
        PollBatch batch,
        RuleMetadata[] ruleMetadataTable,
        MessageHandler handler,
        Executor executor
    ) {
        int messageCount = batch.headerTriples.length / 3;
        for (int i = 0; i < messageCount; i++) {
            int base = i * 3;
            int ruleIndex = batch.headerTriples[base];
            int payloadOffset = batch.headerTriples[base + 1];
            int payloadLength = batch.headerTriples[base + 2];
            RuleMetadata metadata = ruleIndex >= 0 && ruleIndex < ruleMetadataTable.length
                ? ruleMetadataTable[ruleIndex]
                : null;
            Runnable task = () -> {
                handler.onMessage(
                    ruleIndex,
                    batch.payloadData,
                    payloadOffset,
                    payloadLength,
                    metadata
                );
            };
            try {
                executor.execute(task);
            } catch (RejectedExecutionException ignored) {
                // rejection is already logged and counted by the default executor handler
            }
        }
    }

    private PollLoopAction pollDirectBatch(AsyncDirectMessageHandler handler, Executor executor) {
        if (executor == null) {
            return PollLoopAction.CONTINUE;
        }
        PollSlot slot = acquireDirectSlot();
        if (slot == null) {
            return PollLoopAction.CONTINUE;
        }
        int count = nativePollResultsBatchDirect(
            handle,
            -1,
            slot.headerBuffer,
            slot.headerCapacityInts,
            slot.payloadBuffer,
            slot.payloadCapacityBytes
        );
        if (count == BRE_ERR_INVALID_ARGUMENT || count == BRE_ERR_INVALID_STATE) {
            releaseDirectSlot(slot);
            return PollLoopAction.STOP;
        }
        if (count == BRE_ERR_OPERATION_FAILED) {
            releaseDirectSlot(slot);
            return PollLoopAction.BACKOFF;
        }
        if (count <= BRE_OK) {
            if (count == JNI_DIRECT_ERR_PAYLOAD_BUFFER_TOO_SMALL) {
                LOGGER.warning(
                    "BifroRE direct payload buffer is too small for the next message; increase directPayloadBufferBytes"
                );
            } else if (count == JNI_DIRECT_ERR_HEADER_BUFFER_TOO_SMALL) {
                LOGGER.warning("BifroRE direct header buffer is too small; increase pollBatchLimit or header sizing");
            }
            releaseDirectSlot(slot);
            return count >= BRE_OK ? PollLoopAction.CONTINUE : PollLoopAction.BACKOFF;
        }

        slot.messageCount = count;
        IntBuffer headerView = slot.headerBuffer.asReadOnlyBuffer()
            .order(ByteOrder.nativeOrder())
            .asIntBuffer();
        ByteBuffer payloadView = slot.payloadBuffer.asReadOnlyBuffer().order(ByteOrder.nativeOrder());
        for (int i = 0; i < count; i++) {
            int base = i * 3;
            int ruleIndex = headerView.get(base);
            int payloadOffset = headerView.get(base + 1);
            int payloadLength = headerView.get(base + 2);
            RuleMetadata metadata = metadataFor(ruleIndex);
            Runnable task = () -> {
                CompletionStage<?> stage = null;
                try {
                    stage = handler.onMessage(
                        ruleIndex,
                        payloadView,
                        payloadOffset,
                        payloadLength,
                        metadata
                    );
                } catch (Throwable error) {
                    releaseDirectSlotRef(slot);
                    throw error;
                }
                if (stage == null) {
                    releaseDirectSlotRef(slot);
                    return;
                }
                stage.whenComplete((ignored, error) -> {
                    releaseDirectSlotRef(slot);
                    if (error != null) {
                        LOGGER.warning("BifroRE async direct handler completed exceptionally: " + error);
                    }
                });
            };
            try {
                executor.execute(task);
            } catch (RejectedExecutionException ignored) {
                releaseDirectSlotRef(slot);
            }
        }
        return PollLoopAction.CONTINUE;
    }

    private PollSlot acquireDirectSlot() {
        while (pollRunning && handle != 0) {
            try {
                PollSlot slot = freeDirectSlots.poll(DIRECT_SLOT_WAIT_MILLIS, TimeUnit.MILLISECONDS);
                if (slot != null) {
                    return slot;
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }

    private void releaseDirectSlotRef(PollSlot slot) {
        synchronized (slot) {
            slot.messageCount -= 1;
            if (slot.messageCount == 0) {
                releaseDirectSlot(slot);
            }
        }
    }

    private void releaseDirectSlot(PollSlot slot) {
        slot.headerBuffer.clear();
        slot.payloadBuffer.clear();
        slot.messageCount = 0;
        freeDirectSlots.offer(slot);
    }

    private static ArrayBlockingQueue<PollSlot> createDirectSlotPool(
        int slotCount,
        int pollBatchLimit,
        int payloadBufferBytes
    ) {
        ArrayBlockingQueue<PollSlot> slots = new ArrayBlockingQueue<>(slotCount);
        int headerIntsCapacity = Math.max(3, pollBatchLimit * 3);
        for (int i = 0; i < slotCount; i++) {
            slots.add(new PollSlot(headerIntsCapacity, payloadBufferBytes));
        }
        return slots;
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
        if (singletonHeld) {
            releaseSingleton();
            singletonHeld = false;
        }
    }

    private static void acquireSingleton() {
        synchronized (INSTANCE_LOCK) {
            if (SingletonHolder.INSTANCE_EXISTS) {
                throw new IllegalStateException("Only one BifroRE instance is allowed per JVM");
            }
            SingletonHolder.INSTANCE_EXISTS = true;
        }
    }

    private static void releaseSingleton() {
        synchronized (INSTANCE_LOCK) {
            SingletonHolder.INSTANCE_EXISTS = false;
        }
    }

    private static final class SingletonHolder {
        private static boolean INSTANCE_EXISTS = false;
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

    private static native long nativeCreateEngine(
        String path,
        int payloadFormat,
        String clientIdsPath,
        int notifyMode,
        String protobufDescriptorSetPath,
        String protobufMessageName
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
        boolean multiNci
    );
    private static native PollResult nativePollResultsBatch(long handle, int timeoutMillis);
    private static native int nativePollResultsBatchDirect(
        long handle,
        int timeoutMillis,
        ByteBuffer headerBuffer,
        int headerCapacityInts,
        ByteBuffer payloadBuffer,
        int payloadCapacityBytes
    );
    private static native long nativeRegisterLogHandler(LogHandler handler);
    private static native void nativeFreeLogHandler(long cbHandle);
    private static native int nativeSetLogCallback(long cbHandle, int minLevel);
    private static native long[] nativeMetricsSnapshotValues(long handle);
    private static native int nativeSetPollBatchLimit(long handle, int limit);
    private static native int nativeSetDetailedLatencyMetrics(long handle, boolean enabled);
    private static native RuleMetadata[] nativeGetRuleMetadataTable(long handle);
}
