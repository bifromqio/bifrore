package com.bifrore;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public final class BifroRE implements AutoCloseable {
    static {
        System.loadLibrary("bifrore_jni");
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
        public final String ruleId;
        public final byte[] payload;
        public final String destinationsJson;

        EvalResult(String ruleId, byte[] payload, String destinationsJson) {
            this.ruleId = ruleId;
            this.payload = payload;
            this.destinationsJson = destinationsJson;
        }
    }

    private long handle;
    private long logCallbackHandle;
    private final ExecutorService defaultMessageExecutor;
    private final ExecutorService defaultLogExecutor;
    private final String host;
    private final int port;
    private final String clientPrefix;
    private final String nodeId;
    private final int clientCount;
    private final boolean multiNci;
    private final String clientIdsPath;
    private volatile boolean mqttStarted;
    private volatile boolean pollRunning;
    private volatile Thread pollThread;
    private volatile Consumer<EvalResult> nextHandler;
    private volatile Executor nextExecutor;

    public BifroRE(String host, int port, String ruleJsonPath) {
        this(host, port, ruleJsonPath, "bifrore-embed", null, 1, false, PAYLOAD_JSON, "./client_ids");
    }

    public BifroRE(
        String host,
        int port,
        String ruleJsonPath,
        String clientPrefix,
        String nodeId,
        int clientCount,
        boolean multiNci
    ) {
        this(host, port, ruleJsonPath, clientPrefix, nodeId, clientCount, multiNci, PAYLOAD_JSON, "./client_ids");
    }

    public BifroRE(
        String host,
        int port,
        String ruleJsonPath,
        String clientPrefix,
        String nodeId,
        int clientCount,
        boolean multiNci,
        int payloadFormat
    ) {
        this(host, port, ruleJsonPath, clientPrefix, nodeId, clientCount, multiNci, payloadFormat, "./client_ids");
    }

    public BifroRE(
        String host,
        int port,
        String ruleJsonPath,
        String clientPrefix,
        String nodeId,
        int clientCount,
        boolean multiNci,
        int payloadFormat,
        String clientIdsPath
    ) {
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.clientPrefix = Objects.requireNonNull(clientPrefix, "clientPrefix");
        this.nodeId = nodeId;
        this.clientCount = clientCount;
        this.multiNci = multiNci;
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
        this.logCallbackHandle = 0;
        this.defaultMessageExecutor = Executors.newSingleThreadExecutor();
        this.defaultLogExecutor = Executors.newSingleThreadExecutor();
        this.mqttStarted = false;
        this.pollRunning = false;
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
            clientPrefix,
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
        stopPoller();
        mqttStarted = false;
        return 0;
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

    private synchronized void ensurePollerRunning() {
        if (pollRunning || !mqttStarted || handle == 0) {
            return;
        }
        pollRunning = true;
        pollThread = new Thread(() -> {
            while (pollRunning && handle != 0 && mqttStarted) {
                EvalResult[] results = nativePollResultsBatch(handle, -1);
                if (results == null || results.length == 0) {
                    continue;
                }
                Consumer<EvalResult> handler = nextHandler;
                Executor executor = nextExecutor;
                if (handler != null && executor != null) {
                    for (EvalResult result : results) {
                        if (result != null) {
                            executor.execute(() -> handler.accept(result));
                        }
                    }
                }
            }
        }, "bifrore-msg-poller");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    private synchronized void stopPoller() {
        pollRunning = false;
        Thread thread = pollThread;
        pollThread = null;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(1000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public synchronized void close() {
        stop();
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
        defaultLogExecutor.shutdown();
    }

    private static native long nativeCreateWithConfig(String path);
    private static native long nativeCreateWithConfigAndPayloadFormat(String path, int payloadFormat);
    private static native long nativeCreateWithConfigAndPayloadFormatAndClientIdsPath(
        String path,
        int payloadFormat,
        String clientIdsPath
    );
    private static native void nativeDestroy(long handle);
    private static native int nativeStartMqtt(
        long handle,
        String host,
        int port,
        String clientPrefix,
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
}
