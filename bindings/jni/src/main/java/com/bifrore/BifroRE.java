package com.bifrore;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class BifroRE implements AutoCloseable {
    static {
        System.loadLibrary("bifrore_jni");
    }

    public interface MessageHandler {
        void onMessage(String ruleId, byte[] payload, String destinationsJson);
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

    private long handle;
    private long callbackHandle;
    private long logCallbackHandle;
    private final ExecutorService defaultMessageExecutor;
    private final ExecutorService defaultLogExecutor;
    private final String host;
    private final int port;

    public BifroRE(String host, int port, String ruleJsonPath) {
        this.host = host;
        this.port = port;
        this.handle = nativeCreateWithRules(ruleJsonPath);
        if (this.handle == 0) {
            throw new IllegalStateException("Failed to create engine with rule file");
        }
        this.callbackHandle = 0;
        this.logCallbackHandle = 0;
        this.defaultMessageExecutor = Executors.newSingleThreadExecutor();
        this.defaultLogExecutor = Executors.newSingleThreadExecutor();
    }

    public void onMessage(MessageHandler handler) {
        onMessage(handler, null);
    }

    public void onMessage(MessageHandler handler, Executor executor) {
        if (handler == null) {
            if (callbackHandle != 0) {
                nativeFreeHandler(callbackHandle);
                callbackHandle = 0;
            }
            return;
        }
        Executor targetExecutor = executor != null ? executor : defaultMessageExecutor;
        MessageHandler wrapped = (ruleId, payload, destinationsJson) ->
            targetExecutor.execute(() -> handler.onMessage(ruleId, payload, destinationsJson));
        if (callbackHandle != 0) {
            nativeFreeHandler(callbackHandle);
        }
        this.callbackHandle = nativeRegisterHandler(wrapped);
    }

    public int start() {
        return nativeStartMqtt(
            handle,
            host,
            port,
            "bifrore-embed",
            null,
            null,
            true,
            3600,
            "bifrore-group",
            false,
            "",
            30,
            callbackHandle
        );
    }

    public int stop() {
        return nativeStopMqtt(handle);
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

    @Override
    public void close() {
        if (logCallbackHandle != 0) {
            nativeSetLogCallback(0, 3);
            nativeFreeLogHandler(logCallbackHandle);
            logCallbackHandle = 0;
        }
        if (callbackHandle != 0) {
            nativeFreeHandler(callbackHandle);
            callbackHandle = 0;
        }
        if (handle != 0) {
            nativeDestroy(handle);
            handle = 0;
        }
        defaultMessageExecutor.shutdown();
        defaultLogExecutor.shutdown();
    }

    private static native long nativeCreateWithRules(String path);
    private static native void nativeDestroy(long handle);
    private static native int nativeStartMqtt(
        long handle,
        String host,
        int port,
        String clientId,
        String username,
        String password,
        boolean cleanStart,
        int sessionExpiryInterval,
        String groupName,
        boolean ordered,
        String orderedPrefix,
        int keepAliveSecs,
        long cbHandle
    );
    private static native int nativeStopMqtt(long handle);
    private static native long nativeRegisterHandler(MessageHandler handler);
    private static native void nativeFreeHandler(long cbHandle);
    private static native long nativeRegisterLogHandler(LogHandler handler);
    private static native void nativeFreeLogHandler(long cbHandle);
    private static native int nativeSetLogCallback(long cbHandle, int minLevel);
}
