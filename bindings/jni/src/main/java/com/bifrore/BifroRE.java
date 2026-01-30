package com.bifrore;

public final class BifroRE implements AutoCloseable {
    static {
        System.loadLibrary("bifrore_jni");
    }

    public interface MessageHandler {
        void onMessage(String ruleId, byte[] payload, String destinationsJson);
    }

    private long handle;
    private long callbackHandle;
    private MessageHandler handler;
    private final String host;
    private final int port;

    public BifroRE(String host, int port) {
        this.host = host;
        this.port = port;
        this.handle = nativeCreate();
        this.callbackHandle = 0;
    }

    public void onMessage(MessageHandler handler) {
        this.handler = handler;
        if (callbackHandle != 0) {
            nativeFreeHandler(callbackHandle);
        }
        this.callbackHandle = nativeRegisterHandler(handler);
    }

    public int loadRules(String ruleJsonPath) {
        return nativeLoadRules(handle, ruleJsonPath);
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

    public int eval(String topic, byte[] payload) {
        return nativeEval(handle, topic, payload, callbackHandle);
    }

    @Override
    public void close() {
        if (callbackHandle != 0) {
            nativeFreeHandler(callbackHandle);
            callbackHandle = 0;
        }
        if (handle != 0) {
            nativeDestroy(handle);
            handle = 0;
        }
    }

    private static native long nativeCreate();
    private static native void nativeDestroy(long handle);
    private static native int nativeLoadRules(long handle, String path);
    private static native int nativeEval(long handle, String topic, byte[] payload, long cbHandle);
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
}
