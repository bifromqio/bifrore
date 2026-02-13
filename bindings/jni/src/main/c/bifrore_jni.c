#include <jni.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// FFI functions from Rust cdylib
extern void *bre_create_with_rules(const char *path);
extern void bre_destroy(void *engine);
extern int bre_start_mqtt(
    void *engine,
    const char *host,
    uint16_t port,
    const char *client_id,
    const char *username,
    const char *password,
    jboolean clean_start,
    uint32_t session_expiry_interval,
    const char *group_name,
    jboolean ordered,
    const char *ordered_prefix,
    uint16_t keep_alive_secs,
    void (*callback)(void *, const char *, const unsigned char *, size_t, const char *),
    void *user_data);
extern int bre_stop_mqtt(void *engine);
extern int bre_set_log_callback(
    void (*callback)(void *, int, const char *, const char *, uint64_t, const char *, const char *, const char *, uint32_t),
    void *user_data,
    int min_level);

struct CallbackCtx {
    JavaVM *jvm;
    jobject handler;
    jmethodID on_message;
};

struct LogCallbackCtx {
    JavaVM *jvm;
    jobject handler;
    jmethodID on_log;
};

static void call_java_handler(
    void *user_data,
    const char *rule_id,
    const unsigned char *payload,
    size_t payload_len,
    const char *destinations_json) {
    struct CallbackCtx *ctx = (struct CallbackCtx *)user_data;
    if (ctx == NULL || ctx->handler == NULL) {
        return;
    }

    JNIEnv *env = NULL;
    if ((*ctx->jvm)->AttachCurrentThread(ctx->jvm, (void **)&env, NULL) != 0) {
        return;
    }

    jstring rule_id_str = (*env)->NewStringUTF(env, rule_id ? rule_id : "");
    jbyteArray payload_arr = (*env)->NewByteArray(env, (jsize)payload_len);
    if (payload_arr != NULL && payload != NULL) {
        (*env)->SetByteArrayRegion(env, payload_arr, 0, (jsize)payload_len, (const jbyte *)payload);
    }
    jstring destinations_str = (*env)->NewStringUTF(env, destinations_json ? destinations_json : "[]");

    (*env)->CallVoidMethod(env, ctx->handler, ctx->on_message, rule_id_str, payload_arr, destinations_str);

    if (rule_id_str) {
        (*env)->DeleteLocalRef(env, rule_id_str);
    }
    if (payload_arr) {
        (*env)->DeleteLocalRef(env, payload_arr);
    }
    if (destinations_str) {
        (*env)->DeleteLocalRef(env, destinations_str);
    }
}

static void call_java_log_handler(
    void *user_data,
    int level,
    const char *target,
    const char *message,
    uint64_t timestamp_millis,
    const char *thread_id,
    const char *module_path,
    const char *file,
    uint32_t line) {
    struct LogCallbackCtx *ctx = (struct LogCallbackCtx *)user_data;
    if (ctx == NULL || ctx->handler == NULL) {
        return;
    }

    JNIEnv *env = NULL;
    if ((*ctx->jvm)->AttachCurrentThread(ctx->jvm, (void **)&env, NULL) != 0) {
        return;
    }

    jstring target_str = (*env)->NewStringUTF(env, target ? target : "bifrore");
    jstring message_str = (*env)->NewStringUTF(env, message ? message : "");
    jstring thread_id_str = (*env)->NewStringUTF(env, thread_id ? thread_id : "unknown-thread");
    jstring module_path_str = (*env)->NewStringUTF(env, module_path ? module_path : "");
    jstring file_str = (*env)->NewStringUTF(env, file ? file : "");
    (*env)->CallVoidMethod(
        env,
        ctx->handler,
        ctx->on_log,
        (jint)level,
        target_str,
        message_str,
        (jlong)timestamp_millis,
        thread_id_str,
        module_path_str,
        file_str,
        (jint)line);

    if (target_str) {
        (*env)->DeleteLocalRef(env, target_str);
    }
    if (message_str) {
        (*env)->DeleteLocalRef(env, message_str);
    }
    if (thread_id_str) {
        (*env)->DeleteLocalRef(env, thread_id_str);
    }
    if (module_path_str) {
        (*env)->DeleteLocalRef(env, module_path_str);
    }
    if (file_str) {
        (*env)->DeleteLocalRef(env, file_str);
    }
}

JNIEXPORT jlong JNICALL Java_com_bifrore_BifroRE_nativeCreateWithRules(JNIEnv *env, jclass cls, jstring path) {
    (void)cls;
    if (path == NULL) {
        return 0;
    }
    const char *path_str = (*env)->GetStringUTFChars(env, path, NULL);
    void *engine = bre_create_with_rules(path_str);
    (*env)->ReleaseStringUTFChars(env, path, path_str);
    return (jlong)engine;
}

JNIEXPORT void JNICALL Java_com_bifrore_BifroRE_nativeDestroy(JNIEnv *env, jclass cls, jlong handle) {
    (void)env;
    (void)cls;
    if (handle != 0) {
        bre_destroy((void *)handle);
    }
}

JNIEXPORT jint JNICALL Java_com_bifrore_BifroRE_nativeStartMqtt(
    JNIEnv *env,
    jclass cls,
    jlong handle,
    jstring host,
    jint port,
    jstring client_id,
    jstring username,
    jstring password,
    jboolean clean_start,
    jint session_expiry_interval,
    jstring group_name,
    jboolean ordered,
    jstring ordered_prefix,
    jint keep_alive_secs,
    jlong cb_handle) {
    (void)cls;
    if (handle == 0 || host == NULL || client_id == NULL || group_name == NULL || ordered_prefix == NULL) {
        return -1;
    }
    if (cb_handle == 0) {
        return -2;
    }

    const char *host_str = (*env)->GetStringUTFChars(env, host, NULL);
    const char *client_id_str = (*env)->GetStringUTFChars(env, client_id, NULL);
    const char *group_name_str = (*env)->GetStringUTFChars(env, group_name, NULL);
    const char *ordered_prefix_str = (*env)->GetStringUTFChars(env, ordered_prefix, NULL);

    const char *username_str = NULL;
    const char *password_str = NULL;
    if (username != NULL) {
        username_str = (*env)->GetStringUTFChars(env, username, NULL);
    }
    if (password != NULL) {
        password_str = (*env)->GetStringUTFChars(env, password, NULL);
    }

    int rc = bre_start_mqtt(
        (void *)handle,
        host_str,
        (uint16_t)port,
        client_id_str,
        username_str,
        password_str,
        clean_start,
        (uint32_t)session_expiry_interval,
        group_name_str,
        ordered,
        ordered_prefix_str,
        (uint16_t)keep_alive_secs,
        call_java_handler,
        (void *)cb_handle);

    (*env)->ReleaseStringUTFChars(env, host, host_str);
    (*env)->ReleaseStringUTFChars(env, client_id, client_id_str);
    (*env)->ReleaseStringUTFChars(env, group_name, group_name_str);
    (*env)->ReleaseStringUTFChars(env, ordered_prefix, ordered_prefix_str);

    if (username_str != NULL) {
        (*env)->ReleaseStringUTFChars(env, username, username_str);
    }
    if (password_str != NULL) {
        (*env)->ReleaseStringUTFChars(env, password, password_str);
    }

    return rc;
}

JNIEXPORT jint JNICALL Java_com_bifrore_BifroRE_nativeStopMqtt(JNIEnv *env, jclass cls, jlong handle) {
    (void)env;
    (void)cls;
    if (handle == 0) {
        return -1;
    }
    return bre_stop_mqtt((void *)handle);
}

JNIEXPORT jlong JNICALL Java_com_bifrore_BifroRE_nativeRegisterHandler(
    JNIEnv *env,
    jclass cls,
    jobject handler) {
    (void)cls;
    if (handler == NULL) {
        return 0;
    }

    JavaVM *jvm = NULL;
    if ((*env)->GetJavaVM(env, &jvm) != 0) {
        return 0;
    }

    jclass handler_cls = (*env)->GetObjectClass(env, handler);
    if (handler_cls == NULL) {
        return 0;
    }

    jmethodID on_message = (*env)->GetMethodID(
        env,
        handler_cls,
        "onMessage",
        "(Ljava/lang/String;[BLjava/lang/String;)V");
    if (on_message == NULL) {
        return 0;
    }

    struct CallbackCtx *ctx = (struct CallbackCtx *)calloc(1, sizeof(struct CallbackCtx));
    if (ctx == NULL) {
        return 0;
    }
    ctx->jvm = jvm;
    ctx->handler = (*env)->NewGlobalRef(env, handler);
    ctx->on_message = on_message;

    return (jlong)ctx;
}

JNIEXPORT void JNICALL Java_com_bifrore_BifroRE_nativeFreeHandler(JNIEnv *env, jclass cls, jlong cb_handle) {
    (void)cls;
    if (cb_handle == 0) {
        return;
    }
    struct CallbackCtx *ctx = (struct CallbackCtx *)cb_handle;
    if (ctx->handler != NULL) {
        (*env)->DeleteGlobalRef(env, ctx->handler);
    }
    free(ctx);
}

JNIEXPORT jlong JNICALL Java_com_bifrore_BifroRE_nativeRegisterLogHandler(
    JNIEnv *env,
    jclass cls,
    jobject handler) {
    (void)cls;
    if (handler == NULL) {
        return 0;
    }

    JavaVM *jvm = NULL;
    if ((*env)->GetJavaVM(env, &jvm) != 0) {
        return 0;
    }

    jclass handler_cls = (*env)->GetObjectClass(env, handler);
    if (handler_cls == NULL) {
        return 0;
    }

    jmethodID on_log = (*env)->GetMethodID(
        env,
        handler_cls,
        "onLog",
        "(ILjava/lang/String;Ljava/lang/String;JLjava/lang/String;Ljava/lang/String;Ljava/lang/String;I)V");
    if (on_log == NULL) {
        return 0;
    }

    struct LogCallbackCtx *ctx = (struct LogCallbackCtx *)calloc(1, sizeof(struct LogCallbackCtx));
    if (ctx == NULL) {
        return 0;
    }
    ctx->jvm = jvm;
    ctx->handler = (*env)->NewGlobalRef(env, handler);
    ctx->on_log = on_log;
    return (jlong)ctx;
}

JNIEXPORT void JNICALL Java_com_bifrore_BifroRE_nativeFreeLogHandler(JNIEnv *env, jclass cls, jlong cb_handle) {
    (void)cls;
    if (cb_handle == 0) {
        return;
    }
    struct LogCallbackCtx *ctx = (struct LogCallbackCtx *)cb_handle;
    if (ctx->handler != NULL) {
        (*env)->DeleteGlobalRef(env, ctx->handler);
    }
    free(ctx);
}

JNIEXPORT jint JNICALL Java_com_bifrore_BifroRE_nativeSetLogCallback(
    JNIEnv *env,
    jclass cls,
    jlong cb_handle,
    jint min_level) {
    (void)env;
    (void)cls;
    if (cb_handle == 0) {
        return bre_set_log_callback(NULL, NULL, (int)min_level);
    }
    return bre_set_log_callback(call_java_log_handler, (void *)cb_handle, (int)min_level);
}
