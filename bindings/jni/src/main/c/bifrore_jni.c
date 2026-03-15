#include <jni.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct BifroEvalResult {
    char *rule_id;
    unsigned char *payload;
    size_t payload_len;
    char *destinations_json;
};

// FFI functions from Rust cdylib
extern void *bre_create_with_config(const char *path);
extern void *bre_create_with_config_and_payload_format(const char *path, int payload_format);
extern void *bre_create_with_config_and_payload_format_and_client_ids_path(
    const char *path,
    int payload_format,
    const char *client_ids_path);
extern void bre_destroy(void *engine);
extern int bre_disconnect(void *engine);
extern int bre_start_mqtt(
    void *engine,
    const char *host,
    uint16_t port,
    const char *node_id,
    uint16_t client_count,
    const char *username,
    const char *password,
    jboolean clean_start,
    uint32_t session_expiry_interval,
    const char *group_name,
    jboolean ordered,
    const char *ordered_prefix,
    uint16_t keep_alive_secs,
    jboolean multi_nci,
    void (*callback)(void *, const char *, const unsigned char *, size_t, const char *),
    void *user_data);
extern int bre_poll_eval_results_batch(
    void *engine,
    uint32_t timeout_millis,
    struct BifroEvalResult **out_results,
    size_t *out_len);
extern void bre_free_eval_results_batch(struct BifroEvalResult *results, size_t len);
extern int bre_set_log_callback(
    void (*callback)(void *, int, const char *, const char *, uint64_t, const char *, const char *, const char *, uint32_t),
    void *user_data,
    int min_level);
extern int bre_metrics_snapshot(
    void *engine,
    uint64_t *eval_count,
    uint64_t *eval_error_count,
    uint64_t *eval_total_nanos,
    uint64_t *eval_max_nanos);

struct LogCallbackCtx {
    JavaVM *jvm;
    jobject handler;
    jmethodID on_log;
};

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

JNIEXPORT jlong JNICALL Java_com_bifrore_BifroRE_nativeCreateWithConfigAndPayloadFormat(
    JNIEnv *env,
    jclass cls,
    jstring path,
    jint payload_format);
JNIEXPORT jlong JNICALL Java_com_bifrore_BifroRE_nativeCreateWithConfigAndPayloadFormatAndClientIdsPath(
    JNIEnv *env,
    jclass cls,
    jstring path,
    jint payload_format,
    jstring client_ids_path);

JNIEXPORT jlong JNICALL Java_com_bifrore_BifroRE_nativeCreateWithConfig(JNIEnv *env, jclass cls, jstring path) {
    return Java_com_bifrore_BifroRE_nativeCreateWithConfigAndPayloadFormatAndClientIdsPath(
        env, cls, path, 1, NULL);
}

JNIEXPORT jlong JNICALL Java_com_bifrore_BifroRE_nativeCreateWithConfigAndPayloadFormat(
    JNIEnv *env,
    jclass cls,
    jstring path,
    jint payload_format) {
    return Java_com_bifrore_BifroRE_nativeCreateWithConfigAndPayloadFormatAndClientIdsPath(
        env, cls, path, payload_format, NULL);
}

JNIEXPORT jlong JNICALL Java_com_bifrore_BifroRE_nativeCreateWithConfigAndPayloadFormatAndClientIdsPath(
    JNIEnv *env,
    jclass cls,
    jstring path,
    jint payload_format,
    jstring client_ids_path) {
    (void)cls;
    if (path == NULL) {
        return 0;
    }
    const char *path_str = (*env)->GetStringUTFChars(env, path, NULL);
    const char *client_ids_path_str = NULL;
    if (client_ids_path != NULL) {
        client_ids_path_str = (*env)->GetStringUTFChars(env, client_ids_path, NULL);
    }
    void *engine = bre_create_with_config_and_payload_format_and_client_ids_path(
        path_str,
        (int)payload_format,
        client_ids_path_str);
    (*env)->ReleaseStringUTFChars(env, path, path_str);
    if (client_ids_path_str != NULL) {
        (*env)->ReleaseStringUTFChars(env, client_ids_path, client_ids_path_str);
    }
    return (jlong)engine;
}

JNIEXPORT void JNICALL Java_com_bifrore_BifroRE_nativeDestroy(JNIEnv *env, jclass cls, jlong handle) {
    (void)env;
    (void)cls;
    if (handle != 0) {
        bre_destroy((void *)handle);
    }
}

JNIEXPORT jint JNICALL Java_com_bifrore_BifroRE_nativeDisconnect(JNIEnv *env, jclass cls, jlong handle) {
    (void)env;
    (void)cls;
    if (handle == 0) {
        return -1;
    }
    return bre_disconnect((void *)handle);
}

JNIEXPORT jint JNICALL Java_com_bifrore_BifroRE_nativeStartMqtt(
    JNIEnv *env,
    jclass cls,
    jlong handle,
    jstring host,
    jint port,
    jstring node_id,
    jint client_count,
    jstring username,
    jstring password,
    jboolean clean_start,
    jint session_expiry_interval,
    jstring group_name,
    jboolean ordered,
    jstring ordered_prefix,
    jint keep_alive_secs,
    jboolean multi_nci,
    jlong cb_handle) {
    (void)cls;
    (void)cb_handle;
    if (handle == 0 || host == NULL || group_name == NULL || ordered_prefix == NULL) {
        return -1;
    }

    const char *host_str = (*env)->GetStringUTFChars(env, host, NULL);
    const char *group_name_str = (*env)->GetStringUTFChars(env, group_name, NULL);
    const char *ordered_prefix_str = (*env)->GetStringUTFChars(env, ordered_prefix, NULL);
    const char *node_id_str = NULL;
    if (node_id != NULL) {
        node_id_str = (*env)->GetStringUTFChars(env, node_id, NULL);
    }

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
        node_id_str,
        (uint16_t)client_count,
        username_str,
        password_str,
        clean_start,
        (uint32_t)session_expiry_interval,
        group_name_str,
        ordered,
        ordered_prefix_str,
        (uint16_t)keep_alive_secs,
        multi_nci,
        NULL,
        NULL);

    (*env)->ReleaseStringUTFChars(env, host, host_str);
    (*env)->ReleaseStringUTFChars(env, group_name, group_name_str);
    (*env)->ReleaseStringUTFChars(env, ordered_prefix, ordered_prefix_str);
    if (node_id_str != NULL) {
        (*env)->ReleaseStringUTFChars(env, node_id, node_id_str);
    }

    if (username_str != NULL) {
        (*env)->ReleaseStringUTFChars(env, username, username_str);
    }
    if (password_str != NULL) {
        (*env)->ReleaseStringUTFChars(env, password, password_str);
    }

    return rc;
}

JNIEXPORT jobjectArray JNICALL Java_com_bifrore_BifroRE_nativePollResultsBatch(
    JNIEnv *env,
    jclass cls,
    jlong handle,
    jint timeout_millis) {
    (void)cls;
    if (handle == 0) {
        return NULL;
    }

    struct BifroEvalResult *results = NULL;
    size_t result_len = 0;
    int rc = bre_poll_eval_results_batch(
        (void *)handle,
        (uint32_t)timeout_millis,
        &results,
        &result_len);
    if (rc <= 0 || results == NULL || result_len == 0) {
        return NULL;
    }

    jclass result_cls = (*env)->FindClass(env, "com/bifrore/BifroRE$EvalResult");
    if (result_cls == NULL) {
        bre_free_eval_results_batch(results, result_len);
        return NULL;
    }
    jmethodID ctor = (*env)->GetMethodID(env, result_cls, "<init>", "(Ljava/lang/String;[BLjava/lang/String;)V");
    if (ctor == NULL) {
        bre_free_eval_results_batch(results, result_len);
        return NULL;
    }
    jobjectArray array = (*env)->NewObjectArray(env, (jsize)result_len, result_cls, NULL);
    if (array == NULL) {
        bre_free_eval_results_batch(results, result_len);
        return NULL;
    }

    for (size_t i = 0; i < result_len; i++) {
        struct BifroEvalResult *result = &results[i];
        jstring rule_id = (*env)->NewStringUTF(env, result->rule_id ? result->rule_id : "");
        jbyteArray payload = (*env)->NewByteArray(env, (jsize)result->payload_len);
        if (payload != NULL && result->payload != NULL && result->payload_len > 0) {
            (*env)->SetByteArrayRegion(env, payload, 0, (jsize)result->payload_len, (const jbyte *)result->payload);
        }
        jstring destinations = (*env)->NewStringUTF(env, result->destinations_json ? result->destinations_json : "[]");
        jobject obj = (*env)->NewObject(env, result_cls, ctor, rule_id, payload, destinations);
        if (obj != NULL) {
            (*env)->SetObjectArrayElement(env, array, (jsize)i, obj);
            (*env)->DeleteLocalRef(env, obj);
        }
        if (rule_id) {
            (*env)->DeleteLocalRef(env, rule_id);
        }
        if (payload) {
            (*env)->DeleteLocalRef(env, payload);
        }
        if (destinations) {
            (*env)->DeleteLocalRef(env, destinations);
        }
    }

    bre_free_eval_results_batch(results, result_len);
    return array;
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

JNIEXPORT jobject JNICALL Java_com_bifrore_BifroRE_nativeMetricsSnapshot(
    JNIEnv *env,
    jclass cls,
    jlong handle) {
    (void)cls;
    if (handle == 0) {
        return NULL;
    }

    uint64_t eval_count = 0;
    uint64_t eval_error_count = 0;
    uint64_t eval_total_nanos = 0;
    uint64_t eval_max_nanos = 0;
    if (bre_metrics_snapshot(
            (void *)handle,
            &eval_count,
            &eval_error_count,
            &eval_total_nanos,
            &eval_max_nanos) != 0) {
        return NULL;
    }

    jclass snapshot_cls = (*env)->FindClass(env, "com/bifrore/BifroRE$MetricsSnapshot");
    if (snapshot_cls == NULL) {
        return NULL;
    }
    jmethodID ctor = (*env)->GetMethodID(env, snapshot_cls, "<init>", "(JJJJ)V");
    if (ctor == NULL) {
        return NULL;
    }
    return (*env)->NewObject(
        env,
        snapshot_cls,
        ctor,
        (jlong)eval_count,
        (jlong)eval_error_count,
        (jlong)eval_total_nanos,
        (jlong)eval_max_nanos);
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
