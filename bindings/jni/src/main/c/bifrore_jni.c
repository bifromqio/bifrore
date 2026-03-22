#include <jni.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct BifroEvalResult {
    uint16_t rule_index;
    unsigned char *payload;
    size_t payload_len;
};

struct BifroPackedEvalResults {
    uint16_t *rule_indices;
    uint32_t *payload_offsets;
    uint32_t *payload_lengths;
    unsigned char *payload_data;
    size_t len;
    size_t payload_data_len;
};

struct BifroRuleMetadata {
    uint16_t rule_index;
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
extern int bre_poll_eval_results_packed(
    void *engine,
    uint32_t timeout_millis,
    struct BifroPackedEvalResults *out_results);
extern void bre_free_packed_eval_results(struct BifroPackedEvalResults *results);
extern int bre_get_rule_metadata_table(
    void *engine,
    struct BifroRuleMetadata **out_metadata,
    size_t *out_len);
extern void bre_free_rule_metadata_table(struct BifroRuleMetadata *metadata, size_t len);
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
extern int bre_set_poll_batch_limit(void *engine, uint32_t limit);

struct LogCallbackCtx {
    JavaVM *jvm;
    jobject handler;
    jmethodID on_log;
};

struct JniClassCache {
    jclass poll_batch_cls;
    jmethodID poll_batch_ctor;
    jclass rule_metadata_cls;
    jmethodID rule_metadata_ctor;
    jclass metrics_snapshot_cls;
    jmethodID metrics_snapshot_ctor;
};

static struct JniClassCache JNI_CACHE = {0};
static JavaVM *JNI_CACHE_JVM = NULL;
static pthread_once_t JNI_CACHE_ONCE = PTHREAD_ONCE_INIT;
static int JNI_CACHE_INIT_OK = 0;

static void init_jni_cache_once(void) {
    if (JNI_CACHE_JVM == NULL) {
        return;
    }

    JNIEnv *env = NULL;
    if ((*JNI_CACHE_JVM)->AttachCurrentThread(JNI_CACHE_JVM, (void **)&env, NULL) != 0) {
        return;
    }

    jclass local = (*env)->FindClass(env, "com/bifrore/BifroRE$PollBatch");
    if (local == NULL) {
        return;
    }
    JNI_CACHE.poll_batch_cls = (*env)->NewGlobalRef(env, local);
    (*env)->DeleteLocalRef(env, local);
    if (JNI_CACHE.poll_batch_cls == NULL) {
        return;
    }
    JNI_CACHE.poll_batch_ctor = (*env)->GetMethodID(
        env,
        JNI_CACHE.poll_batch_cls,
        "<init>",
        "([I[I[I[B)V"
    );
    if (JNI_CACHE.poll_batch_ctor == NULL) {
        return;
    }

    local = (*env)->FindClass(env, "com/bifrore/BifroRE$RuleMetadata");
    if (local == NULL) {
        return;
    }
    JNI_CACHE.rule_metadata_cls = (*env)->NewGlobalRef(env, local);
    (*env)->DeleteLocalRef(env, local);
    if (JNI_CACHE.rule_metadata_cls == NULL) {
        return;
    }
    JNI_CACHE.rule_metadata_ctor = (*env)->GetMethodID(
        env,
        JNI_CACHE.rule_metadata_cls,
        "<init>",
        "(ILjava/lang/String;)V"
    );
    if (JNI_CACHE.rule_metadata_ctor == NULL) {
        return;
    }

    local = (*env)->FindClass(env, "com/bifrore/BifroRE$MetricsSnapshot");
    if (local == NULL) {
        return;
    }
    JNI_CACHE.metrics_snapshot_cls = (*env)->NewGlobalRef(env, local);
    (*env)->DeleteLocalRef(env, local);
    if (JNI_CACHE.metrics_snapshot_cls == NULL) {
        return;
    }
    JNI_CACHE.metrics_snapshot_ctor = (*env)->GetMethodID(
        env,
        JNI_CACHE.metrics_snapshot_cls,
        "<init>",
        "(JJJJ)V"
    );
    if (JNI_CACHE.metrics_snapshot_ctor == NULL) {
        return;
    }

    JNI_CACHE_INIT_OK = 1;
}

static int ensure_jni_cache(JNIEnv *env) {
    if (JNI_CACHE_INIT_OK) {
        return 1;
    }
    if (JNI_CACHE_JVM == NULL) {
        if ((*env)->GetJavaVM(env, &JNI_CACHE_JVM) != 0) {
            return 0;
        }
    }
    pthread_once(&JNI_CACHE_ONCE, init_jni_cache_once);
    return JNI_CACHE_INIT_OK;
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

JNIEXPORT jobject JNICALL Java_com_bifrore_BifroRE_nativePollResultsBatch(
    JNIEnv *env,
    jclass cls,
    jlong handle,
    jint timeout_millis) {
    (void)cls;
    if (handle == 0) {
        return NULL;
    }

    struct BifroPackedEvalResults results = {0};
    int rc = bre_poll_eval_results_packed(
        (void *)handle,
        (uint32_t)timeout_millis,
        &results);
    if (rc <= 0 || results.len == 0) {
        return NULL;
    }
    if (!ensure_jni_cache(env)) {
        bre_free_packed_eval_results(&results);
        return NULL;
    }
    if (results.len > (size_t)INT32_MAX || results.payload_data_len > (size_t)INT32_MAX) {
        bre_free_packed_eval_results(&results);
        return NULL;
    }

    jintArray rule_indexes = (*env)->NewIntArray(env, (jsize)results.len);
    jintArray payload_offsets = (*env)->NewIntArray(env, (jsize)results.len);
    jintArray payload_lengths = (*env)->NewIntArray(env, (jsize)results.len);
    jbyteArray payload_data = (*env)->NewByteArray(env, (jsize)results.payload_data_len);
    if (rule_indexes == NULL || payload_offsets == NULL || payload_lengths == NULL || payload_data == NULL) {
        if (rule_indexes != NULL) {
            (*env)->DeleteLocalRef(env, rule_indexes);
        }
        if (payload_offsets != NULL) {
            (*env)->DeleteLocalRef(env, payload_offsets);
        }
        if (payload_lengths != NULL) {
            (*env)->DeleteLocalRef(env, payload_lengths);
        }
        if (payload_data != NULL) {
            (*env)->DeleteLocalRef(env, payload_data);
        }
        bre_free_packed_eval_results(&results);
        return NULL;
    }

    jint *rule_index_buffer = (jint *)calloc(results.len, sizeof(jint));
    jint *offset_buffer = (jint *)calloc(results.len, sizeof(jint));
    jint *length_buffer = (jint *)calloc(results.len, sizeof(jint));
    if (rule_index_buffer == NULL || offset_buffer == NULL || length_buffer == NULL) {
        free(rule_index_buffer);
        free(offset_buffer);
        free(length_buffer);
        (*env)->DeleteLocalRef(env, rule_indexes);
        (*env)->DeleteLocalRef(env, payload_offsets);
        (*env)->DeleteLocalRef(env, payload_lengths);
        (*env)->DeleteLocalRef(env, payload_data);
        bre_free_packed_eval_results(&results);
        return NULL;
    }

    for (size_t i = 0; i < results.len; i++) {
        rule_index_buffer[i] = (jint)results.rule_indices[i];
        offset_buffer[i] = (jint)results.payload_offsets[i];
        length_buffer[i] = (jint)results.payload_lengths[i];
    }
    (*env)->SetIntArrayRegion(env, rule_indexes, 0, (jsize)results.len, rule_index_buffer);
    (*env)->SetIntArrayRegion(env, payload_offsets, 0, (jsize)results.len, offset_buffer);
    (*env)->SetIntArrayRegion(env, payload_lengths, 0, (jsize)results.len, length_buffer);
    if (results.payload_data_len > 0) {
        (*env)->SetByteArrayRegion(
            env,
            payload_data,
            0,
            (jsize)results.payload_data_len,
            (const jbyte *)results.payload_data
        );
    }
    free(rule_index_buffer);
    free(offset_buffer);
    free(length_buffer);

    jobject batch = (*env)->NewObject(
        env,
        JNI_CACHE.poll_batch_cls,
        JNI_CACHE.poll_batch_ctor,
        rule_indexes,
        payload_offsets,
        payload_lengths,
        payload_data
    );
    (*env)->DeleteLocalRef(env, rule_indexes);
    (*env)->DeleteLocalRef(env, payload_offsets);
    (*env)->DeleteLocalRef(env, payload_lengths);
    (*env)->DeleteLocalRef(env, payload_data);
    bre_free_packed_eval_results(&results);
    return batch;
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
    if (!ensure_jni_cache(env)) {
        return NULL;
    }
    return (*env)->NewObject(
        env,
        JNI_CACHE.metrics_snapshot_cls,
        JNI_CACHE.metrics_snapshot_ctor,
        (jlong)eval_count,
        (jlong)eval_error_count,
        (jlong)eval_total_nanos,
        (jlong)eval_max_nanos);
}

JNIEXPORT jint JNICALL Java_com_bifrore_BifroRE_nativeSetPollBatchLimit(
    JNIEnv *env,
    jclass cls,
    jlong handle,
    jint limit) {
    (void)env;
    (void)cls;
    if (handle == 0) {
        return -1;
    }
    return bre_set_poll_batch_limit((void *)handle, (uint32_t)limit);
}

JNIEXPORT jobjectArray JNICALL Java_com_bifrore_BifroRE_nativeGetRuleMetadataTable(
    JNIEnv *env,
    jclass cls,
    jlong handle) {
    (void)cls;
    if (handle == 0) {
        return NULL;
    }

    struct BifroRuleMetadata *metadata = NULL;
    size_t metadata_len = 0;
    if (bre_get_rule_metadata_table((void *)handle, &metadata, &metadata_len) != 0) {
        return NULL;
    }
    if (!ensure_jni_cache(env)) {
        bre_free_rule_metadata_table(metadata, metadata_len);
        return NULL;
    }
    jobjectArray array = (*env)->NewObjectArray(env, (jsize)metadata_len, JNI_CACHE.rule_metadata_cls, NULL);
    if (array == NULL) {
        bre_free_rule_metadata_table(metadata, metadata_len);
        return NULL;
    }
    for (size_t i = 0; i < metadata_len; i++) {
        struct BifroRuleMetadata *record = &metadata[i];
        jstring destinations = (*env)->NewStringUTF(env, record->destinations_json ? record->destinations_json : "[]");
        jobject obj = (*env)->NewObject(env, JNI_CACHE.rule_metadata_cls, JNI_CACHE.rule_metadata_ctor, (jint)record->rule_index, destinations);
        if (obj != NULL) {
            (*env)->SetObjectArrayElement(env, array, (jsize)i, obj);
            (*env)->DeleteLocalRef(env, obj);
        }
        if (destinations) {
            (*env)->DeleteLocalRef(env, destinations);
        }
    }
    bre_free_rule_metadata_table(metadata, metadata_len);
    return array;
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
