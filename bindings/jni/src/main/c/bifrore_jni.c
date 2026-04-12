#include <jni.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct BifroPackedEvalResults {
    uint32_t *rule_indices;
    uint32_t *payload_offsets;
    uint32_t *payload_lengths;
    unsigned char *payload_data;
    size_t len;
    size_t payload_data_len;
};

struct BifroRuleMetadata {
    uint16_t rule_index;
    char **destinations;
    size_t destinations_len;
};

struct BifroREMetricsSnapshot {
    uint64_t ingress_message_count;
    uint64_t core_queue_depth;
    uint64_t core_queue_depth_max;
    uint64_t core_queue_drop_count;
    uint64_t ffi_queue_depth;
    uint64_t ffi_queue_depth_max;
    uint64_t ffi_queue_drop_count;
    uint64_t eval_count;
    uint64_t eval_error_count;
    uint64_t eval_total_total_nanos;
    uint64_t eval_total_max_nanos;
    uint64_t topic_match_count;
    uint64_t topic_match_total_nanos;
    uint64_t topic_match_max_nanos;
    uint64_t payload_decode_count;
    uint64_t payload_decode_total_nanos;
    uint64_t payload_decode_max_nanos;
    uint64_t msg_ir_build_count;
    uint64_t msg_ir_build_total_nanos;
    uint64_t msg_ir_build_max_nanos;
    uint64_t fast_where_count;
    uint64_t fast_where_total_nanos;
    uint64_t fast_where_max_nanos;
    uint64_t predicate_count;
    uint64_t predicate_total_nanos;
    uint64_t predicate_max_nanos;
    uint64_t projection_count;
    uint64_t projection_total_nanos;
    uint64_t projection_max_nanos;
};

// FFI functions from Rust cdylib
extern void *bre_create_engine(
    const char *path,
    int payload_format,
    const char *client_ids_path,
    int notify_mode,
    const char *protobuf_descriptor_set_path,
    const char *protobuf_message_name);
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
    jboolean multi_nci);
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
    struct BifroREMetricsSnapshot *out_snapshot);
extern int bre_set_poll_batch_limit(void *engine, uint32_t limit);
extern int bre_set_detailed_latency_metrics(void *engine, jboolean enabled);

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
};

struct DirectPendingBatch {
    struct BifroPackedEvalResults results;
    size_t next_index;
    size_t remaining_messages;
    size_t remaining_payload_bytes;
};

static struct JniClassCache JNI_CACHE = {0};
static JavaVM *JNI_CACHE_JVM = NULL;
static pthread_once_t JNI_CACHE_ONCE = PTHREAD_ONCE_INIT;
static int JNI_CACHE_INIT_OK = 0;
static struct DirectPendingBatch DIRECT_PENDING = {0};
static int DIRECT_PENDING_ACTIVE = 0;
static const size_t DIRECT_MERGE_PAYLOAD_NUM = 1;
static const size_t DIRECT_MERGE_PAYLOAD_DEN = 4;
static const size_t DIRECT_MERGE_HEADER_NUM = 1;
static const size_t DIRECT_MERGE_HEADER_DEN = 4;

static struct DirectPendingBatch *find_pending_batch(void) {
    if (DIRECT_PENDING_ACTIVE) {
        return &DIRECT_PENDING;
    }
    return NULL;
}

static void clear_pending_batch(void) {
    if (DIRECT_PENDING_ACTIVE) {
        bre_free_packed_eval_results(&DIRECT_PENDING.results);
        memset(&DIRECT_PENDING, 0, sizeof(DIRECT_PENDING));
        DIRECT_PENDING_ACTIVE = 0;
    }
}

static struct DirectPendingBatch *store_pending_batch(
    struct BifroPackedEvalResults *results) {
    clear_pending_batch();
    DIRECT_PENDING.results = *results;
    DIRECT_PENDING.next_index = 0;
    DIRECT_PENDING.remaining_messages = results->len;
    DIRECT_PENDING.remaining_payload_bytes = results->payload_data_len;
    DIRECT_PENDING_ACTIVE = 1;
    results->rule_indices = NULL;
    results->payload_offsets = NULL;
    results->payload_lengths = NULL;
    results->payload_data = NULL;
    results->len = 0;
    results->payload_data_len = 0;
    return &DIRECT_PENDING;
}

static struct DirectPendingBatch *store_pending_batch_with_index(
    struct BifroPackedEvalResults *results,
    size_t next_index) {
    clear_pending_batch();
    DIRECT_PENDING.results = *results;
    DIRECT_PENDING.next_index = next_index;
    DIRECT_PENDING.remaining_messages = next_index >= results->len ? 0 : results->len - next_index;
    DIRECT_PENDING.remaining_payload_bytes = next_index >= results->len
        ? 0
        : results->payload_data_len - (size_t)results->payload_offsets[next_index];
    DIRECT_PENDING_ACTIVE = 1;
    results->rule_indices = NULL;
    results->payload_offsets = NULL;
    results->payload_lengths = NULL;
    results->payload_data = NULL;
    results->len = 0;
    results->payload_data_len = 0;
    return &DIRECT_PENDING;
}

static int should_merge_pending_batch(
    const struct DirectPendingBatch *batch,
    size_t header_capacity_records,
    size_t payload_capacity_bytes) {
    return batch->remaining_messages > 0
        && batch->remaining_payload_bytes * DIRECT_MERGE_PAYLOAD_DEN < payload_capacity_bytes * DIRECT_MERGE_PAYLOAD_NUM
        && batch->remaining_messages * DIRECT_MERGE_HEADER_DEN < header_capacity_records * DIRECT_MERGE_HEADER_NUM;
}

static int copy_batch_to_direct_buffers(
    const struct BifroPackedEvalResults *results,
    size_t *next_index,
    uint32_t *header_ptr,
    size_t header_capacity_records,
    unsigned char *payload_ptr,
    size_t payload_capacity_bytes,
    size_t *emitted,
    size_t *payload_offset,
    size_t *copied_messages,
    size_t *copied_payload_bytes) {
    if (*next_index >= results->len) {
        return 0;
    }
    if (header_capacity_records == 0) {
        return -4;
    }
    size_t first_length = (size_t)results->payload_lengths[*next_index];
    if (*emitted == 0 && *payload_offset == 0 && first_length > payload_capacity_bytes) {
        return -5;
    }

    while (*next_index < results->len && *emitted < header_capacity_records) {
        size_t idx = *next_index;
        size_t payload_length = (size_t)results->payload_lengths[idx];
        if (*payload_offset + payload_length > payload_capacity_bytes) {
            break;
        }
        header_ptr[*emitted * 3] = results->rule_indices[idx];
        header_ptr[*emitted * 3 + 1] = (uint32_t)(*payload_offset);
        header_ptr[*emitted * 3 + 2] = results->payload_lengths[idx];
        if (payload_length > 0) {
            memcpy(
                payload_ptr + *payload_offset,
                results->payload_data + results->payload_offsets[idx],
                payload_length
            );
        }
        *payload_offset += payload_length;
        *emitted += 1;
        *next_index += 1;
        if (copied_messages != NULL) {
            *copied_messages += 1;
        }
        if (copied_payload_bytes != NULL) {
            *copied_payload_bytes += payload_length;
        }
    }
    return 0;
}

static int poll_results_batch_direct_core(
    void *engine,
    uint32_t timeout_millis,
    uint32_t *header_ptr,
    size_t header_capacity_records,
    unsigned char *payload_ptr,
    size_t payload_capacity_bytes) {
    struct DirectPendingBatch *pending = find_pending_batch();
    if (pending != NULL) {
        int merge_pending = should_merge_pending_batch(
            pending,
            header_capacity_records,
            payload_capacity_bytes
        );
        size_t emitted = 0;
        size_t payload_offset = 0;
        size_t copied_messages = 0;
        size_t copied_payload_bytes = 0;
        int rc = copy_batch_to_direct_buffers(
            &pending->results,
            &pending->next_index,
            header_ptr,
            header_capacity_records,
            payload_ptr,
            payload_capacity_bytes,
            &emitted,
            &payload_offset,
            &copied_messages,
            &copied_payload_bytes
        );
        if (rc < 0) {
            return rc;
        }
        pending->remaining_messages -= copied_messages;
        pending->remaining_payload_bytes -= copied_payload_bytes;
        if (pending->next_index >= pending->results.len) {
            clear_pending_batch();
        }
        if (merge_pending && emitted < header_capacity_records && payload_offset < payload_capacity_bytes) {
            struct BifroPackedEvalResults fetched = {0};
            int fetch_rc = bre_poll_eval_results_packed(engine, 0, &fetched);
            if (fetch_rc > 0 && fetched.len > 0) {
                size_t fetched_index = 0;
                copied_messages = 0;
                copied_payload_bytes = 0;
                rc = copy_batch_to_direct_buffers(
                    &fetched,
                    &fetched_index,
                    header_ptr,
                    header_capacity_records,
                    payload_ptr,
                    payload_capacity_bytes,
                    &emitted,
                    &payload_offset,
                    &copied_messages,
                    &copied_payload_bytes
                );
                if (rc < 0 && emitted == 0) {
                    bre_free_packed_eval_results(&fetched);
                    return rc;
                }
                if (fetched_index < fetched.len) {
                    store_pending_batch_with_index(&fetched, fetched_index);
                } else {
                    bre_free_packed_eval_results(&fetched);
                }
            } else if (fetched.len > 0) {
                bre_free_packed_eval_results(&fetched);
            }
        }
        return (int)emitted;
    }

    struct BifroPackedEvalResults fetched = {0};
    int rc = bre_poll_eval_results_packed(engine, timeout_millis, &fetched);
    if (rc <= 0 || fetched.len == 0) {
        if (fetched.len > 0) {
            bre_free_packed_eval_results(&fetched);
        }
        return rc;
    }

    size_t fetched_index = 0;
    size_t emitted = 0;
    size_t payload_offset = 0;
    size_t copied_messages = 0;
    size_t copied_payload_bytes = 0;
    rc = copy_batch_to_direct_buffers(
        &fetched,
        &fetched_index,
        header_ptr,
        header_capacity_records,
        payload_ptr,
        payload_capacity_bytes,
        &emitted,
        &payload_offset,
        &copied_messages,
        &copied_payload_bytes
    );
    if (rc < 0) {
        bre_free_packed_eval_results(&fetched);
        return rc;
    }
    if (fetched_index < fetched.len) {
        if (store_pending_batch_with_index(&fetched, fetched_index) == NULL) {
            bre_free_packed_eval_results(&fetched);
            return -1;
        }
    } else {
        bre_free_packed_eval_results(&fetched);
    }
    return (int)emitted;
}

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
        "(I[Ljava/lang/String;)V"
    );
    if (JNI_CACHE.rule_metadata_ctor == NULL) {
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

JNIEXPORT jlong JNICALL Java_com_bifrore_BifroRE_nativeCreateEngine(
    JNIEnv *env,
    jclass cls,
    jstring path,
    jint payload_format,
    jstring client_ids_path,
    jint notify_mode,
    jstring protobuf_descriptor_set_path,
    jstring protobuf_message_name);

JNIEXPORT jlong JNICALL Java_com_bifrore_BifroRE_nativeCreateEngine(
    JNIEnv *env,
    jclass cls,
    jstring path,
    jint payload_format,
    jstring client_ids_path,
    jint notify_mode,
    jstring protobuf_descriptor_set_path,
    jstring protobuf_message_name) {
    (void)cls;
    if (path == NULL) {
        return 0;
    }
    const char *path_str = (*env)->GetStringUTFChars(env, path, NULL);
    const char *client_ids_path_str = NULL;
    const char *protobuf_descriptor_set_path_str = NULL;
    const char *protobuf_message_name_str = NULL;
    if (client_ids_path != NULL) {
        client_ids_path_str = (*env)->GetStringUTFChars(env, client_ids_path, NULL);
    }
    if (protobuf_descriptor_set_path != NULL) {
        protobuf_descriptor_set_path_str =
            (*env)->GetStringUTFChars(env, protobuf_descriptor_set_path, NULL);
    }
    if (protobuf_message_name != NULL) {
        protobuf_message_name_str = (*env)->GetStringUTFChars(env, protobuf_message_name, NULL);
    }
    void *engine = bre_create_engine(
        path_str,
        (int)payload_format,
        client_ids_path_str,
        (int)notify_mode,
        protobuf_descriptor_set_path_str,
        protobuf_message_name_str);
    (*env)->ReleaseStringUTFChars(env, path, path_str);
    if (client_ids_path_str != NULL) {
        (*env)->ReleaseStringUTFChars(env, client_ids_path, client_ids_path_str);
    }
    if (protobuf_descriptor_set_path_str != NULL) {
        (*env)->ReleaseStringUTFChars(
            env, protobuf_descriptor_set_path, protobuf_descriptor_set_path_str);
    }
    if (protobuf_message_name_str != NULL) {
        (*env)->ReleaseStringUTFChars(env, protobuf_message_name, protobuf_message_name_str);
    }
    return (jlong)engine;
}

JNIEXPORT void JNICALL Java_com_bifrore_BifroRE_nativeDestroy(JNIEnv *env, jclass cls, jlong handle) {
    (void)env;
    (void)cls;
    if (handle != 0) {
        clear_pending_batch();
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
    jboolean multi_nci) {
    (void)cls;
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
        multi_nci);

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

    (*env)->SetIntArrayRegion(env, rule_indexes, 0, (jsize)results.len, (const jint *)results.rule_indices);
    (*env)->SetIntArrayRegion(env, payload_offsets, 0, (jsize)results.len, (const jint *)results.payload_offsets);
    (*env)->SetIntArrayRegion(env, payload_lengths, 0, (jsize)results.len, (const jint *)results.payload_lengths);
    if (results.payload_data_len > 0) {
        (*env)->SetByteArrayRegion(
            env,
            payload_data,
            0,
            (jsize)results.payload_data_len,
            (const jbyte *)results.payload_data
        );
    }

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

JNIEXPORT jint JNICALL Java_com_bifrore_BifroRE_nativePollResultsBatchDirect(
    JNIEnv *env,
    jclass cls,
    jlong handle,
    jint timeout_millis,
    jobject header_buffer,
    jint header_capacity_ints,
    jobject payload_buffer,
    jint payload_capacity_bytes) {
    (void)cls;
    if (handle == 0 || header_buffer == NULL || payload_buffer == NULL) {
        return -1;
    }

    uint32_t *header_ptr = (uint32_t *)(*env)->GetDirectBufferAddress(env, header_buffer);
    unsigned char *payload_ptr = (unsigned char *)(*env)->GetDirectBufferAddress(env, payload_buffer);
    if (header_ptr == NULL || payload_ptr == NULL || header_capacity_ints <= 0 || payload_capacity_bytes <= 0) {
        return -1;
    }
    size_t header_capacity_records = (size_t)header_capacity_ints / 3;
    return poll_results_batch_direct_core(
        (void *)handle,
        (uint32_t)timeout_millis,
        header_ptr,
        header_capacity_records,
        payload_ptr,
        (size_t)payload_capacity_bytes
    );
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

JNIEXPORT jlongArray JNICALL Java_com_bifrore_BifroRE_nativeMetricsSnapshotValues(
    JNIEnv *env,
    jclass cls,
    jlong handle) {
    (void)cls;
    if (handle == 0) {
        return NULL;
    }

    struct BifroREMetricsSnapshot snapshot;
    if (bre_metrics_snapshot(
            (void *)handle,
            &snapshot) != 0) {
        return NULL;
    }
    jlong values[] = {
        (jlong)snapshot.ingress_message_count,
        (jlong)snapshot.core_queue_depth,
        (jlong)snapshot.core_queue_depth_max,
        (jlong)snapshot.core_queue_drop_count,
        (jlong)snapshot.ffi_queue_depth,
        (jlong)snapshot.ffi_queue_depth_max,
        (jlong)snapshot.ffi_queue_drop_count,
        (jlong)snapshot.eval_count,
        (jlong)snapshot.eval_error_count,
        (jlong)snapshot.eval_total_total_nanos,
        (jlong)snapshot.eval_total_max_nanos,
        (jlong)snapshot.topic_match_count,
        (jlong)snapshot.topic_match_total_nanos,
        (jlong)snapshot.topic_match_max_nanos,
        (jlong)snapshot.payload_decode_count,
        (jlong)snapshot.payload_decode_total_nanos,
        (jlong)snapshot.payload_decode_max_nanos,
        (jlong)snapshot.msg_ir_build_count,
        (jlong)snapshot.msg_ir_build_total_nanos,
        (jlong)snapshot.msg_ir_build_max_nanos,
        (jlong)snapshot.fast_where_count,
        (jlong)snapshot.fast_where_total_nanos,
        (jlong)snapshot.fast_where_max_nanos,
        (jlong)snapshot.predicate_count,
        (jlong)snapshot.predicate_total_nanos,
        (jlong)snapshot.predicate_max_nanos,
        (jlong)snapshot.projection_count,
        (jlong)snapshot.projection_total_nanos,
        (jlong)snapshot.projection_max_nanos,
    };
    jsize value_count = (jsize)(sizeof(values) / sizeof(values[0]));
    jlongArray array = (*env)->NewLongArray(env, value_count);
    if (array == NULL) {
        return NULL;
    }
    (*env)->SetLongArrayRegion(env, array, 0, value_count, values);
    return array;
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

JNIEXPORT jint JNICALL Java_com_bifrore_BifroRE_nativeSetDetailedLatencyMetrics(
    JNIEnv *env,
    jclass cls,
    jlong handle,
    jboolean enabled) {
    (void)env;
    (void)cls;
    if (handle == 0) {
        return -1;
    }
    return bre_set_detailed_latency_metrics((void *)handle, enabled);
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
    jclass string_cls = (*env)->FindClass(env, "java/lang/String");
    if (string_cls == NULL) {
        bre_free_rule_metadata_table(metadata, metadata_len);
        return NULL;
    }
    for (size_t i = 0; i < metadata_len; i++) {
        struct BifroRuleMetadata *record = &metadata[i];
        jobjectArray destinations = (*env)->NewObjectArray(
            env,
            (jsize)record->destinations_len,
            string_cls,
            NULL
        );
        if (destinations != NULL) {
            for (size_t j = 0; j < record->destinations_len; j++) {
                jstring destination = (*env)->NewStringUTF(
                    env,
                    (record->destinations != NULL && record->destinations[j] != NULL)
                        ? record->destinations[j]
                        : ""
                );
                if (destination != NULL) {
                    (*env)->SetObjectArrayElement(env, destinations, (jsize)j, destination);
                    (*env)->DeleteLocalRef(env, destination);
                }
            }
        }
        jobject obj = (*env)->NewObject(
            env,
            JNI_CACHE.rule_metadata_cls,
            JNI_CACHE.rule_metadata_ctor,
            (jint)record->rule_index,
            destinations
        );
        if (obj != NULL) {
            (*env)->SetObjectArrayElement(env, array, (jsize)i, obj);
            (*env)->DeleteLocalRef(env, obj);
        }
        if (destinations) {
            (*env)->DeleteLocalRef(env, destinations);
        }
    }
    (*env)->DeleteLocalRef(env, string_cls);
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
