#include "bifrore_jni_common.h"

struct JniClassCache JNI_CACHE = {0};
JavaVM *JNI_CACHE_JVM = NULL;
pthread_once_t JNI_CACHE_ONCE = PTHREAD_ONCE_INIT;
int JNI_CACHE_INIT_OK = 0;

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
        "([I[B)V"
    );
    if (JNI_CACHE.poll_batch_ctor == NULL) {
        return;
    }

    local = (*env)->FindClass(env, "com/bifrore/BifroRE$PollResult");
    if (local == NULL) {
        return;
    }
    JNI_CACHE.poll_result_cls = (*env)->NewGlobalRef(env, local);
    (*env)->DeleteLocalRef(env, local);
    if (JNI_CACHE.poll_result_cls == NULL) {
        return;
    }
    JNI_CACHE.poll_result_ctor = (*env)->GetMethodID(
        env,
        JNI_CACHE.poll_result_cls,
        "<init>",
        "(ILcom/bifrore/BifroRE$PollBatch;)V"
    );
    if (JNI_CACHE.poll_result_ctor == NULL) {
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

int ensure_jni_cache(JNIEnv *env) {
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
    jstring protobuf_descriptor_set_path) {
    (void)cls;
    if (path == NULL) {
        return 0;
    }
    const char *path_str = (*env)->GetStringUTFChars(env, path, NULL);
    const char *client_ids_path_str = NULL;
    const char *protobuf_descriptor_set_path_str = NULL;
    if (client_ids_path != NULL) {
        client_ids_path_str = (*env)->GetStringUTFChars(env, client_ids_path, NULL);
    }
    if (protobuf_descriptor_set_path != NULL) {
        protobuf_descriptor_set_path_str =
            (*env)->GetStringUTFChars(env, protobuf_descriptor_set_path, NULL);
    }
    void *engine = bre_create_engine(
        path_str,
        (int)payload_format,
        client_ids_path_str,
        (int)notify_mode,
        protobuf_descriptor_set_path_str);
    (*env)->ReleaseStringUTFChars(env, path, path_str);
    if (client_ids_path_str != NULL) {
        (*env)->ReleaseStringUTFChars(env, client_ids_path, client_ids_path_str);
    }
    if (protobuf_descriptor_set_path_str != NULL) {
        (*env)->ReleaseStringUTFChars(
            env, protobuf_descriptor_set_path, protobuf_descriptor_set_path_str);
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
        return BRE_ERR_INVALID_ARGUMENT;
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
        return BRE_ERR_INVALID_ARGUMENT;
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

JNIEXPORT jint JNICALL Java_com_bifrore_BifroRE_nativeSetLogCallback(
    JNIEnv *env,
    jclass cls,
    jlong callback_handle,
    jint min_level) {
    (void)env;
    (void)cls;
    struct LogCallbackCtx *ctx = (struct LogCallbackCtx *)callback_handle;
    return bre_set_log_callback(
        ctx == NULL ? NULL : call_java_log_handler,
        ctx,
        (int)min_level
    );
}

JNIEXPORT void JNICALL Java_com_bifrore_BifroRE_nativeFreeLogHandler(
    JNIEnv *env,
    jclass cls,
    jlong callback_handle) {
    (void)cls;
    struct LogCallbackCtx *ctx = (struct LogCallbackCtx *)callback_handle;
    if (ctx == NULL) {
        return;
    }
    if (ctx->handler != NULL) {
        (*env)->DeleteGlobalRef(env, ctx->handler);
    }
    free(ctx);
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
    if (bre_metrics_snapshot((void *)handle, &snapshot) != 0) {
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
        (jlong)snapshot.message_pipeline_count,
        (jlong)snapshot.message_pipeline_total_nanos,
        (jlong)snapshot.message_pipeline_max_nanos,
        (jlong)snapshot.eval_count,
        (jlong)snapshot.eval_error_count,
        (jlong)snapshot.eval_type_error_count,
        (jlong)snapshot.payload_error_count,
        (jlong)snapshot.exec_count,
        (jlong)snapshot.exec_total_nanos,
        (jlong)snapshot.exec_max_nanos,
        (jlong)snapshot.topic_match_count,
        (jlong)snapshot.topic_match_total_nanos,
        (jlong)snapshot.topic_match_max_nanos,
        (jlong)snapshot.payload_decode_count,
        (jlong)snapshot.payload_decode_total_nanos,
        (jlong)snapshot.payload_decode_max_nanos,
        (jlong)snapshot.msg_ir_build_count,
        (jlong)snapshot.msg_ir_build_total_nanos,
        (jlong)snapshot.msg_ir_build_max_nanos,
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
        return BRE_ERR_INVALID_ARGUMENT;
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
        return BRE_ERR_INVALID_ARGUMENT;
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
        if (destinations != NULL) {
            (*env)->DeleteLocalRef(env, destinations);
        }
        if (obj != NULL) {
            (*env)->SetObjectArrayElement(env, array, (jsize)i, obj);
            (*env)->DeleteLocalRef(env, obj);
        }
    }
    bre_free_rule_metadata_table(metadata, metadata_len);
    return array;
}
