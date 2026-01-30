#include <jni.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// FFI functions from Rust cdylib
extern void *bre_create(void);
extern void bre_destroy(void *engine);
extern int bre_load_rules_from_json(void *engine, const char *path);
extern int bre_eval(
    void *engine,
    const char *topic,
    const unsigned char *payload,
    size_t payload_len,
    void (*callback)(void *, const char *, const unsigned char *, size_t, const char *),
    void *user_data);
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

struct CallbackCtx {
    JavaVM *jvm;
    jobject handler;
    jmethodID on_message;
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

JNIEXPORT jlong JNICALL Java_com_bifrore_BifroRE_nativeCreate(JNIEnv *env, jclass cls) {
    (void)env;
    (void)cls;
    return (jlong)bre_create();
}

JNIEXPORT void JNICALL Java_com_bifrore_BifroRE_nativeDestroy(JNIEnv *env, jclass cls, jlong handle) {
    (void)env;
    (void)cls;
    if (handle != 0) {
        bre_destroy((void *)handle);
    }
}

JNIEXPORT jint JNICALL Java_com_bifrore_BifroRE_nativeLoadRules(JNIEnv *env, jclass cls, jlong handle, jstring path) {
    (void)cls;
    if (handle == 0 || path == NULL) {
        return -1;
    }
    const char *path_str = (*env)->GetStringUTFChars(env, path, NULL);
    int rc = bre_load_rules_from_json((void *)handle, path_str);
    (*env)->ReleaseStringUTFChars(env, path, path_str);
    return rc;
}

JNIEXPORT jint JNICALL Java_com_bifrore_BifroRE_nativeEval(
    JNIEnv *env,
    jclass cls,
    jlong handle,
    jstring topic,
    jbyteArray payload,
    jlong cb_handle) {
    (void)cls;
    if (handle == 0 || topic == NULL || payload == NULL || cb_handle == 0) {
        return -1;
    }
    const char *topic_str = (*env)->GetStringUTFChars(env, topic, NULL);
    jsize payload_len = (*env)->GetArrayLength(env, payload);
    jbyte *payload_bytes = (*env)->GetByteArrayElements(env, payload, NULL);

    int rc = bre_eval(
        (void *)handle,
        topic_str,
        (const unsigned char *)payload_bytes,
        (size_t)payload_len,
        call_java_handler,
        (void *)cb_handle);

    (*env)->ReleaseByteArrayElements(env, payload, payload_bytes, JNI_ABORT);
    (*env)->ReleaseStringUTFChars(env, topic, topic_str);
    return rc;
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
