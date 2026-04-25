#include "bifrore_jni_common.h"

JNIEXPORT jobject JNICALL Java_com_bifrore_BifroRE_nativePollResultsBatch(
    JNIEnv *env,
    jclass cls,
    jlong handle,
    jint timeout_millis) {
    (void)cls;
    if (!ensure_jni_cache(env)) {
        return NULL;
    }
    if (handle == 0) {
        return (*env)->NewObject(
            env,
            JNI_CACHE.poll_result_cls,
            JNI_CACHE.poll_result_ctor,
            (jint)BRE_ERR_INVALID_ARGUMENT,
            NULL
        );
    }

    struct BifroPackedEvalResults results = {0};
    int rc = bre_poll_eval_results_packed(
        (void *)handle,
        (uint32_t)timeout_millis,
        &results);
    if (rc < BRE_OK || results.len == 0) {
        return (*env)->NewObject(
            env,
            JNI_CACHE.poll_result_cls,
            JNI_CACHE.poll_result_ctor,
            (jint)rc,
            NULL
        );
    }
    if (results.len > (size_t)INT32_MAX || results.payload_data_len > (size_t)INT32_MAX) {
        bre_free_packed_eval_results(&results);
        return (*env)->NewObject(
            env,
            JNI_CACHE.poll_result_cls,
            JNI_CACHE.poll_result_ctor,
            (jint)BRE_ERR_INTERNAL_ERROR,
            NULL
        );
    }

    size_t header_len = results.len * 3;
    jintArray header_triples = (*env)->NewIntArray(env, (jsize)header_len);
    jbyteArray payload_data = (*env)->NewByteArray(env, (jsize)results.payload_data_len);
    if (header_triples == NULL || payload_data == NULL) {
        if (header_triples != NULL) {
            (*env)->DeleteLocalRef(env, header_triples);
        }
        if (payload_data != NULL) {
            (*env)->DeleteLocalRef(env, payload_data);
        }
        bre_free_packed_eval_results(&results);
        return (*env)->NewObject(
            env,
            JNI_CACHE.poll_result_cls,
            JNI_CACHE.poll_result_ctor,
            (jint)BRE_ERR_INTERNAL_ERROR,
            NULL
        );
    }
    jint *header_values = (jint *)malloc(sizeof(jint) * header_len);
    if (header_values == NULL) {
        (*env)->DeleteLocalRef(env, header_triples);
        (*env)->DeleteLocalRef(env, payload_data);
        bre_free_packed_eval_results(&results);
        return (*env)->NewObject(
            env,
            JNI_CACHE.poll_result_cls,
            JNI_CACHE.poll_result_ctor,
            (jint)BRE_ERR_INTERNAL_ERROR,
            NULL
        );
    }
    for (size_t idx = 0; idx < results.len; idx++) {
        size_t base = idx * 3;
        header_values[base] = (jint)results.rule_indices[idx];
        header_values[base + 1] = (jint)results.payload_offsets[idx];
        header_values[base + 2] = (jint)results.payload_lengths[idx];
    }
    (*env)->SetIntArrayRegion(env, header_triples, 0, (jsize)header_len, header_values);
    free(header_values);
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
        header_triples,
        payload_data
    );
    (*env)->DeleteLocalRef(env, header_triples);
    (*env)->DeleteLocalRef(env, payload_data);
    bre_free_packed_eval_results(&results);
    jobject result = (*env)->NewObject(
        env,
        JNI_CACHE.poll_result_cls,
        JNI_CACHE.poll_result_ctor,
        (jint)rc,
        batch
    );
    if (batch != NULL) {
        (*env)->DeleteLocalRef(env, batch);
    }
    return result;
}
