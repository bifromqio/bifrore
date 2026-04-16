#include "bifrore_jni_common.h"

struct DirectPendingBatch {
    struct BifroPackedEvalResults results;
    size_t next_index;
    size_t remaining_messages;
    size_t remaining_payload_bytes;
};

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

void clear_pending_batch(void) {
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
        return BRE_OK;
    }
    if (header_capacity_records == 0) {
        return JNI_DIRECT_ERR_HEADER_BUFFER_TOO_SMALL;
    }
    size_t first_length = (size_t)results->payload_lengths[*next_index];
    if (*emitted == 0 && *payload_offset == 0 && first_length > payload_capacity_bytes) {
        return JNI_DIRECT_ERR_PAYLOAD_BUFFER_TOO_SMALL;
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
    return BRE_OK;
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
        if (rc < BRE_OK) {
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
                if (rc < BRE_OK && emitted == 0) {
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
    if (rc < BRE_OK || fetched.len == 0) {
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
    if (rc < BRE_OK) {
        bre_free_packed_eval_results(&fetched);
        return rc;
    }
    if (fetched_index < fetched.len) {
        if (store_pending_batch_with_index(&fetched, fetched_index) == NULL) {
            bre_free_packed_eval_results(&fetched);
            return BRE_ERR_INVALID_ARGUMENT;
        }
    } else {
        bre_free_packed_eval_results(&fetched);
    }
    return (int)emitted;
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
        return BRE_ERR_INVALID_ARGUMENT;
    }

    uint32_t *header_ptr = (uint32_t *)(*env)->GetDirectBufferAddress(env, header_buffer);
    unsigned char *payload_ptr = (unsigned char *)(*env)->GetDirectBufferAddress(env, payload_buffer);
    if (header_ptr == NULL || payload_ptr == NULL || header_capacity_ints <= 0 || payload_capacity_bytes <= 0) {
        return BRE_ERR_INVALID_ARGUMENT;
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
