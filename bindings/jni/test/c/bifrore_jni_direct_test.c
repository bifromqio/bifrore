#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../src/main/c/bifrore_jni.c"

static struct BifroPackedEvalResults FETCH_QUEUE[4];
static int FETCH_RC_QUEUE[4];
static size_t FETCH_QUEUE_LEN = 0;
static size_t FETCH_QUEUE_INDEX = 0;

void *bre_create_with_config(const char *path) {
    (void)path;
    return NULL;
}

void *bre_create_with_config_and_payload_format(const char *path, int payload_format) {
    (void)path;
    (void)payload_format;
    return NULL;
}

void *bre_create_with_config_and_payload_format_and_client_ids_path(
    const char *path,
    int payload_format,
    const char *client_ids_path) {
    (void)path;
    (void)payload_format;
    (void)client_ids_path;
    return NULL;
}

void *bre_create_with_config_and_payload_format_and_client_ids_path_and_notify_mode(
    const char *path,
    int payload_format,
    const char *client_ids_path,
    int notify_mode) {
    (void)path;
    (void)payload_format;
    (void)client_ids_path;
    (void)notify_mode;
    return NULL;
}

void *bre_create_with_config_and_payload_format_and_client_ids_path_and_notify_mode_and_protobuf_schema(
    const char *path,
    int payload_format,
    const char *client_ids_path,
    int notify_mode,
    const char *protobuf_descriptor_set_path,
    const char *protobuf_message_name) {
    (void)path;
    (void)payload_format;
    (void)client_ids_path;
    (void)notify_mode;
    (void)protobuf_descriptor_set_path;
    (void)protobuf_message_name;
    return NULL;
}

void bre_destroy(void *engine) {
    (void)engine;
}

int bre_disconnect(void *engine) {
    (void)engine;
    return 0;
}

int bre_start_mqtt(
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
    void *user_data) {
    (void)engine;
    (void)host;
    (void)port;
    (void)node_id;
    (void)client_count;
    (void)username;
    (void)password;
    (void)clean_start;
    (void)session_expiry_interval;
    (void)group_name;
    (void)ordered;
    (void)ordered_prefix;
    (void)keep_alive_secs;
    (void)multi_nci;
    (void)callback;
    (void)user_data;
    return 0;
}

int bre_poll_eval_results_packed(
    void *engine,
    uint32_t timeout_millis,
    struct BifroPackedEvalResults *out_results) {
    (void)engine;
    (void)timeout_millis;
    if (FETCH_QUEUE_INDEX >= FETCH_QUEUE_LEN) {
        memset(out_results, 0, sizeof(*out_results));
        return 0;
    }
    *out_results = FETCH_QUEUE[FETCH_QUEUE_INDEX];
    FETCH_QUEUE[FETCH_QUEUE_INDEX].rule_indices = NULL;
    FETCH_QUEUE[FETCH_QUEUE_INDEX].payload_offsets = NULL;
    FETCH_QUEUE[FETCH_QUEUE_INDEX].payload_lengths = NULL;
    FETCH_QUEUE[FETCH_QUEUE_INDEX].payload_data = NULL;
    FETCH_QUEUE[FETCH_QUEUE_INDEX].len = 0;
    FETCH_QUEUE[FETCH_QUEUE_INDEX].payload_data_len = 0;
    return FETCH_RC_QUEUE[FETCH_QUEUE_INDEX++];
}

void bre_free_packed_eval_results(struct BifroPackedEvalResults *results) {
    free(results->rule_indices);
    free(results->payload_offsets);
    free(results->payload_lengths);
    free(results->payload_data);
    results->rule_indices = NULL;
    results->payload_offsets = NULL;
    results->payload_lengths = NULL;
    results->payload_data = NULL;
    results->len = 0;
    results->payload_data_len = 0;
}

int bre_get_rule_metadata_table(
    void *engine,
    struct BifroRuleMetadata **out_metadata,
    size_t *out_len) {
    (void)engine;
    (void)out_metadata;
    (void)out_len;
    return 0;
}

void bre_free_rule_metadata_table(struct BifroRuleMetadata *metadata, size_t len) {
    (void)metadata;
    (void)len;
}

int bre_set_log_callback(
    void (*callback)(void *, int, const char *, const char *, uint64_t, const char *, const char *, const char *, uint32_t),
    void *user_data,
    int min_level) {
    (void)callback;
    (void)user_data;
    (void)min_level;
    return 0;
}

int bre_metrics_snapshot(
    void *engine,
    uint64_t *eval_count,
    uint64_t *eval_error_count,
    uint64_t *eval_total_nanos,
    uint64_t *eval_max_nanos) {
    (void)engine;
    (void)eval_count;
    (void)eval_error_count;
    (void)eval_total_nanos;
    (void)eval_max_nanos;
    return 0;
}

int bre_set_poll_batch_limit(void *engine, uint32_t limit) {
    (void)engine;
    (void)limit;
    return 0;
}

static struct BifroPackedEvalResults make_results(
    const uint32_t *rule_indices,
    const uint32_t *payload_lengths,
    size_t len) {
    struct BifroPackedEvalResults results = {0};
    uint32_t offset = 0;
    results.rule_indices = calloc(len, sizeof(uint32_t));
    results.payload_offsets = calloc(len, sizeof(uint32_t));
    results.payload_lengths = calloc(len, sizeof(uint32_t));
    results.payload_data = calloc(len == 0 ? 1 : 64, sizeof(unsigned char));
    results.len = len;
    for (size_t i = 0; i < len; i++) {
        results.rule_indices[i] = rule_indices[i];
        results.payload_offsets[i] = offset;
        results.payload_lengths[i] = payload_lengths[i];
        for (uint32_t j = 0; j < payload_lengths[i]; j++) {
            results.payload_data[offset + j] = (unsigned char)(offset + j + 1);
        }
        offset += payload_lengths[i];
    }
    results.payload_data_len = offset;
    return results;
}

static void reset_direct_test_state(void) {
    clear_pending_batch();
    for (size_t i = FETCH_QUEUE_INDEX; i < FETCH_QUEUE_LEN; i++) {
        if (FETCH_QUEUE[i].rule_indices != NULL) {
            bre_free_packed_eval_results(&FETCH_QUEUE[i]);
        }
    }
    memset(FETCH_QUEUE, 0, sizeof(FETCH_QUEUE));
    memset(FETCH_RC_QUEUE, 0, sizeof(FETCH_RC_QUEUE));
    FETCH_QUEUE_LEN = 0;
    FETCH_QUEUE_INDEX = 0;
}

static void queue_fetch(int rc, struct BifroPackedEvalResults results) {
    assert(FETCH_QUEUE_LEN < 4);
    FETCH_RC_QUEUE[FETCH_QUEUE_LEN] = rc;
    FETCH_QUEUE[FETCH_QUEUE_LEN] = results;
    FETCH_QUEUE_LEN += 1;
}

static void test_store_pending_batch_with_index_tracks_remaining_tail(void) {
    uint32_t rule_indices[] = {1, 2, 3, 4};
    uint32_t payload_lengths[] = {3, 0, 5, 2};
    struct BifroPackedEvalResults results = make_results(rule_indices, payload_lengths, 4);

    struct DirectPendingBatch *pending = store_pending_batch_with_index(&results, 2);
    assert(pending != NULL);
    assert(pending->remaining_messages == 2);
    assert(pending->remaining_payload_bytes == 7);
    assert(results.rule_indices == NULL);
    clear_pending_batch();
}

static void test_copy_batch_to_direct_buffers_reports_copied_counts(void) {
    uint32_t rule_indices[] = {7, 8, 9};
    uint32_t payload_lengths[] = {2, 3, 4};
    struct BifroPackedEvalResults results = make_results(rule_indices, payload_lengths, 3);
    uint32_t header[6] = {0};
    unsigned char payload[8] = {0};
    size_t next_index = 0;
    size_t emitted = 0;
    size_t payload_offset = 0;
    size_t copied_messages = 0;
    size_t copied_payload_bytes = 0;

    int rc = copy_batch_to_direct_buffers(
        &results,
        &next_index,
        header,
        2,
        payload,
        sizeof(payload),
        &emitted,
        &payload_offset,
        &copied_messages,
        &copied_payload_bytes
    );

    assert(rc == 0);
    assert(emitted == 2);
    assert(next_index == 2);
    assert(payload_offset == 5);
    assert(copied_messages == 2);
    assert(copied_payload_bytes == 5);
    assert(header[0] == 7);
    assert(header[1] == 0);
    assert(header[2] == 2);
    assert(header[3] == 8);
    assert(header[4] == 2);
    assert(header[5] == 3);
    bre_free_packed_eval_results(&results);
}

static void test_should_merge_pending_batch_uses_cached_counters(void) {
    memset(&DIRECT_PENDING, 0, sizeof(DIRECT_PENDING));
    DIRECT_PENDING.remaining_messages = 1;
    DIRECT_PENDING.remaining_payload_bytes = 8;
    assert(should_merge_pending_batch(&DIRECT_PENDING, 8, 64) == 1);

    DIRECT_PENDING.remaining_messages = 4;
    DIRECT_PENDING.remaining_payload_bytes = 20;
    assert(should_merge_pending_batch(&DIRECT_PENDING, 8, 64) == 0);
}

static void test_pending_only_full_drain(void) {
    reset_direct_test_state();
    uint32_t rules[] = {10, 11};
    uint32_t lengths[] = {2, 3};
    struct BifroPackedEvalResults pending_results = make_results(rules, lengths, 2);
    store_pending_batch(&pending_results);

    uint32_t header[6] = {0};
    unsigned char payload[8] = {0};
    int rc = poll_results_batch_direct_core((void *)0x1, 50, header, 2, payload, sizeof(payload));

    assert(rc == 2);
    assert(find_pending_batch() == NULL);
    assert(header[0] == 10 && header[1] == 0 && header[2] == 2);
    assert(header[3] == 11 && header[4] == 2 && header[5] == 3);
    reset_direct_test_state();
}

static void test_pending_only_partial_drain(void) {
    reset_direct_test_state();
    uint32_t rules[] = {21, 22, 23};
    uint32_t lengths[] = {2, 3, 4};
    struct BifroPackedEvalResults pending_results = make_results(rules, lengths, 3);
    store_pending_batch(&pending_results);

    uint32_t header[6] = {0};
    unsigned char payload[5] = {0};
    int rc = poll_results_batch_direct_core((void *)0x1, 50, header, 2, payload, sizeof(payload));

    assert(rc == 2);
    struct DirectPendingBatch *pending = find_pending_batch();
    assert(pending != NULL);
    assert(pending->next_index == 2);
    assert(pending->remaining_messages == 1);
    assert(pending->remaining_payload_bytes == 4);
    reset_direct_test_state();
}

static void test_pending_small_tail_fetch_merge_with_remainder(void) {
    reset_direct_test_state();
    uint32_t pending_rules[] = {31};
    uint32_t pending_lengths[] = {1};
    struct BifroPackedEvalResults pending_results = make_results(pending_rules, pending_lengths, 1);
    store_pending_batch(&pending_results);

    uint32_t fetched_rules[] = {41, 42};
    uint32_t fetched_lengths[] = {3, 4};
    queue_fetch(2, make_results(fetched_rules, fetched_lengths, 2));

    uint32_t header[24] = {0};
    unsigned char payload[6] = {0};
    int rc = poll_results_batch_direct_core((void *)0x1, 50, header, 8, payload, sizeof(payload));

    assert(rc == 2);
    assert(header[0] == 31 && header[1] == 0 && header[2] == 1);
    assert(header[3] == 41 && header[4] == 1 && header[5] == 3);
    struct DirectPendingBatch *pending = find_pending_batch();
    assert(pending != NULL);
    assert(pending->remaining_messages == 1);
    assert(pending->remaining_payload_bytes == 4);
    assert(pending->results.rule_indices[pending->next_index] == 42);
    reset_direct_test_state();
}

static void test_pending_small_tail_fetch_fully_fits(void) {
    reset_direct_test_state();
    uint32_t pending_rules[] = {51};
    uint32_t pending_lengths[] = {1};
    struct BifroPackedEvalResults pending_results = make_results(pending_rules, pending_lengths, 1);
    store_pending_batch(&pending_results);

    uint32_t fetched_rules[] = {61, 62};
    uint32_t fetched_lengths[] = {1, 2};
    queue_fetch(2, make_results(fetched_rules, fetched_lengths, 2));

    uint32_t header[24] = {0};
    unsigned char payload[8] = {0};
    int rc = poll_results_batch_direct_core((void *)0x1, 50, header, 8, payload, sizeof(payload));

    assert(rc == 3);
    assert(find_pending_batch() == NULL);
    assert(header[0] == 51 && header[3] == 61 && header[6] == 62);
    reset_direct_test_state();
}

static void test_pending_small_tail_no_fetch_available(void) {
    reset_direct_test_state();
    uint32_t pending_rules[] = {71};
    uint32_t pending_lengths[] = {1};
    struct BifroPackedEvalResults pending_results = make_results(pending_rules, pending_lengths, 1);
    store_pending_batch(&pending_results);

    uint32_t header[24] = {0};
    unsigned char payload[8] = {0};
    int rc = poll_results_batch_direct_core((void *)0x1, 50, header, 8, payload, sizeof(payload));

    assert(rc == 1);
    assert(find_pending_batch() == NULL);
    assert(header[0] == 71 && header[1] == 0 && header[2] == 1);
    reset_direct_test_state();
}

static void test_pending_not_merge_eligible(void) {
    reset_direct_test_state();
    uint32_t pending_rules[] = {81, 82, 83};
    uint32_t pending_lengths[] = {6, 6, 6};
    struct BifroPackedEvalResults pending_results = make_results(pending_rules, pending_lengths, 3);
    store_pending_batch(&pending_results);

    uint32_t fetched_rules[] = {91};
    uint32_t fetched_lengths[] = {2};
    queue_fetch(1, make_results(fetched_rules, fetched_lengths, 1));

    uint32_t header[3] = {0};
    unsigned char payload[6] = {0};
    int rc = poll_results_batch_direct_core((void *)0x1, 50, header, 1, payload, sizeof(payload));

    assert(rc == 1);
    struct DirectPendingBatch *pending = find_pending_batch();
    assert(pending != NULL);
    assert(FETCH_QUEUE_INDEX == 0);
    assert(pending->remaining_messages == 2);
    assert(pending->remaining_payload_bytes == 12);
    reset_direct_test_state();
}

static void test_payload_buffer_fills_before_header_buffer(void) {
    reset_direct_test_state();
    uint32_t rules[] = {101, 102, 103};
    uint32_t lengths[] = {2, 3, 4};
    struct BifroPackedEvalResults pending_results = make_results(rules, lengths, 3);
    store_pending_batch(&pending_results);

    uint32_t header[9] = {0};
    unsigned char payload[5] = {0};
    int rc = poll_results_batch_direct_core((void *)0x1, 50, header, 3, payload, sizeof(payload));

    assert(rc == 2);
    struct DirectPendingBatch *pending = find_pending_batch();
    assert(pending != NULL);
    assert(pending->remaining_messages == 1);
    assert(pending->remaining_payload_bytes == 4);
    reset_direct_test_state();
}

static void test_header_buffer_fills_before_payload_buffer(void) {
    reset_direct_test_state();
    uint32_t rules[] = {111, 112, 113};
    uint32_t lengths[] = {2, 2, 2};
    struct BifroPackedEvalResults pending_results = make_results(rules, lengths, 3);
    store_pending_batch(&pending_results);

    uint32_t header[3] = {0};
    unsigned char payload[16] = {0};
    int rc = poll_results_batch_direct_core((void *)0x1, 50, header, 1, payload, sizeof(payload));

    assert(rc == 1);
    struct DirectPendingBatch *pending = find_pending_batch();
    assert(pending != NULL);
    assert(pending->remaining_messages == 2);
    assert(pending->remaining_payload_bytes == 4);
    reset_direct_test_state();
}

int main(void) {
    test_store_pending_batch_with_index_tracks_remaining_tail();
    test_copy_batch_to_direct_buffers_reports_copied_counts();
    test_should_merge_pending_batch_uses_cached_counters();
    test_pending_only_full_drain();
    test_pending_only_partial_drain();
    test_pending_small_tail_fetch_merge_with_remainder();
    test_pending_small_tail_fetch_fully_fits();
    test_pending_small_tail_no_fetch_available();
    test_pending_not_merge_eligible();
    test_payload_buffer_fills_before_header_buffer();
    test_header_buffer_fills_before_payload_buffer();
    puts("bifrore_jni_direct_test: ok");
    return 0;
}
