#ifndef BIFRORE_JNI_COMMON_H
#define BIFRORE_JNI_COMMON_H

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
    uint64_t message_pipeline_count;
    uint64_t message_pipeline_total_nanos;
    uint64_t message_pipeline_max_nanos;
    uint64_t eval_count;
    uint64_t eval_error_count;
    uint64_t eval_type_error_count;
    uint64_t payload_schema_error_count;
    uint64_t payload_decode_error_count;
    uint64_t payload_build_error_count;
    uint64_t exec_count;
    uint64_t exec_total_nanos;
    uint64_t exec_max_nanos;
    uint64_t topic_match_count;
    uint64_t topic_match_total_nanos;
    uint64_t topic_match_max_nanos;
    uint64_t payload_decode_count;
    uint64_t payload_decode_total_nanos;
    uint64_t payload_decode_max_nanos;
    uint64_t msg_ir_build_count;
    uint64_t msg_ir_build_total_nanos;
    uint64_t msg_ir_build_max_nanos;
    uint64_t predicate_count;
    uint64_t predicate_total_nanos;
    uint64_t predicate_max_nanos;
    uint64_t projection_count;
    uint64_t projection_total_nanos;
    uint64_t projection_max_nanos;
};

extern void *bre_create_engine(
    const char *path,
    int payload_format,
    const char *client_ids_path,
    int notify_mode,
    const char *protobuf_descriptor_set_path);
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
    jclass poll_result_cls;
    jmethodID poll_result_ctor;
    jclass poll_batch_cls;
    jmethodID poll_batch_ctor;
    jclass rule_metadata_cls;
    jmethodID rule_metadata_ctor;
};

extern struct JniClassCache JNI_CACHE;
extern JavaVM *JNI_CACHE_JVM;
extern pthread_once_t JNI_CACHE_ONCE;
extern int JNI_CACHE_INIT_OK;

static const int BRE_OK = 0;
static const int BRE_POLL_RESULT_READY = 1;
static const int BRE_ERR_INVALID_ARGUMENT = -1;
static const int BRE_ERR_INVALID_STATE = -2;
static const int BRE_ERR_INVALID_PARAMETER = -3;
static const int BRE_ERR_START_FAILED = -4;
static const int BRE_ERR_ALREADY_STARTED = -5;
static const int BRE_ERR_WORKER_UNAVAILABLE = -6;
static const int BRE_ERR_INTERNAL_QUEUE_ERROR = -7;
static const int BRE_ERR_INTERNAL_ERROR = -8;

static const int JNI_DIRECT_ERR_HEADER_BUFFER_TOO_SMALL = -4;
static const int JNI_DIRECT_ERR_PAYLOAD_BUFFER_TOO_SMALL = -5;

int ensure_jni_cache(JNIEnv *env);
void clear_pending_batch(void);

#endif
