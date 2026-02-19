#ifndef FFQ_FFI_H
#define FFQ_FFI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct FfqEngineHandle FfqEngineHandle;
typedef struct FfqResultHandle FfqResultHandle;

typedef enum FfqStatusCode {
  FFQ_STATUS_OK = 0,
  FFQ_STATUS_INVALID_CONFIG = 1,
  FFQ_STATUS_PLANNING = 2,
  FFQ_STATUS_EXECUTION = 3,
  FFQ_STATUS_IO = 4,
  FFQ_STATUS_UNSUPPORTED = 5,
  FFQ_STATUS_INTERNAL = 6,
} FfqStatusCode;

FfqStatusCode ffq_engine_new_default(FfqEngineHandle **out_engine, char *err_buf, size_t err_buf_len);
FfqStatusCode ffq_engine_new_from_config_json(const char *config_json, FfqEngineHandle **out_engine, char *err_buf, size_t err_buf_len);
FfqStatusCode ffq_engine_new_from_config_kv(const char *config_kv, FfqEngineHandle **out_engine, char *err_buf, size_t err_buf_len);
void ffq_engine_free(FfqEngineHandle *engine);

FfqStatusCode ffq_engine_register_table_json(FfqEngineHandle *engine, const char *table_json, char *err_buf, size_t err_buf_len);
FfqStatusCode ffq_engine_register_catalog_path(FfqEngineHandle *engine, const char *catalog_path, char *err_buf, size_t err_buf_len);

FfqStatusCode ffq_engine_execute_sql(FfqEngineHandle *engine, const char *sql, FfqResultHandle **out_result, char *err_buf, size_t err_buf_len);
void ffq_result_free(FfqResultHandle *result);

FfqStatusCode ffq_result_ipc_bytes(const FfqResultHandle *result, const uint8_t **out_ptr, size_t *out_len, char *err_buf, size_t err_buf_len);
size_t ffq_result_row_count(const FfqResultHandle *result);
size_t ffq_result_batch_count(const FfqResultHandle *result);

const char *ffq_status_name(FfqStatusCode code);

#ifdef __cplusplus
}
#endif

#endif
