#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ffq_ffi.h"

static int check_status(FfqStatusCode code, const char *step, const char *err) {
  if (code == FFQ_STATUS_OK) {
    return 1;
  }
  fprintf(stderr, "%s failed: %s (%s)\n", step, ffq_status_name(code), err ? err : "");
  return 0;
}

int main(int argc, char **argv) {
  if (argc < 2) {
    fprintf(stderr, "usage: %s /absolute/path/to/lineitem.parquet\n", argv[0]);
    return 2;
  }

  char err[1024] = {0};
  FfqEngineHandle *engine = NULL;
  FfqStatusCode code = ffq_engine_new_default(&engine, err, sizeof(err));
  if (!check_status(code, "ffq_engine_new_default", err)) {
    return 1;
  }

  char table_json[2048];
  snprintf(
      table_json,
      sizeof(table_json),
      "{\"name\":\"lineitem\",\"uri\":\"%s\",\"format\":\"parquet\"}",
      argv[1]);
  code = ffq_engine_register_table_json(engine, table_json, err, sizeof(err));
  if (!check_status(code, "ffq_engine_register_table_json", err)) {
    ffq_engine_free(engine);
    return 1;
  }

  FfqResultHandle *r1 = NULL;
  code = ffq_engine_execute_sql(
      engine, "SELECT 1 AS one FROM lineitem LIMIT 1", &r1, err, sizeof(err));
  if (!check_status(code, "ffq_engine_execute_sql(select 1)", err)) {
    ffq_engine_free(engine);
    return 1;
  }
  const uint8_t *ipc_ptr = NULL;
  size_t ipc_len = 0;
  code = ffq_result_ipc_bytes(r1, &ipc_ptr, &ipc_len, err, sizeof(err));
  if (!check_status(code, "ffq_result_ipc_bytes(select 1)", err)) {
    ffq_result_free(r1);
    ffq_engine_free(engine);
    return 1;
  }
  printf(
      "select1: batches=%zu rows=%zu ipc_bytes=%zu\n",
      ffq_result_batch_count(r1),
      ffq_result_row_count(r1),
      ipc_len);
  ffq_result_free(r1);

  FfqResultHandle *r2 = NULL;
  code = ffq_engine_execute_sql(
      engine, "SELECT l_orderkey FROM lineitem LIMIT 5", &r2, err, sizeof(err));
  if (!check_status(code, "ffq_engine_execute_sql(parquet scan)", err)) {
    ffq_engine_free(engine);
    return 1;
  }
  ipc_ptr = NULL;
  ipc_len = 0;
  code = ffq_result_ipc_bytes(r2, &ipc_ptr, &ipc_len, err, sizeof(err));
  if (!check_status(code, "ffq_result_ipc_bytes(parquet scan)", err)) {
    ffq_result_free(r2);
    ffq_engine_free(engine);
    return 1;
  }
  printf(
      "parquet_scan: batches=%zu rows=%zu ipc_bytes=%zu\n",
      ffq_result_batch_count(r2),
      ffq_result_row_count(r2),
      ipc_len);
  ffq_result_free(r2);

  ffq_engine_free(engine);
  puts("ffi example: OK");
  return 0;
}
