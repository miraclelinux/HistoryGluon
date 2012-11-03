#ifndef _test_uitls_h
#define _test_uitls_h

#include "history-gluon.h"

#define TEST_STD_ID_UINT   0x10
#define TEST_STD_ID_FLOAT  0x65536
#define TEST_STD_ID_STRING 0x87654321
#define TEST_STD_ID_BLOB   0x123456789abcdef

extern history_gluon_context_t g_ctx;
extern history_gluon_data_t *g_data;
extern history_gluon_data_array_t *g_array;

void create_global_context(void);
void free_global_context(void);
void cleanup_global_data(void);

void assert_delete_all_for_id(uint64_t id, uint64_t *num_deleted);
void assert_make_context_delete_add_samples(uint64_t id,
                                            void (*add_samples_fn)(void));

/* add */
void assert_add_uint(uint64_t id, struct timespec *ts, uint64_t value);
void assert_add_float(uint64_t id, struct timespec *ts, double v);
void assert_add_string(uint64_t id, struct timespec *ts, char *v);
void assert_add_blob(uint64_t id, struct timespec *ts, uint8_t *v, uint64_t len);

void assert_add_samples_with_data(uint64_t num, history_gluon_data_t *sample_array);
void assert_add_uint_hgl_data(history_gluon_data_t *gluon_data);

/* verify */
void assert_equal_hgl_data(history_gluon_data_t *expect,
                           history_gluon_data_t *actual);

/* query */
void
assert_query(uint64_t id, struct timespec *ts,
             history_gluon_query_t query_type, int expected_result);
void
assert_add_uint_and_query_verify(uint64_t id, struct timespec *ts,
                                 uint64_t value);
void
assert_add_float_and_query_verify(uint64_t id, struct timespec *ts,
                                  double value);
void
assert_add_string_and_query_verify(uint64_t id, struct timespec *ts,
                                   char *value);
void
assert_add_blob_and_query_verify(uint64_t id, struct timespec *ts,
                                 uint8_t *value, uint64_t length);

void
asset_range_query_common(uint64_t id, history_gluon_data_t *samples,
                         struct timespec *ts0, struct timespec *ts1,
                         history_gluon_sort_order_t sort_order,
                         uint64_t num_max_entries,
                         uint64_t num_expected_entries,
                         uint64_t expected_first_idx);

#endif // _test_uitls_h
