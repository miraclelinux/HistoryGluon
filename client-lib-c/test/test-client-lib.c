#include <cutter.h>
#include "utils.h"
#include "history-gluon.h"

/* ---------------------------------------------------------------------------
 * Global variables
 * ------------------------------------------------------------------------ */
static history_gluon_data_t g_uint_samples[] = {
	{
		.id = TEST_STD_ID_UINT,
		.type = HISTORY_GLUON_TYPE_UINT,
		.ts.tv_sec = 1234567890,
		.ts.tv_nsec = 123456789,
		.v.uint = 1,
	},
	{
		.id = TEST_STD_ID_UINT,
		.type = HISTORY_GLUON_TYPE_UINT,
		.ts.tv_sec = 1506070800,
		.ts.tv_nsec = 100000000,
		.v.uint = 10,
	},
	{
		.id = TEST_STD_ID_UINT,
		.type = HISTORY_GLUON_TYPE_UINT,
		.ts.tv_sec = 1600000000,
		.ts.tv_nsec = 500000000,
		.v.uint = 12340,
	},
	{
		.id = TEST_STD_ID_UINT,
		.type = HISTORY_GLUON_TYPE_UINT,
		.ts.tv_sec = 1600000001,
		.ts.tv_nsec = 200000000,
		.v.uint = 0x123456789abcdef0,
	},
	{
		.id = TEST_STD_ID_UINT,
		.type = HISTORY_GLUON_TYPE_UINT,
		.ts.tv_sec = 2500000000,
		.ts.tv_nsec = 300000000,
		.v.uint = 0xfedcba9876543210,
	},
};
static const int NUM_UINT_SAMPLES =
  sizeof(g_uint_samples) / sizeof(history_gluon_data_t);

static history_gluon_data_t g_float_samples[] = {
	{
		.id = TEST_STD_ID_FLOAT,
		.type = HISTORY_GLUON_TYPE_FLOAT,
		.ts.tv_sec = 1234567890,
		.ts.tv_nsec = 123456789,
		.v.fp = 0.1,
	},
	{
		.id = TEST_STD_ID_FLOAT,
		.type = HISTORY_GLUON_TYPE_FLOAT,
		.ts.tv_sec = 1506070800,
		.ts.tv_nsec = 100000000,
		.v.fp = 99.9,
	},
	{
		.id = TEST_STD_ID_FLOAT,
		.type = HISTORY_GLUON_TYPE_FLOAT,
		.ts.tv_sec = 1600000000,
		.ts.tv_nsec = 500000000,
		.v.fp = 100.0,
	},
	{
		.id = TEST_STD_ID_FLOAT,
		.type = HISTORY_GLUON_TYPE_FLOAT,
		.ts.tv_sec = 1600000001,
		.ts.tv_nsec = 200000000,
		.v.fp = -10.5,
	},
	{
		.id = TEST_STD_ID_FLOAT,
		.type = HISTORY_GLUON_TYPE_FLOAT,
		.ts.tv_sec = 2500000000,
		.ts.tv_nsec = 300000000,
		.v.fp = -3.2e5,
	},
};
static const int NUM_FLOAT_SAMPLES =
  sizeof(g_float_samples) / sizeof(history_gluon_data_t);

static char string_sample0[] = "Hello, World!";
static char string_sample1[] = 
  "Linux is a Unix-like computer operating system assembled under the model "
  "of free and open source software development and distribution. "
  "The defining component of Linux is the Linux kernel, an operating system "
  "kernel first released 5 October 1991 by Linus Torvalds.";
static char string_sample2[] = 
  "C++ (pronounced \"see plus plus\") is a statically typed, free-form, "
  "multi-paradigm, compiled, general-purpose programming language. It is "
  "regarded as an intermediate-level language, as it comprises a combination "
  "of both high-level and low-level language features. Developed by Bjarne "
  "Stroustrup starting in 1979 at Bell Labs, it adds object oriented "
  "features, such as classes, and other enhancements to the C programming "
  "language. Originally named C with Classes, the language was renamed "
  "C++ in 1983, as a pun involving the increment operator.\n";
static char string_sample3[] = "walrus";
static char string_sample4[] = "Are you hungry?";

static history_gluon_data_t g_string_samples[] = {
	{
		.id = TEST_STD_ID_STRING,
		.type = HISTORY_GLUON_TYPE_STRING,
		.ts.tv_sec = 1234567890,
		.ts.tv_nsec = 123456789,
		.v.string = string_sample0,
		.length = sizeof(string_sample0) - 1,
	},
	{
		.id = TEST_STD_ID_STRING,
		.type = HISTORY_GLUON_TYPE_STRING,
		.ts.tv_sec = 1506070800,
		.ts.tv_nsec = 100000000,
		.v.string = string_sample1,
		.length = sizeof(string_sample1) - 1,
	},
	{
		.id = TEST_STD_ID_STRING,
		.type = HISTORY_GLUON_TYPE_STRING,
		.ts.tv_sec = 1600000000,
		.ts.tv_nsec = 500000000,
		.v.string = string_sample2,
		.length = sizeof(string_sample2) - 1,
	},
	{
		.id = TEST_STD_ID_STRING,
		.type = HISTORY_GLUON_TYPE_STRING,
		.ts.tv_sec = 1600000001,
		.ts.tv_nsec = 200000000,
		.v.string = string_sample3,
		.length = sizeof(string_sample3) - 1,
	},
	{
		.id = TEST_STD_ID_STRING,
		.type = HISTORY_GLUON_TYPE_STRING,
		.ts.tv_sec = 2500000000,
		.ts.tv_nsec = 300000000,
		.v.string = string_sample4,
		.length = sizeof(string_sample4) - 1,
	},
};
static const int NUM_STRING_SAMPLES =
  sizeof(g_string_samples) / sizeof(history_gluon_data_t);

static uint8_t blob_sample0[] = {0x21, 0x08, 0x05};
static uint8_t blob_sample1[] = {0x21, 0xf8, 0x25, 0x88, 0x99, 0xaa};
static uint8_t blob_sample2[] = {0xc0, 0x72, 0x01, 0x99};
static uint8_t blob_sample3[] = {0xff};
static uint8_t blob_sample4[] = {0x2f, 0x53, 0x45, 0x25, 0x83, 0xab, 0x58, 0x88,
                                 0x10, 0x09, 0xc0, 0xde, 0xfe, 0x83, 0x2a, 0xcc,
                                };

static history_gluon_data_t g_blob_samples[] = {
	{
		.id = TEST_STD_ID_BLOB,
		.type = HISTORY_GLUON_TYPE_BLOB,
		.ts.tv_sec = 1234567890,
		.ts.tv_nsec = 123456789,
		.v.blob = blob_sample0,
		.length = sizeof(blob_sample0) / sizeof(uint8_t),
	},
	{
		.id = TEST_STD_ID_BLOB,
		.type = HISTORY_GLUON_TYPE_BLOB,
		.ts.tv_sec = 1506070800,
		.ts.tv_nsec = 100000000,
		.v.blob = blob_sample1,
		.length = sizeof(blob_sample1) / sizeof(uint8_t),
	},
	{
		.id = TEST_STD_ID_BLOB,
		.type = HISTORY_GLUON_TYPE_BLOB,
		.ts.tv_sec = 1600000000,
		.ts.tv_nsec = 500000000,
		.v.blob = blob_sample2,
		.length = sizeof(blob_sample2) / sizeof(uint8_t),
	},
	{
		.id = TEST_STD_ID_BLOB,
		.type = HISTORY_GLUON_TYPE_BLOB,
		.ts.tv_sec = 1600000001,
		.ts.tv_nsec = 200000000,
		.v.blob = blob_sample3,
		.length = sizeof(blob_sample3) / sizeof(uint8_t),
	},
	{
		.id = TEST_STD_ID_BLOB,
		.type = HISTORY_GLUON_TYPE_BLOB,
		.ts.tv_sec = 2500000000,
		.ts.tv_nsec = 300000000,
		.v.blob = blob_sample4,
		.length = sizeof(blob_sample4) / sizeof(uint8_t),
	},
};
static const int NUM_BLOB_SAMPLES =
  sizeof(g_blob_samples) / sizeof(history_gluon_data_t);


/* ----------------------------------------------------------------------------
 * Utility functions
 * ------------------------------------------------------------------------- */
static void assert_add_uint_samples(void) {
	assert_add_samples_with_data(NUM_UINT_SAMPLES, g_uint_samples);
}

static void assert_add_float_samples(void) {
	assert_add_samples_with_data(NUM_FLOAT_SAMPLES, g_float_samples);
}

static void assert_add_string_samples(void) {
	assert_add_samples_with_data(NUM_STRING_SAMPLES, g_string_samples);
}

static void assert_add_blob_samples(void) {
	assert_add_samples_with_data(NUM_BLOB_SAMPLES, g_blob_samples);
}

static void
set_mean_ts(struct timespec *ts0, struct timespec *ts1, struct timespec *ts)
{
	static const int NS_500MS = 500000000;
	uint32_t dsec = ts1->tv_sec - ts0->tv_sec;
	int dsec_mod2 = dsec % 2;
	ts->tv_sec = ts0->tv_sec + dsec / 2;
	ts->tv_nsec = ts0->tv_nsec + (ts1->tv_nsec - ts0->tv_nsec) / 2;

	if (dsec_mod2 == 0)
		return;

	if (ts->tv_nsec < NS_500MS) {
		ts->tv_nsec += NS_500MS;
		return;
	}

	ts->tv_sec++;
	ts->tv_nsec -= NS_500MS;
}

static double get_history_gluon_data_value(history_gluon_data_t *gluon_data)
{
	if (gluon_data->type == HISTORY_GLUON_TYPE_FLOAT)
		return gluon_data->v.fp;
	else if (gluon_data->type == HISTORY_GLUON_TYPE_UINT)
		return gluon_data->v.uint;
	return 0;
}

static void
calc_statistics(history_gluon_data_t *samples, uint64_t idx0, uint64_t num,
                history_gluon_statistics_t *statistics)
{
	uint64_t i = idx0;
	statistics->count = 1;
	double value = get_history_gluon_data_value(&samples[i]);
	statistics->min = value;
	statistics->max = value;
	statistics->sum = value;

	for (i = idx0 + 1; i < idx0 + num; i++) {
		double value = get_history_gluon_data_value(&samples[i]);
		if (value < statistics->min)
			statistics->min = value;
		else if (value > statistics->max)
			statistics->max = value;
		statistics->sum += value;
		statistics->count++;
	}
	statistics->average = statistics->sum / statistics->count;
	statistics->delta = statistics->max - statistics->min;
}

static void
assert_statistics(history_gluon_statistics_t *expected,
                  history_gluon_statistics_t *actual)
{
	cut_assert_equal_int_least64(expected->count, actual->count);
	double err = 0.0;
	cut_assert_equal_double(expected->min, err, actual->min);
	cut_assert_equal_double(expected->max, err, actual->max);
	cut_assert_equal_double(expected->sum, err, actual->sum);
	cut_assert_equal_double(expected->average, err, actual->average);
	cut_assert_equal_double(expected->delta, err, actual->delta);
}

/* ---------------------------------------------------------------------------
 * Teset cases
 * ------------------------------------------------------------------------ */
void setup(void)
{
}

void teardown(void)
{
	cleanup_global_data();
}

/* ---------------------------------------------------------------------------
 * Context
 * ------------------------------------------------------------------------ */
void test_create_context(void)
{
	create_global_context();
}

void test_free_context(void)
{
	create_global_context();
	free_global_context();
}

/* ---------------------------------------------------------------------------
 * Add Data
 * ------------------------------------------------------------------------ */
static void init_add(uint64_t id)
{
	create_global_context();
	assert_delete_all_for_id(id, NULL);
}

static void assert_add_common(uint64_t id, history_gluon_result_t (*add_fn)(uint64_t id))
{
	init_add(id);
	history_gluon_result_t ret = (*add_fn)(id);
	cut_assert_equal_int(HGL_SUCCESS, ret);
}

static void
assert_add_common_twice(uint64_t id, history_gluon_result_t (*add_fn)(uint64_t id))
{
	assert_add_common(id,add_fn);
	history_gluon_result_t ret = (*add_fn)(id);
	cut_assert_equal_int(HGL_SUCCESS, ret);
}

/* uint */
static history_gluon_result_t add_uint_one_sample(uint64_t id)
{
	struct timespec ts;
	ts.tv_sec = 1;
	ts.tv_nsec = 2;
	uint64_t value = 3;
	return history_gluon_add_uint(g_ctx, id, &ts, value);
}

void test_add_uint(void)
{
	assert_add_common(TEST_STD_ID_UINT, add_uint_one_sample);
}

void test_add_uint_twice(void)
{
	assert_add_common_twice(TEST_STD_ID_UINT, add_uint_one_sample);
}

/* float */
static history_gluon_result_t add_float_one_sample(uint64_t id)
{
	struct timespec ts;
	ts.tv_sec = 20;
	ts.tv_nsec = 40;
	double value = -10.5;
	return history_gluon_add_float(g_ctx, id, &ts, value);
}

void test_add_float(void)
{
	assert_add_common(TEST_STD_ID_FLOAT, add_float_one_sample);
}

void test_add_float_twice(void)
{
	assert_add_common_twice(TEST_STD_ID_FLOAT, add_float_one_sample);
}

/* string */
static history_gluon_result_t add_string_one_sample(uint64_t id)
{
	struct timespec ts;
	ts.tv_sec = 300;
	ts.tv_nsec = 500;
	char value[] = "test_string";
	return history_gluon_add_string(g_ctx, id, &ts, value);
}

void test_add_string(void)
{
	assert_add_common(TEST_STD_ID_STRING, add_string_one_sample);
}

void test_add_string_twice(void)
{
	assert_add_common_twice(TEST_STD_ID_STRING, add_string_one_sample);
}

/* blob */
static history_gluon_result_t add_blob_one_sample(uint64_t id)
{
	struct timespec ts;
	ts.tv_sec = 4300;
	ts.tv_nsec = 8500;
	uint8_t value[] = {0x21, 0x22, 0xff, 0x80, 0x95};
	return history_gluon_add_blob(g_ctx, id, &ts, value, sizeof(value));
}

void test_add_blob(void)
{
	assert_add_common(TEST_STD_ID_BLOB, add_blob_one_sample);
}

void test_add_blob_twice(void)
{
	assert_add_common_twice(TEST_STD_ID_BLOB, add_blob_one_sample);
}

/* add twice and check */
void test_add_uint_twice_and_check(void)
{
	struct timespec ts = {2000, 9012};
	uint64_t v = 300;
	create_global_context();
	assert_add_uint_and_query_verify(TEST_STD_ID_UINT, &ts, v);
	assert_add_uint_and_query_verify(TEST_STD_ID_UINT, &ts, v+1);
}

void test_add_float_twice_and_check(void)
{
	struct timespec ts = {2000, 9012};
	double v = 1.2;
	create_global_context();
	assert_add_float_and_query_verify(TEST_STD_ID_FLOAT, &ts, v);
	assert_add_float_and_query_verify(TEST_STD_ID_FLOAT, &ts, v+1);
}

void test_add_string_twice_and_check(void)
{
	struct timespec ts = {2000, 9012};
	create_global_context();
	assert_add_string_and_query_verify(TEST_STD_ID_STRING, &ts, "Dog");
	assert_add_string_and_query_verify(TEST_STD_ID_STRING, &ts, "Cat");
}

void test_add_blob_twice_and_check(void)
{
	struct timespec ts = {2000, 9012};
	uint8_t v1[] = {0x12, 0x45};
	uint8_t v2[] = {0xaa, 0xbb, 0xcc};
	create_global_context();
	assert_add_blob_and_query_verify(TEST_STD_ID_BLOB, &ts, v1,
	                                 sizeof(v1)/sizeof(uint8_t));
	assert_add_blob_and_query_verify(TEST_STD_ID_BLOB, &ts, v2,
	                                 sizeof(v2)/sizeof(uint8_t));
}

/* ---------------------------------------------------------------------------
 * Query
 * ------------------------------------------------------------------------ */
/* query */
static void assert_add_and_query(history_gluon_data_t *samples)
{
	create_global_context();

	// delete and add data
	int idx = 2;
	history_gluon_data_t *sample = &samples[idx];
	assert_delete_all_for_id(sample->id, NULL);
	assert_add_hgl_data(sample);

	// query
	assert_query(sample->id, &sample->ts,
	             HISTORY_GLUON_QUERY_TYPE_ONLY_MATCH, HGL_SUCCESS);
	assert_equal_hgl_data(sample, g_data);
}

void test_add_uint_and_query(void)
{
	assert_add_and_query(g_uint_samples);
}

void test_add_float_and_query(void)
{
	assert_add_and_query(g_float_samples);
}

void test_add_stringt_and_query(void)
{
	assert_add_and_query(g_string_samples);
}

void test_add_blob_and_query(void)
{
	assert_add_and_query(g_blob_samples);
}

/* query not found */
static void assert_add_and_query_not_found(history_gluon_data_t *samples)
{
	create_global_context();

	// delete and add data
	int idx = 1;
	history_gluon_data_t *sample = &samples[idx];
	assert_delete_all_for_id(sample->id, NULL);
	assert_delete_all_for_id(sample->id+1, NULL);
	assert_add_hgl_data(sample);

	// query
	assert_query(sample->id+1, &sample->ts,
	             HISTORY_GLUON_QUERY_TYPE_ONLY_MATCH, HGLSVERR_NOT_FOUND);
}

void test_add_uint_and_query_not_found(void)
{
	assert_add_and_query_not_found(g_uint_samples);
}

void test_add_float_and_query_not_found(void)
{
	assert_add_and_query_not_found(g_float_samples);
}

void test_add_string_and_query_not_found(void)
{
	assert_add_and_query_not_found(g_string_samples);
}

void test_add_blob_and_query_not_found(void)
{
	assert_add_and_query_not_found(g_blob_samples);
}

/* query less/greater */
static void
assert_add_and_query_less_greater(uint64_t id, void (*add_samples_fn)(void),
                                  history_gluon_data_t *samples,
                                  history_gluon_query_t query_type)
{
	assert_make_context_delete_add_samples(id, add_samples_fn);

	// query
	int idx = 1;
	struct timespec ts;
	set_mean_ts(&samples[idx].ts, &samples[idx+1].ts, &ts);

	// query
	assert_query(id, &ts, query_type, HGL_SUCCESS);

	int exp_idx = idx;
	if (query_type == HISTORY_GLUON_QUERY_TYPE_LESS_DATA)
		exp_idx = idx;
	else if (query_type ==  HISTORY_GLUON_QUERY_TYPE_GREATER_DATA)
		exp_idx = idx + 1;
	else
		cut_fail("Unknown query_type: %d", query_type);
	assert_equal_hgl_data(&samples[exp_idx], g_data);
}

void test_add_uint_and_query_less(void)
{
	assert_add_and_query_less_greater(TEST_STD_ID_UINT,
	                                  assert_add_uint_samples,
	                                  g_uint_samples, 
	                                  HISTORY_GLUON_QUERY_TYPE_LESS_DATA);
}

void test_add_uint_and_query_greater(void)
{
	assert_add_and_query_less_greater(TEST_STD_ID_UINT,
	                                  assert_add_uint_samples,
	                                  g_uint_samples, 
	                                  HISTORY_GLUON_QUERY_TYPE_GREATER_DATA);
}

void test_add_float_and_query_less(void)
{
	assert_add_and_query_less_greater(TEST_STD_ID_FLOAT,
	                                  assert_add_float_samples,
	                                  g_float_samples, 
	                                  HISTORY_GLUON_QUERY_TYPE_LESS_DATA);
}

void test_add_float_and_query_greater(void)
{
	assert_add_and_query_less_greater(TEST_STD_ID_FLOAT,
	                                  assert_add_float_samples,
	                                  g_float_samples, 
	                                  HISTORY_GLUON_QUERY_TYPE_GREATER_DATA);
}

void test_add_string_and_query_less(void)
{
	assert_add_and_query_less_greater(TEST_STD_ID_STRING,
	                                  assert_add_string_samples,
	                                  g_string_samples, 
	                                  HISTORY_GLUON_QUERY_TYPE_LESS_DATA);
}

void test_add_string_and_query_greater(void)
{
	assert_add_and_query_less_greater(TEST_STD_ID_STRING,
	                                  assert_add_string_samples,
	                                  g_string_samples, 
	                                  HISTORY_GLUON_QUERY_TYPE_GREATER_DATA);
}

void test_add_blob_and_query_less(void)
{
	assert_add_and_query_less_greater(TEST_STD_ID_BLOB,
	                                  assert_add_blob_samples,
	                                  g_blob_samples, 
	                                  HISTORY_GLUON_QUERY_TYPE_LESS_DATA);
}

void test_add_blob_and_query_greater(void)
{
	assert_add_and_query_less_greater(TEST_STD_ID_BLOB,
	                                  assert_add_blob_samples,
	                                  g_blob_samples, 
	                                  HISTORY_GLUON_QUERY_TYPE_GREATER_DATA);
}

/* --------------------------------------------------------------------------------------
 * Range Query
 * ----------------------------------------------------------------------------------- */
/* asc/dsc */
static void
assert_range_query_1_3(uint64_t id, void (*add_samples_fn)(void), 
                      history_gluon_data_t *samples,
                      history_gluon_sort_order_t sort_order)
{
	assert_make_context_delete_add_samples(id, add_samples_fn);

	// range query
	int idx0 = 1;
	int idx1 = 3;
	asset_range_query_common(id, samples,
	                         &samples[idx0].ts, &samples[idx1].ts,
	                         sort_order,
	                         HISTORY_GLUON_NUM_ENTRIES_UNLIMITED,
	                         idx1 - idx0, idx0);
}

void test_range_query_uint_asc(void)
{
	assert_range_query_1_3(TEST_STD_ID_UINT, assert_add_uint_samples,
	                       g_uint_samples,
	                       HISTORY_GLUON_SORT_ASCENDING);
}

void test_range_query_uint_dsc(void)
{
	assert_range_query_1_3(TEST_STD_ID_UINT, assert_add_uint_samples,
	                       g_uint_samples,
	                       HISTORY_GLUON_SORT_DESCENDING);
}

void test_range_query_float_asc(void)
{
	assert_range_query_1_3(TEST_STD_ID_FLOAT, assert_add_float_samples,
	                       g_float_samples, HISTORY_GLUON_SORT_ASCENDING);
}

void test_range_query_float_dsc(void)
{
	assert_range_query_1_3(TEST_STD_ID_FLOAT, assert_add_float_samples,
	                       g_float_samples, HISTORY_GLUON_SORT_DESCENDING);
}

void test_range_query_string_asc(void)
{
	assert_range_query_1_3(TEST_STD_ID_STRING, assert_add_string_samples,
	                       g_string_samples, HISTORY_GLUON_SORT_ASCENDING);
}

void test_range_query_string_dsc(void)
{
	assert_range_query_1_3(TEST_STD_ID_STRING, assert_add_string_samples,
	                       g_string_samples, HISTORY_GLUON_SORT_DESCENDING);
}

void test_range_query_blob_asc(void)
{
	assert_range_query_1_3(TEST_STD_ID_BLOB, assert_add_blob_samples,
	                       g_blob_samples, HISTORY_GLUON_SORT_ASCENDING);
}

void test_range_query_blob_dsc(void)
{
	assert_range_query_1_3(TEST_STD_ID_BLOB, assert_add_blob_samples,
	                       g_blob_samples, HISTORY_GLUON_SORT_DESCENDING);
}

/* not found head */
void assert_range_query_not_found_head(uint64_t id,
                                       void (*add_samples_fn)(void),
                                       history_gluon_data_t *samples)
{
	assert_make_context_delete_add_samples(id, add_samples_fn);

	// range query
	struct timespec *ts0 = &HISTORY_GLUON_TIMESPEC_START;
	struct timespec *ts1 = &samples[0].ts;
	asset_range_query_common(id, samples, ts0, ts1,
	                         HISTORY_GLUON_SORT_ASCENDING, 
	                         HISTORY_GLUON_NUM_ENTRIES_UNLIMITED, 0, 0);
}

void test_range_query_uint_not_found_head(void)
{
	assert_range_query_not_found_head(TEST_STD_ID_UINT,
	                                  assert_add_uint_samples,
	                                  g_uint_samples);
}

void test_range_query_float_not_found_head(void)
{
	assert_range_query_not_found_head(TEST_STD_ID_FLOAT,
	                                  assert_add_float_samples,
	                                  g_float_samples);
}

void test_range_query_string_not_found_head(void)
{
	assert_range_query_not_found_head(TEST_STD_ID_STRING,
	                                  assert_add_string_samples,
	                                  g_string_samples);
}

void test_range_query_blob_not_found_head(void)
{
	assert_range_query_not_found_head(TEST_STD_ID_BLOB,
	                                  assert_add_blob_samples,
	                                  g_blob_samples);
}

/* not_found_tail */
void assert_range_query_not_found_tail(uint64_t id,
                                       void (*add_samples_fn)(void),
                                       history_gluon_data_t *samples,
                                       int num_samples)
{
	assert_make_context_delete_add_samples(id, add_samples_fn);

	// range query
	struct timespec ts0;
	memcpy(&ts0, &samples[num_samples-1].ts, sizeof(struct timespec));
	ts0.tv_sec++;
	struct timespec *ts1 = &HISTORY_GLUON_TIMESPEC_END;
	asset_range_query_common(id, samples,  &ts0, ts1,
	                         HISTORY_GLUON_SORT_ASCENDING, 
	                         HISTORY_GLUON_NUM_ENTRIES_UNLIMITED, 0, 0);
}

void test_range_query_uint_not_found_tail(void)
{
	assert_range_query_not_found_tail(TEST_STD_ID_UINT,
	                                  assert_add_uint_samples,
	                                  g_uint_samples, NUM_UINT_SAMPLES);
}

void test_range_query_float_not_found_tail(void)
{
	assert_range_query_not_found_tail(TEST_STD_ID_FLOAT,
	                                  assert_add_float_samples,
	                                  g_float_samples, NUM_FLOAT_SAMPLES);
}

void test_range_query_string_not_found_tail(void)
{
	assert_range_query_not_found_tail(TEST_STD_ID_STRING,
	                                  assert_add_string_samples,
	                                  g_string_samples, NUM_STRING_SAMPLES);
}

void test_range_query_blob_not_found_tail(void)
{
	assert_range_query_not_found_tail(TEST_STD_ID_BLOB,
	                                  assert_add_blob_samples,
	                                  g_blob_samples, NUM_BLOB_SAMPLES);
}

/* get_tail */
void assert_range_query_get_tail(uint64_t id, void (*add_samples_fn)(void),
                                 history_gluon_data_t *samples, int num_samples)
{
	assert_make_context_delete_add_samples(id, add_samples_fn);

	// range query
	struct timespec *ts0 = &(samples[num_samples-1].ts);
	struct timespec *ts1 = &HISTORY_GLUON_TIMESPEC_END;
	asset_range_query_common(id, samples,  ts0, ts1,
	                         HISTORY_GLUON_SORT_ASCENDING, 
	                         HISTORY_GLUON_NUM_ENTRIES_UNLIMITED,
	                         1, num_samples-1);
}

void test_range_query_uint_get_tail(void)
{
	assert_range_query_get_tail(TEST_STD_ID_UINT, assert_add_uint_samples,
	                            g_uint_samples, NUM_UINT_SAMPLES);
}

void test_range_query_float_get_tail(void)
{
	assert_range_query_get_tail(TEST_STD_ID_FLOAT, assert_add_float_samples,
	                            g_float_samples, NUM_FLOAT_SAMPLES);
}

void test_range_query_string_get_tail(void)
{
	assert_range_query_get_tail(TEST_STD_ID_STRING,
	                            assert_add_string_samples,
	                            g_string_samples, NUM_STRING_SAMPLES);
}

void test_range_query_blob_get_tail(void)
{
	assert_range_query_get_tail(TEST_STD_ID_BLOB, assert_add_blob_samples,
	                            g_blob_samples, NUM_BLOB_SAMPLES);
}

/* ---------------------------------------------------------------------------
 * Get Minimum Time
 * ------------------------------------------------------------------------ */
static void assert_get_minimum_time(uint64_t id, void (*add_samples_fn)(void),
                                    history_gluon_data_t *samples)
{
	assert_make_context_delete_add_samples(id, add_samples_fn);

	/* get the minimum */
	struct timespec ts;
	history_gluon_result_t ret;
	ret = history_gluon_get_minimum_time(g_ctx, id, &ts);
	cut_assert_equal_int(HGL_SUCCESS, ret);

	/* test the obtained time */
	cut_assert_equal_int_least32(samples[0].ts.tv_sec, ts.tv_sec);
	cut_assert_equal_int_least32(samples[0].ts.tv_nsec, ts.tv_nsec);
}

void test_get_minimum_time_uint(void)
{
	assert_get_minimum_time(TEST_STD_ID_UINT, assert_add_uint_samples,
                                g_uint_samples);
}

void test_get_minimum_time_float(void)
{
	assert_get_minimum_time(TEST_STD_ID_FLOAT, assert_add_float_samples,
                                g_float_samples);
}

void test_get_minimum_time_string(void)
{
	assert_get_minimum_time(TEST_STD_ID_STRING, assert_add_string_samples,
                                g_string_samples);
}

void test_get_minimum_time_blob(void)
{
	assert_get_minimum_time(TEST_STD_ID_BLOB, assert_add_blob_samples,
                                g_blob_samples);
}

static void assert_get_minimum_time_not_found(uint64_t id)
{
	create_global_context();
	assert_delete_all_for_id(id, NULL);

	/* get the minimum */
	struct timespec ts;
	history_gluon_result_t ret;
	ret = history_gluon_get_minimum_time(g_ctx, id, &ts);
	cut_assert_equal_int(HGLSVERR_NOT_FOUND, ret);
}

void test_get_minimum_time_not_found_uint(void)
{
	assert_get_minimum_time_not_found(TEST_STD_ID_UINT);
}

void test_get_minimum_time_not_found_float(void)
{
	assert_get_minimum_time_not_found(TEST_STD_ID_FLOAT);
}

void test_get_minimum_time_not_found_string(void)
{
	assert_get_minimum_time_not_found(TEST_STD_ID_STRING);
}

void test_get_minimum_time_not_found_blob(void)
{
	assert_get_minimum_time_not_found(TEST_STD_ID_BLOB);
}

/* ---------------------------------------------------------------------------
 * Get Statistics
 * ------------------------------------------------------------------------ */
static void assert_get_statistics(uint64_t id, void (*add_samples_fn)(void),
                                  history_gluon_data_t *samples,
                                  uint64_t idx0, uint64_t num_samples,
                                  struct timespec *ts0, struct timespec *ts1,
                                  history_gluon_result_t expected_result)
{
	assert_make_context_delete_add_samples(id, add_samples_fn);

	/* get the minimum */
	history_gluon_statistics_t statistics;
	history_gluon_result_t ret;
	ret = history_gluon_get_statistics(g_ctx, id, ts0, ts1, &statistics);
	cut_assert_equal_int(expected_result, ret);
	if (expected_result != HGL_SUCCESS)
		return;

	/* test the obtained value */
	history_gluon_statistics_t expected_statistics;
	calc_statistics(samples, idx0, num_samples, &expected_statistics);
	assert_statistics(&expected_statistics, &statistics);
}

void test_get_statistics_uint(void)
{
	assert_get_statistics(TEST_STD_ID_UINT, assert_add_uint_samples,
	                      g_uint_samples, 0, NUM_UINT_SAMPLES,
	                      &HISTORY_GLUON_TIMESPEC_START,
	                      &HISTORY_GLUON_TIMESPEC_END,
	                      HGL_SUCCESS);
}

void test_get_statistics_float(void)
{
	assert_get_statistics(TEST_STD_ID_FLOAT, assert_add_float_samples,
                              g_float_samples, 0, NUM_FLOAT_SAMPLES,
	                      &HISTORY_GLUON_TIMESPEC_START,
	                      &HISTORY_GLUON_TIMESPEC_END,
	                      HGL_SUCCESS);
}

void test_get_statistics_uint_range(void)
{
	int idx0 = 1;
	int num = 3;
	assert_get_statistics(TEST_STD_ID_UINT, assert_add_uint_samples,
	                      g_uint_samples, idx0, num,
	                      &g_uint_samples[idx0].ts,
	                      &g_uint_samples[idx0+num].ts,
	                      HGL_SUCCESS);
}

void test_get_statistics_float_range(void)
{
	int idx0 = 1;
	int num = 3;
	assert_get_statistics(TEST_STD_ID_FLOAT, assert_add_float_samples,
	                      g_float_samples, idx0, num,
	                      &g_uint_samples[idx0].ts,
	                      &g_uint_samples[idx0+num].ts,
	                      HGL_SUCCESS);
}


void test_get_statistics_string(void)
{
	assert_get_statistics(TEST_STD_ID_STRING, assert_add_string_samples,
	                      g_string_samples, 0, NUM_STRING_SAMPLES,
	                      &HISTORY_GLUON_TIMESPEC_START,
	                      &HISTORY_GLUON_TIMESPEC_END,
	                      HGLSVERR_INVALID_DATA_TYPE);
}

void test_get_statistics_blob(void)
{
	assert_get_statistics(TEST_STD_ID_BLOB, assert_add_blob_samples,
	                      g_blob_samples, 0, NUM_BLOB_SAMPLES,
	                      &HISTORY_GLUON_TIMESPEC_START,
	                      &HISTORY_GLUON_TIMESPEC_END,
	                      HGLSVERR_INVALID_DATA_TYPE);
}

/* ---------------------------------------------------------------------------
 * Delete Data
 * ------------------------------------------------------------------------ */
void test_delete_all(void)
{
	create_global_context();

	int id = 1;
	assert_delete_all_for_id(id, NULL);

	// add 2 items
	struct timespec ts;
	ts.tv_sec = 10000;
	ts.tv_nsec = 0;
	assert_add_float(id, &ts, -1.2e5);

	ts.tv_sec = 10000;
	ts.tv_nsec = 10;
	assert_add_float(id, &ts, 3.14);

	uint64_t num_deleted;
	assert_delete_all_for_id(id, &num_deleted);
	cut_assert_equal_int(2, num_deleted);

	assert_delete_all_for_id(id, &num_deleted);
	cut_assert_equal_int(0, num_deleted);
}

static void asert_delete_common(uint64_t id, void (*add_samples_fn)(void),
                                struct timespec *ts,
                                history_gluon_delete_way_t delete_way,
                                uint64_t expected_num_deleted)
{
	assert_make_context_delete_add_samples(id, add_samples_fn);
	uint64_t num_deleted;
	history_gluon_result_t ret;
	ret = history_gluon_delete(g_ctx, id, ts, delete_way, &num_deleted);
	cut_assert_equal_int(HGL_SUCCESS, ret);
	cut_assert_equal_int_least64(expected_num_deleted, num_deleted);
}

/* EQUAL */
static void
assert_delete_eq(uint64_t num_samples, history_gluon_data_t *samples,
                 uint64_t id, void (*add_samples_fn)(void))
{
	uint64_t cut_idx = 3;
	uint64_t expected_num_deleted = 1;
	asert_delete_common(id, add_samples_fn, &samples[cut_idx].ts,
	                    HISTORY_GLUON_DELETE_TYPE_EQUAL,
	                    expected_num_deleted);
}

void test_delete_eq_uint(void)
{
	assert_delete_eq(NUM_UINT_SAMPLES, g_uint_samples,
	                 TEST_STD_ID_UINT, assert_add_uint_samples);
}

void test_delete_eq_float(void)
{
	assert_delete_eq(NUM_FLOAT_SAMPLES, g_float_samples,
	                 TEST_STD_ID_FLOAT, assert_add_float_samples);
}

void test_delete_eq_string(void)
{
	assert_delete_eq(NUM_STRING_SAMPLES, g_string_samples,
	                 TEST_STD_ID_STRING, assert_add_string_samples);
}

void test_delete_eq_blob(void)
{
	assert_delete_eq(NUM_BLOB_SAMPLES, g_blob_samples,
	                 TEST_STD_ID_BLOB, assert_add_blob_samples);
}

/* EQUAL_OR_LESS */
static void
assert_delete_eq_or_less(uint64_t num_samples, history_gluon_data_t *samples, uint64_t id,
                         void (*add_samples_fn)(void))
{
	uint64_t cut_idx = 3;
	uint64_t expected_num_deleted = cut_idx + 1;
	asert_delete_common(id, add_samples_fn, &samples[cut_idx].ts,
	                    HISTORY_GLUON_DELETE_TYPE_EQUAL_OR_LESS,
	                    expected_num_deleted);
}

void test_delete_eq_or_less_uint(void)
{
	assert_delete_eq_or_less(NUM_UINT_SAMPLES, g_uint_samples,
	                         TEST_STD_ID_UINT, assert_add_uint_samples);
}

void test_delete_eq_or_less_float(void)
{
	assert_delete_eq_or_less(NUM_FLOAT_SAMPLES, g_float_samples,
	                         TEST_STD_ID_FLOAT, assert_add_float_samples);
}

void test_delete_eq_or_less_string(void)
{
	assert_delete_eq_or_less(NUM_STRING_SAMPLES, g_string_samples,
	                         TEST_STD_ID_STRING, assert_add_string_samples);
}

void test_delete_eq_or_less_blob(void)
{
	assert_delete_eq_or_less(NUM_BLOB_SAMPLES, g_blob_samples,
	                         TEST_STD_ID_BLOB, assert_add_blob_samples);
}

/* LESS */
static void
assert_delete_less(uint64_t num_samples, history_gluon_data_t *samples,
                   uint64_t id, void (*add_samples_fn)(void))
{
	uint64_t cut_idx = 3;
	uint64_t expected_num_deleted = cut_idx;
	asert_delete_common(id, add_samples_fn, &samples[cut_idx].ts,
	                    HISTORY_GLUON_DELETE_TYPE_LESS,
	                    expected_num_deleted);
}

void test_delete_less_uint(void)
{
	assert_delete_less(NUM_UINT_SAMPLES, g_uint_samples,
	                   TEST_STD_ID_UINT, assert_add_uint_samples);
}

void test_delete_less_float(void)
{
	assert_delete_less(NUM_FLOAT_SAMPLES, g_float_samples,
	                   TEST_STD_ID_FLOAT, assert_add_float_samples);
}

void test_delete_less_string(void)
{
	assert_delete_less(NUM_STRING_SAMPLES, g_string_samples,
	                   TEST_STD_ID_STRING, assert_add_string_samples);
}

void test_delete_less_blob(void)
{
	assert_delete_less(NUM_BLOB_SAMPLES, g_blob_samples,
	                   TEST_STD_ID_BLOB, assert_add_blob_samples);
}

/* EQUAL_OR_GREATER */
static void
assert_delete_eq_or_gt(uint64_t num_samples, history_gluon_data_t *samples,
                       uint64_t id, void (*add_samples_fn)(void))
{
	uint64_t cut_idx = 3;
	uint64_t expected_num_deleted = num_samples - cut_idx;
	asert_delete_common(id, add_samples_fn, &samples[cut_idx].ts,
	                    HISTORY_GLUON_DELETE_TYPE_EQUAL_OR_GREATER,
	                    expected_num_deleted);
}

void test_delete_eq_or_gt_uint(void)
{
	assert_delete_eq_or_gt(NUM_UINT_SAMPLES, g_uint_samples,
	                       TEST_STD_ID_UINT, assert_add_uint_samples);
}

void test_delete_eq_or_gt_float(void)
{
	assert_delete_eq_or_gt(NUM_FLOAT_SAMPLES, g_float_samples,
	                       TEST_STD_ID_FLOAT, assert_add_float_samples);
}

void test_delete_eq_or_gt_string(void)
{
	assert_delete_eq_or_gt(NUM_STRING_SAMPLES, g_string_samples,
	                       TEST_STD_ID_STRING, assert_add_string_samples);
}

void test_delete_eq_or_gt_blob(void) {
	assert_delete_eq_or_gt(NUM_BLOB_SAMPLES, g_blob_samples,
	                       TEST_STD_ID_BLOB, assert_add_blob_samples);
}

/* GREATER */
static void
assert_delete_gt(uint64_t num_samples, history_gluon_data_t *samples, uint64_t id,
                 void (*add_samples_fn)(void))
{
	uint64_t cut_idx = 3;
	uint64_t expected_num_deleted = num_samples - cut_idx - 1;
	asert_delete_common(id, add_samples_fn, &samples[cut_idx].ts,
	                    HISTORY_GLUON_DELETE_TYPE_GREATER,
	                    expected_num_deleted);
}

void test_delete_gt_uint(void)
{
	assert_delete_gt(NUM_UINT_SAMPLES, g_uint_samples,
	                 TEST_STD_ID_UINT, assert_add_uint_samples);
}

void test_delete_gt_float(void)
{
	assert_delete_gt(NUM_FLOAT_SAMPLES, g_float_samples,
	                 TEST_STD_ID_FLOAT, assert_add_float_samples);
}

void test_delete_gt_string(void)
{
	assert_delete_gt(NUM_STRING_SAMPLES, g_string_samples,
	                 TEST_STD_ID_STRING, assert_add_string_samples);
}

void test_delete_gt_blob(void)
{
	assert_delete_gt(NUM_BLOB_SAMPLES, g_blob_samples,
	                 TEST_STD_ID_BLOB, assert_add_blob_samples);
}

/* not found */
static void
assert_delete_not_found(uint64_t num_samples, history_gluon_data_t *samples,
                        uint64_t id, void (*add_samples_fn)(void))
{
	uint64_t cut_idx = 3;
	uint64_t expected_num_deleted = 0;
	struct timespec ts;
	set_mean_ts(&samples[cut_idx].ts, &samples[cut_idx+1].ts, &ts);
	asert_delete_common(id, add_samples_fn, &ts,
	                    HISTORY_GLUON_DELETE_TYPE_EQUAL,
	                    expected_num_deleted);
}

void test_delete_not_found_uint(void)
{
	assert_delete_not_found(NUM_UINT_SAMPLES, g_uint_samples,
	                        TEST_STD_ID_UINT, assert_add_uint_samples);
}

void test_delete_not_found_float(void)
{
	assert_delete_not_found(NUM_FLOAT_SAMPLES, g_float_samples,
	                        TEST_STD_ID_FLOAT, assert_add_float_samples);
}

void test_delete_not_found_string(void)
{
	assert_delete_not_found(NUM_STRING_SAMPLES, g_string_samples,
	                        TEST_STD_ID_STRING, assert_add_string_samples);
}

void test_delete_not_found_blob(void)
{
	assert_delete_not_found(NUM_BLOB_SAMPLES, g_blob_samples,
	                        TEST_STD_ID_BLOB, assert_add_blob_samples);
}

