#include <cutter.h>
#include "history-gluon.h"

#define TEST_STD_ID_UINT   0x10
#define TEST_STD_ID_FLOAT  0x65536
#define TEST_STD_ID_STRING 0x87654321
#define TEST_STD_ID_BLOB   0x123456789abcdef

/* --------------------------------------------------------------------------------------
 * Global variables
 * ----------------------------------------------------------------------------------- */
static history_gluon_context_t g_ctx = NULL;
static history_gluon_data_t *g_data = NULL;
static history_gluon_data_array_t *g_array = NULL;

/* --------------------------------------------------------------------------------------
 * Utility functions
 * ----------------------------------------------------------------------------------- */
static void create_global_context(void)
{
	g_ctx = history_gluon_create_context();
	cut_assert(g_ctx);
}

static void free_global_context()
{
	history_gluon_free_context(g_ctx);
	g_ctx = NULL;
}

static void assert_delete_all_for_id(uint64_t id, uint64_t *num_deleted)
{
	struct timespec ts = {0, 0};
	history_gluon_result_t ret;
	ret = history_gluon_delete(g_ctx, id, &ts,
	                           HISTORY_GLUON_DELTE_TYPE_EQUAL_OR_GREATER,
	                           num_deleted);
	cut_assert_equal_int(HGL_SUCCESS, ret);
}

static void assert_add_uint(uint64_t id, struct timespec *ts, uint64_t value)
{
	history_gluon_result_t ret = history_gluon_add_uint(g_ctx, id, ts, value);
	cut_assert_equal_int(HGL_SUCCESS, ret);
}

static void assert_add_uint_hgl_data(history_gluon_data_t *gluon_data)
{
	assert_add_uint(gluon_data->id, &gluon_data->ts, gluon_data->v_uint);
}

static void assert_add_float(uint64_t id, struct timespec *ts, double v)
{
	history_gluon_result_t ret = history_gluon_add_float(g_ctx, id, ts, v);
	cut_assert_equal_int(HGL_SUCCESS, ret);
}

static void assert_add_float_hgl_data(history_gluon_data_t *gluon_data)
{
	assert_add_float(gluon_data->id, &gluon_data->ts, gluon_data->v_float);
}

static void assert_add_string(uint64_t id, struct timespec *ts, char *v)
{
	history_gluon_result_t ret = history_gluon_add_string(g_ctx, id, ts, v);
	cut_assert_equal_int(HGL_SUCCESS, ret);
}

static void assert_add_string_hgl_data(history_gluon_data_t *gluon_data)
{
	assert_add_string(gluon_data->id, &gluon_data->ts, gluon_data->v_string);
}

static void assert_add_blob(uint64_t id, struct timespec *ts, uint8_t *v, uint64_t len)
{
	history_gluon_result_t ret = history_gluon_add_blob(g_ctx, id, ts, v, len);
	cut_assert_equal_int(HGL_SUCCESS, ret);
}

static void assert_add_blob_hgl_data(history_gluon_data_t *gluon_data)
{
	assert_add_blob(gluon_data->id, &gluon_data->ts, gluon_data->v_blob,
	                gluon_data->length);
}

static history_gluon_data_t g_uint_samples[] = {
	{
		.id = TEST_STD_ID_UINT,
		.ts.tv_sec = 1234567890,
		.ts.tv_nsec = 123456789,
		.v_uint = 1,
	},
	{
		.id = TEST_STD_ID_UINT,
		.ts.tv_sec = 1506070800,
		.ts.tv_nsec = 100000000,
		.v_uint = 10,
	},
	{
		.id = TEST_STD_ID_UINT,
		.ts.tv_sec = 1600000000,
		.ts.tv_nsec = 500000000,
		.v_uint = 12340,
	},
	{
		.id = TEST_STD_ID_UINT,
		.ts.tv_sec = 1600000001,
		.ts.tv_nsec = 200000000,
		.v_uint = 0x123456789abcdef0,
	},
	{
		.id = TEST_STD_ID_UINT,
		.ts.tv_sec = 2500000000,
		.ts.tv_nsec = 300000000,
		.v_uint = 0xfedcba9876543210,
	},
};
static const int NUM_UINT_SAMPLES =
  sizeof(g_uint_samples) / sizeof(history_gluon_data_t);

static history_gluon_data_t g_float_samples[] = {
	{
		.id = TEST_STD_ID_FLOAT,
		.ts.tv_sec = 1234567890,
		.ts.tv_nsec = 123456789,
		.v_float = 0.1,
	},
	{
		.id = TEST_STD_ID_FLOAT,
		.ts.tv_sec = 1506070800,
		.ts.tv_nsec = 100000000,
		.v_float = 99.9,
	},
	{
		.id = TEST_STD_ID_FLOAT,
		.ts.tv_sec = 1600000000,
		.ts.tv_nsec = 500000000,
		.v_float = 100.0,
	},
	{
		.id = TEST_STD_ID_FLOAT,
		.ts.tv_sec = 1600000001,
		.ts.tv_nsec = 200000000,
		.v_float = -10.5,
	},
	{
		.id = TEST_STD_ID_FLOAT,
		.ts.tv_sec = 2500000000,
		.ts.tv_nsec = 300000000,
		.v_float = -3.2e5,
	},
};
static const int NUM_FLOAT_SAMPLES =
  sizeof(g_float_samples) / sizeof(history_gluon_data_t);

static history_gluon_data_t g_string_samples[] = {
	{
		.id = TEST_STD_ID_STRING,
		.ts.tv_sec = 1234567890,
		.ts.tv_nsec = 123456789,
		.v_string = "Hello, World!",
	},
	{
		.id = TEST_STD_ID_STRING,
		.ts.tv_sec = 1506070800,
		.ts.tv_nsec = 100000000,
		.v_string =
		  "Linux is a Unix-like computer operating system assembled under the "
		  "model of free and open source software development and distribution. "
		  "The defining component of Linux is the Linux kernel, an operating "
		  "system kernel first released 5 October 1991 by Linus Torvalds.",
	},
	{
		.id = TEST_STD_ID_STRING,
		.ts.tv_sec = 1600000000,
		.ts.tv_nsec = 500000000,
		.v_string =
		  "C++ (pronounced \"see plus plus\") is a statically typed, free-form, "
		  "multi-paradigm, compiled, general-purpose programming language. It is "
		  "regarded as an intermediate-level language, as it comprises a "
		  "combination of both high-level and low-level language features. "
		  "Developed by Bjarne Stroustrup starting in 1979 at Bell Labs, it adds "
		  "object oriented features, such as classes, and other enhancements to "
		  "the C programming language. Originally named C with Classes, the "
		  "language was renamed C++ in 1983, as a pun involving the increment "
		  "operator.\n",
	},
	{
		.id = TEST_STD_ID_STRING,
		.ts.tv_sec = 1600000001,
		.ts.tv_nsec = 200000000,
		.v_string = "walrus",
	},
	{
		.id = TEST_STD_ID_STRING,
		.ts.tv_sec = 2500000000,
		.ts.tv_nsec = 300000000,
		.v_string = "Are you hungry?",
	},
};
static const int NUM_STRING_SAMPLES =
  sizeof(g_string_samples) / sizeof(history_gluon_data_t);

static uint8_t blob_sample0[] = {0x21, 0x08, 0x05};
static uint8_t blob_sample1[] = {0x21, 0xf8, 0x25, 0x88, 0x99, 0xaa};
static uint8_t blob_sample2[] = {0xc0, 0x72, 0x01, 0x99};
static uint8_t blob_sample3[] = {0xff};
static uint8_t blob_sample4[] = {0x2f, 0x53, 0x45, 0x25, 0x83, 0xab, 0x58, 0x88,
                                 0x10, 0x09, 0xc0, 0xde, 0xfe, 0x83, 0x2a, 0xcc, };

static history_gluon_data_t g_blob_samples[] = {
	{
		.id = TEST_STD_ID_BLOB,
		.ts.tv_sec = 1234567890,
		.ts.tv_nsec = 123456789,
		.v_blob = blob_sample0,
		.length = sizeof(blob_sample0) / sizeof(uint8_t),
	},
	{
		.id = TEST_STD_ID_BLOB,
		.ts.tv_sec = 1506070800,
		.ts.tv_nsec = 100000000,
		.v_blob = blob_sample1,
		.length = sizeof(blob_sample1) / sizeof(uint8_t),
	},
	{
		.id = TEST_STD_ID_BLOB,
		.ts.tv_sec = 1600000000,
		.ts.tv_nsec = 500000000,
		.v_blob = blob_sample2,
		.length = sizeof(blob_sample2) / sizeof(uint8_t),
	},
	{
		.id = TEST_STD_ID_BLOB,
		.ts.tv_sec = 1600000001,
		.ts.tv_nsec = 200000000,
		.v_blob = blob_sample3,
		.length = sizeof(blob_sample3) / sizeof(uint8_t),
	},
	{
		.id = TEST_STD_ID_BLOB,
		.ts.tv_sec = 2500000000,
		.ts.tv_nsec = 300000000,
		.v_blob = blob_sample4,
		.length = sizeof(blob_sample4) / sizeof(uint8_t),
	},
};
static const int NUM_BLOB_SAMPLES =
  sizeof(g_blob_samples) / sizeof(history_gluon_data_t);

static void assert_add_uint_samples() {
	int i;
	for (i = 0; i < NUM_UINT_SAMPLES; i++)
		assert_add_uint_hgl_data(&g_uint_samples[i]);
}

static void add_float_samples() {
	int i;
	for (i = 0; i < NUM_FLOAT_SAMPLES; i++)
		assert_add_float_hgl_data(&g_float_samples[i]);
}

static void add_string_samples() {
	int i;
	for (i = 0; i < NUM_STRING_SAMPLES; i++)
		assert_add_string_hgl_data(&g_string_samples[i]);
}

static void add_blob_samples() {
	int i;
	for (i = 0; i < NUM_BLOB_SAMPLES; i++)
		assert_add_blob_hgl_data(&g_blob_samples[i]);
}

static void assert_equal_hgl_data(history_gluon_data_t *expect, history_gluon_data_t *actual)
{
	cut_assert_equal_int_least64(expect->id, actual->id);
	cut_assert_equal_int_least32(expect->ts.tv_sec, actual->ts.tv_nsec);
	cut_assert_equal_int(expect->type, actual->type);
	if (expect->type == HISTORY_GLUON_TYPE_FLOAT) {
		double err = 0.0;
		cut_assert_equal_double(expect->v_float, err, actual->v_float);
	} else if(expect->type == HISTORY_GLUON_TYPE_STRING) {
		cut_assert_equal_string(expect->v_string, actual->v_string);
		cut_assert_equal_int_least64(expect->length, actual->length);
	} else if(expect->type == HISTORY_GLUON_TYPE_UINT) {
		cut_assert_equal_int_least64(expect->v_uint, actual->v_uint);
	} else if(expect->type == HISTORY_GLUON_TYPE_BLOB) {
		cut_assert_equal_memory(expect->v_blob, expect->length,
		                        actual->v_blob, actual->length);
	} else
		cut_fail("Unknown type: %d", expect->type);
}

static void set_mean_ts(struct timespec *ts0, struct timespec *ts1, struct timespec *ts)
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

/* --------------------------------------------------------------------------------------
 * Teset cases
 * ----------------------------------------------------------------------------------- */
void setup(void)
{
}

void teardown(void)
{
	if (g_data) {
		history_gluon_free_data(g_ctx, g_data);
		g_data = NULL;
	}
	if (g_array) {
		history_gluon_free_data_array(g_ctx, g_array);
		g_array = NULL;
	}
	if (g_ctx) {
		history_gluon_free_context(g_ctx);
		g_ctx = NULL;
	}
}
/* --------------------------------------------------------------------------------------
 * Context
 * ----------------------------------------------------------------------------------- */
void test_create_context(void)
{
	create_global_context();
}

void test_free_context(void)
{
	create_global_context();
	free_global_context();
}

/* --------------------------------------------------------------------------------------
 * Add Data
 * ----------------------------------------------------------------------------------- */
void test_add_uint(void)
{
	create_global_context();

	uint64_t id = 114;
	assert_delete_all_for_id(id, NULL);

	struct timespec ts;
	ts.tv_sec = 1;
	ts.tv_nsec = 2;
	uint64_t value = 3;
	assert_add_uint(id, &ts, value);
}

void test_add_float(void)
{
	create_global_context();

	uint64_t id = 126;
	assert_delete_all_for_id(id, NULL);

	struct timespec ts;
	ts.tv_sec = 20;
	ts.tv_nsec = 40;
	double value = -10.5;
	assert_add_float(id, &ts, value);
}

void test_add_string(void)
{
	create_global_context();

	uint64_t id = 138;
	assert_delete_all_for_id(id, NULL);

	struct timespec ts;
	ts.tv_sec = 300;
	ts.tv_nsec = 500;
	char value[] = "test_string";
	assert_add_string(id, &ts, value);
}

void test_add_blob(void)
{
	create_global_context();

	uint64_t id = 151;
	assert_delete_all_for_id(id, NULL);

	struct timespec ts;
	ts.tv_sec = 4300;
	ts.tv_nsec = 8500;
	uint8_t value[] = {0x21, 0x22, 0xff, 0x80, 0x95};
	assert_add_blob(id, &ts, value, sizeof(value));
}

/* --------------------------------------------------------------------------------------
 * Query
 * ----------------------------------------------------------------------------------- */
/* uint */
void test_add_uint_and_query(void)
{
	create_global_context();

	// delete and add data
	int idx = 2;
	history_gluon_data_t *sample = &g_uint_samples[idx];
	assert_delete_all_for_id(sample->id, NULL);
	assert_add_uint(sample->id, &sample->ts, sample->v_uint);

	// query
	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, sample->id, &sample->ts,
	                          HISTORY_GLUON_QUERY_TYPE_ONLY_MATCH, &g_data);
	cut_assert_equal_int(HGL_SUCCESS, ret);
	cut_assert_equal_int_least64(sample->v_uint, g_data->v_uint);
}

void test_add_uint_and_query_not_found(void)
{
	create_global_context();

	// delete and add data
	int idx = 1;
	history_gluon_data_t *sample = &g_uint_samples[idx];
	assert_delete_all_for_id(sample->id, NULL);
	assert_delete_all_for_id(sample->id+1, NULL);
	assert_add_uint(sample->id, &sample->ts, sample->v_uint);

	// query
	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, sample->id+1, &sample->ts,
	                          HISTORY_GLUON_QUERY_TYPE_ONLY_MATCH, &g_data);
	cut_assert_equal_int(HGLERR_NOT_FOUND, ret);
}

void test_add_uint_and_query_less(void)
{
	create_global_context();

	// delete and add data
	assert_delete_all_for_id(TEST_STD_ID_UINT, NULL);
	assert_add_uint_samples();

	// query
	int idx = 1;
	struct timespec ts;
	set_mean_ts(&g_uint_samples[idx].ts, &g_uint_samples[idx+1].ts, &ts);

	// query
	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, TEST_STD_ID_UINT, &ts,
	                          HISTORY_GLUON_QUERY_TYPE_LESS_DATA, &g_data);
	cut_assert_equal_int(HGL_SUCCESS, ret);
	cut_assert_equal_int_least64(g_uint_samples[idx].v_uint, g_data->v_uint);
}

void test_add_uint_and_query_greater(void)
{
	create_global_context();

	// delete and add data
	assert_delete_all_for_id(TEST_STD_ID_UINT, NULL);
	assert_add_uint_samples();

	// query
	int idx = 1;
	struct timespec ts;
	set_mean_ts(&g_uint_samples[idx].ts, &g_uint_samples[idx+1].ts, &ts);

	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, TEST_STD_ID_UINT, &ts,
	                          HISTORY_GLUON_QUERY_TYPE_GREATER_DATA, &g_data);
	cut_assert_equal_int(HGL_SUCCESS, ret);
	cut_assert_equal_int_least64(g_uint_samples[idx+1].v_uint, g_data->v_uint);
}

/* float */
void test_add_float_and_query(void)
{
	create_global_context();

	// delete and add data
	int idx = 2;
	history_gluon_data_t *sample = &g_float_samples[idx];
	assert_delete_all_for_id(sample->id, NULL);
	assert_add_float(sample->id, &sample->ts, sample->v_float);

	// query
	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, sample->id, &sample->ts,
	                          HISTORY_GLUON_QUERY_TYPE_ONLY_MATCH, &g_data);
	cut_assert_equal_int(HGL_SUCCESS, ret);
	double err = 0.0;
	cut_assert_equal_double(sample->v_float, err, g_data->v_float);
}

void test_add_float_and_query_not_found(void)
{
	create_global_context();

	// delete and add data
	int idx = 1;
	history_gluon_data_t *sample = &g_float_samples[idx];
	assert_delete_all_for_id(sample->id, NULL);
	assert_delete_all_for_id(sample->id+1, NULL);
	assert_add_float_hgl_data(sample);

	// query
	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, sample->id+1, &sample->ts,
	                          HISTORY_GLUON_QUERY_TYPE_ONLY_MATCH, &g_data);
	cut_assert_equal_int(HGLERR_NOT_FOUND, ret);
}

void test_add_float_and_query_less(void)
{
	create_global_context();

	// delete and add data
	assert_delete_all_for_id(TEST_STD_ID_FLOAT, NULL);
	add_float_samples();

	// query
	int idx = 1;
	struct timespec ts;
	set_mean_ts(&g_float_samples[idx].ts, &g_float_samples[idx+1].ts, &ts);

	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, TEST_STD_ID_FLOAT, &ts,
	                          HISTORY_GLUON_QUERY_TYPE_LESS_DATA, &g_data);
	cut_assert_equal_int(HGL_SUCCESS, ret);
	double err = 0.0;
	cut_assert_equal_double(g_float_samples[idx].v_float, err, g_data->v_float);
}

void test_add_float_and_query_greater(void)
{
	create_global_context();

	// delete and add data
	assert_delete_all_for_id(TEST_STD_ID_FLOAT, NULL);
	add_float_samples();

	// query
	int idx = 1;
	struct timespec ts;
	set_mean_ts(&g_float_samples[idx].ts, &g_float_samples[idx+1].ts, &ts);

	// query
	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, TEST_STD_ID_FLOAT, &ts,
	                          HISTORY_GLUON_QUERY_TYPE_GREATER_DATA, &g_data);
	cut_assert_equal_int(HGL_SUCCESS, ret);
	double err = 0.0;
	cut_assert_equal_double(g_float_samples[idx+1].v_float, err, g_data->v_float);
}

/* string */
void test_add_string_and_query(void)
{
	create_global_context();

	// delete and add data
	int idx = 2;
	history_gluon_data_t *sample = &g_string_samples[idx];
	assert_delete_all_for_id(sample->id, NULL);
	assert_add_string(sample->id, &sample->ts, sample->v_string);

	// query
	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, sample->id, &sample->ts,
	                          HISTORY_GLUON_QUERY_TYPE_ONLY_MATCH, &g_data);
	cut_assert_equal_int(HGL_SUCCESS, ret);
	cut_assert_equal_string(sample->v_string, g_data->v_string);
}

void test_add_string_and_query_not_found(void)
{
	create_global_context();

	// delete and add data
	int idx = 1;
	history_gluon_data_t *sample = &g_string_samples[idx];
	assert_delete_all_for_id(sample->id, NULL);
	assert_delete_all_for_id(sample->id+1, NULL);
	assert_add_string(sample->id, &sample->ts, sample->v_string);

	// query
	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, sample->id+1, &sample->ts,
	                          HISTORY_GLUON_QUERY_TYPE_ONLY_MATCH, &g_data);
	cut_assert_equal_int(HGLERR_NOT_FOUND, ret);
}

void test_add_string_and_query_less(void)
{
	create_global_context();

	// delete and add data
	assert_delete_all_for_id(TEST_STD_ID_STRING, NULL);
	add_string_samples();

	// query
	int idx = 1;
	struct timespec ts;
	set_mean_ts(&g_string_samples[idx].ts, &g_string_samples[idx+1].ts, &ts);

	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, TEST_STD_ID_STRING, &ts,
	                          HISTORY_GLUON_QUERY_TYPE_LESS_DATA, &g_data);
	cut_assert_equal_int(HGL_SUCCESS, ret);
	cut_assert_equal_string(g_string_samples[idx].v_string, g_data->v_string);
}

void test_add_string_and_query_greater(void)
{
	create_global_context();

	// delete and add data
	assert_delete_all_for_id(TEST_STD_ID_STRING, NULL);
	add_string_samples();

	// query
	int idx = 1;
	struct timespec ts;
	set_mean_ts(&g_string_samples[idx].ts, &g_string_samples[idx+1].ts, &ts);

	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, TEST_STD_ID_STRING, &ts,
	                          HISTORY_GLUON_QUERY_TYPE_GREATER_DATA, &g_data);
	cut_assert_equal_int(HGL_SUCCESS, ret);
	cut_assert_equal_string(g_string_samples[idx+1].v_string, g_data->v_string);
}

/* blob */
void test_add_blob_and_query(void)
{
	create_global_context();

	// delete and add data
	int idx = 2;
	history_gluon_data_t *sample = &g_blob_samples[idx];
	assert_delete_all_for_id(sample->id, NULL);
	assert_add_blob(sample->id, &sample->ts, sample->v_blob, sample->length);

	// query
	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, sample->id, &sample->ts,
	                          HISTORY_GLUON_QUERY_TYPE_ONLY_MATCH, &g_data);
	cut_assert_equal_int(HGL_SUCCESS, ret);
	cut_assert_equal_memory(sample->v_blob, sample->length,
	                        g_data->v_blob, g_data->length);
}

void test_add_blob_and_query_not_found(void)
{
	create_global_context();

	// delete and add data
	int idx = 1;
	history_gluon_data_t *sample = &g_blob_samples[idx];
	assert_delete_all_for_id(sample->id, NULL);
	assert_delete_all_for_id(sample->id+1, NULL);
	assert_add_blob(sample->id, &sample->ts, sample->v_blob, sample->length);

	// query
	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, sample->id+1, &sample->ts,
	                          HISTORY_GLUON_QUERY_TYPE_ONLY_MATCH, &g_data);
	cut_assert_equal_int(HGLERR_NOT_FOUND, ret);
}

void test_add_blob_and_query_less(void)
{
	create_global_context();

	// delete and add data
	assert_delete_all_for_id(TEST_STD_ID_BLOB, NULL);
	add_blob_samples();

	// query
	int idx = 1;
	struct timespec ts;
	set_mean_ts(&g_blob_samples[idx].ts, &g_blob_samples[idx+1].ts, &ts);

	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, TEST_STD_ID_BLOB, &ts,
	                          HISTORY_GLUON_QUERY_TYPE_LESS_DATA, &g_data);
	cut_assert_equal_int(HGL_SUCCESS, ret);
	cut_assert_equal_memory(g_blob_samples[idx].v_blob, g_blob_samples[idx].length,
	                        g_data->v_blob, g_data->length);
}

void test_add_blob_and_query_greater(void)
{
	create_global_context();

	// delete and add data
	assert_delete_all_for_id(TEST_STD_ID_BLOB, NULL);
	add_blob_samples();

	// query
	int idx = 1;
	struct timespec ts;
	set_mean_ts(&g_blob_samples[idx].ts, &g_blob_samples[idx+1].ts, &ts);

	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, TEST_STD_ID_BLOB, &ts,
	                          HISTORY_GLUON_QUERY_TYPE_GREATER_DATA, &g_data);
	cut_assert_equal_int(HGL_SUCCESS, ret);
	cut_assert_equal_memory(g_blob_samples[idx+1].v_blob,
	                        g_blob_samples[idx+1].length,
	                        g_data->v_blob, g_data->length);
}

/* --------------------------------------------------------------------------------------
 * Range Query
 * ----------------------------------------------------------------------------------- */
void test_range_query_uint_asc(void)
{
	create_global_context();
	assert_delete_all_for_id(TEST_STD_ID_UINT, NULL);
	assert_add_uint_samples();

	// range query
	int idx0 = 1;
	int idx1 = 3;
	history_gluon_result_t ret;
	ret = history_gluon_range_query(g_ctx, TEST_STD_ID_UINT,
	                                &g_uint_samples[idx0].ts,
	                                &g_uint_samples[idx1].ts,
	                                HISTORY_GLUON_SORT_ASCENDING, 
	                                HISTORY_GLUON_NUM_ENTRIES_UNLIMITED,
	                                &g_array);
	cut_assert_equal_int(HGL_SUCCESS, ret);

	// assertion
	cut_assert_equal_int_least64(idx1 - idx0, g_array->num_data);
	uint64_t i;
	for  (i = 0; i < g_array->num_data; i++) {
		history_gluon_data_t *expect_data = &g_uint_samples[i];
		assert_equal_hgl_data(expect_data, g_array->array[i]);
	}
}

/* --------------------------------------------------------------------------------------
 * Get Minimum Time
 * ----------------------------------------------------------------------------------- */
void test_get_minimum_time(void)
{
	create_global_context();
	assert_delete_all_for_id(TEST_STD_ID_FLOAT, NULL);
	add_float_samples();

	// get the minimum
	struct timespec ts;
	history_gluon_result_t ret;
	ret = history_gluon_get_minmum_time(g_ctx, TEST_STD_ID_FLOAT, &ts);
	cut_assert_equal_int(HGL_SUCCESS, ret);

	// test the obtained time
	cut_assert_equal_int(g_float_samples[0].ts.tv_sec, ts.tv_sec);
}

/* --------------------------------------------------------------------------------------
 * Get Statistics
 * ----------------------------------------------------------------------------------- */

/* --------------------------------------------------------------------------------------
 * Delete Data
 * ----------------------------------------------------------------------------------- */
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

void test_delete_less(void)
{
	create_global_context();
	assert_delete_all_for_id(TEST_STD_ID_FLOAT, NULL);
	add_float_samples();

	// delete below threshold
	int cut_idx = 3;
	struct timespec ts;
	memcpy(&ts, &g_float_samples[cut_idx].ts, sizeof(struct timespec));
	uint64_t num_deleted;
	history_gluon_result_t ret;
	ret = history_gluon_delete(g_ctx, TEST_STD_ID_FLOAT, &ts,
	                           HISTORY_GLUON_DELTE_TYPE_LESS,
	                           &num_deleted);
	cut_assert_equal_int(HGL_SUCCESS, ret);
	cut_assert_equal_int(cut_idx, num_deleted);
}
