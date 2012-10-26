#include <cutter.h>
#include "history-gluon.h"

static history_gluon_context_t g_ctx = NULL;

#define TEST_STD_ID  0x65536

/* Utility functions */
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

static void assert_delete_all_for_id(uint64_t id, uint32_t *num_deleted_entries)
{
	struct timespec ts;
	ts.tv_sec = 0xffffffff;
	ts.tv_nsec = 0xffffffff;
	int ret = history_gluon_delete_below_threshold(g_ctx, id, &ts,
	                                               num_deleted_entries);
	cut_assert_equal_int(0, ret);
}

static void assert_add_uint64(uint64_t id, struct timespec *ts, uint64_t value)
{
	int ret = history_gluon_add_uint64(g_ctx, id, ts, value);
	cut_assert_equal_int(0, ret);
}

static void assert_add_float(uint64_t id, struct timespec *ts, double v)
{
	int ret = history_gluon_add_float(g_ctx, id, ts, v);
	cut_assert_equal_int(0, ret);
}

static history_gluon_value_t g_float_samples[] = {
	{
		.id = TEST_STD_ID,
		.time.tv_sec = 1234567890,
		.time.tv_nsec = 123456789,
		.v_float = 0.1,
	},
	{
		.id = TEST_STD_ID,
		.time.tv_sec = 1506070800,
		.time.tv_nsec = 100000000,
		.v_float = 99.9,
	},
	{
		.id = TEST_STD_ID,
		.time.tv_sec = 1600000000,
		.time.tv_nsec = 500000000,
		.v_float = 100.0,
	},
	{
		.id = TEST_STD_ID,
		.time.tv_sec = 1600000001,
		.time.tv_nsec = 200000000,
		.v_float = -10.5,
	},
	{
		.id = TEST_STD_ID,
		.time.tv_sec = 2500000000,
		.time.tv_nsec = 300000000,
		.v_float = -3.2e5,
	},
};

static const int NUM_SAMPLES = sizeof(g_float_samples) / sizeof(history_gluon_value_t);

static void add_samples() {
	int i;
	for (i = 0; i < NUM_SAMPLES; i++) {
		assert_add_float(g_float_samples[i].id, &g_float_samples[i].time,
		                  g_float_samples[i].v_float);
	}
}

/* Teset cases */
void setup(void)
{
}

void teardown(void)
{
	if (g_ctx) {
		history_gluon_free_context(g_ctx);
		g_ctx = NULL;
	}
}

void test_create_context(void)
{
	create_global_context();
}

void test_free_context(void)
{
	create_global_context();
	free_global_context();
}

void test_add_uint64(void)
{
	create_global_context();

	uint64_t id = 1;
	struct timespec ts;
	ts.tv_sec = 1;
	ts.tv_nsec = 2;
	uint64_t value = 3;
	assert_add_uint64(id, &ts, value);
}

void test_add_float(void)
{
	create_global_context();

	uint64_t id = 10;
	struct timespec ts;
	ts.tv_sec = 20;
	ts.tv_nsec = 40;
	double value = -10.5;
	assert_add_float(id, &ts, value);
}

void test_add_string(void)
{
	create_global_context();

	uint64_t id = 100;
	struct timespec ts;
	ts.tv_sec = 300;
	ts.tv_nsec = 500;
	char value[] = "test_string";
	int ret = history_gluon_add_string(g_ctx, id, &ts, value);
	cut_assert_equal_int(0, ret);
}

void test_add_blob(void)
{
	create_global_context();

	uint64_t id = 1000;
	struct timespec ts;
	ts.tv_sec = 4300;
	ts.tv_nsec = 8500;
	uint8_t value[] = {0x21, 0x22, 0xff, 0x80, 0x95};
	int ret = history_gluon_add_blob(g_ctx, id, &ts, value, sizeof(value));
	cut_assert_equal_int(0, ret);
}

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

	uint32_t num_deleted;
	assert_delete_all_for_id(id, &num_deleted);
	cut_assert_equal_int(2, num_deleted);

	assert_delete_all_for_id(id, &num_deleted);
	cut_assert_equal_int(0, num_deleted);
}

void test_get_minimum_time(void)
{
	create_global_context();
	assert_delete_all_for_id(TEST_STD_ID, NULL);
	add_samples();

	// get the minimum
	struct timespec ts;
	int ret = history_gluon_get_minmum_time(g_ctx, TEST_STD_ID, &ts);
	cut_assert_equal_int(0, ret);

	// test the obtained time
	cut_assert_equal_int(g_float_samples[0].time.tv_sec, ts.tv_sec);
}

void test_delete_below_threshold(void)
{
	create_global_context();
	assert_delete_all_for_id(TEST_STD_ID, NULL);
	add_samples();

	// delete below threshold
	int cut_idx = 3;
	struct timespec ts;
	memcpy(&ts, &g_float_samples[cut_idx].time, sizeof(struct timespec));
	uint32_t num_deleted;
	int ret = history_gluon_delete_below_threshold(g_ctx, TEST_STD_ID, &ts,
	                                               &num_deleted);
	cut_assert_equal_int(0, ret);
	cut_assert_equal_int(cut_idx, num_deleted);
}


