#include <cutter.h>
#include "history-gluon.h"

static history_gluon_context_t g_ctx = NULL;

/* Utility functions */
void create_global_context(void)
{
	g_ctx = history_gluon_create_context();
	cut_assert(g_ctx);
}

void free_global_context()
{
	history_gluon_free_context(g_ctx);
	g_ctx = NULL;
}

void delete_all_data(uint64_t id, uint32_t *num_deleted_entries)
{
	struct timespec ts;
	ts.tv_sec = 0xffffffff;
	ts.tv_nsec = 0xffffffff;
	int ret = history_gluon_delete_below_threshold(g_ctx, id, &ts,
	                                               num_deleted_entries);
	cut_assert_equal_int(0, ret);
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
	int ret = history_gluon_add_uint64(g_ctx, id, &ts, value);
	cut_assert_equal_int(0, ret);
}

void test_add_float(void)
{
	create_global_context();

	uint64_t id = 10;
	struct timespec ts;
	ts.tv_sec = 20;
	ts.tv_nsec = 40;
	double value = -10.5;
	int ret = history_gluon_add_float(g_ctx, id, &ts, value);
	cut_assert_equal_int(0, ret);
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
	delete_all_data(id, NULL);

	test_add_uint64();
	test_add_float();

	uint32_t num_deleted;
	delete_all_data(id, &num_deleted);
	cut_assert_equal_int(2, num_deleted);

	delete_all_data(id, &num_deleted);
	cut_assert_equal_int(0, num_deleted);
}


