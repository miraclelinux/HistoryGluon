#include <cutter.h>
#include "test-utils.h"
#include "history-gluon.h"

static history_gluon_data_t samples_query_diff_ns[] = {
	{
		.id = TEST_STD_ID_UINT,
		.type = HISTORY_GLUON_TYPE_UINT,
		.ts.tv_sec = 0x20000000,
		.ts.tv_nsec = 0,
		.v_uint = 0x22223333,
	},
	{
		.id = TEST_STD_ID_UINT,
		.type = HISTORY_GLUON_TYPE_UINT,
		.ts.tv_sec = 0x20000000,
		.ts.tv_nsec = 3,
		.v_uint = 0x9a9a8b8b,
	},
	{
		.id = TEST_STD_ID_UINT,
		.type = HISTORY_GLUON_TYPE_UINT,
		.ts.tv_sec = 0x25010101,
		.ts.tv_nsec = 200345003,
		.v_uint = 0x9a9a8b8b00,
	},
};

static int NUM_SAMPLES_QUERY_DIFF_NS =
  sizeof(samples_query_diff_ns)/sizeof(history_gluon_data_t);

static void add_query_diff_ns_samples(void)
{
	assert_add_samples_with_data(NUM_SAMPLES_QUERY_DIFF_NS,
	                             samples_query_diff_ns);
}

/* ---------------------------------------------------------------------------
 * Teset cases
 * ------------------------------------------------------------------------- */
void setup(void)
{
}

void teardown(void)
{
	cleanup_global_data();
}

void test_range_query_diff_ns(void)
{
	assert_make_context_delete_add_samples(TEST_STD_ID_UINT,
	                                       add_query_diff_ns_samples);
	int start_idx = 0;
	int num_samples = 2;
	struct timespec *ts0 = &samples_query_diff_ns[start_idx].ts;
	struct timespec *ts1 =
	  &samples_query_diff_ns[start_idx + num_samples -1].ts;
	uint64_t expected_num_entries = num_samples - 1;
	uint64_t expected_first_idx = start_idx;
	asset_range_query_common(TEST_STD_ID_UINT, samples_query_diff_ns,
	                         ts0, ts1,
	                         HISTORY_GLUON_SORT_ASCENDING, 
	                         HISTORY_GLUON_NUM_ENTRIES_UNLIMITED,
	                         expected_num_entries, expected_first_idx);
}
