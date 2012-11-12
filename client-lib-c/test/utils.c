#include <cutter.h>
#include <glib.h>
#include "utils.h"

/* ---------------------------------------------------------------------------
 * Global variables
 * ------------------------------------------------------------------------- */
history_gluon_context_t g_ctx = NULL;
history_gluon_data_t *g_data = NULL;
history_gluon_data_array_t *g_array = NULL;
GTree *g_gtree = NULL;

/* ---------------------------------------------------------------------------
 * Private functions
 * ------------------------------------------------------------------------- */

/* ---------------------------------------------------------------------------
 * Public functions
 * ------------------------------------------------------------------------- */
void create_global_context(void)
{
	history_gluon_result_t ret;
	ret = history_gluon_create_context("test", NULL, 0, &g_ctx);
	cut_assert_equal_int(HGL_SUCCESS, ret);
	cut_assert(g_ctx);
}

void free_global_context(void)
{
	history_gluon_free_context(g_ctx);
	g_ctx = NULL;
}

void cleanup_global_data(void)
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
	if (g_gtree) {
		g_tree_unref(g_gtree);
		g_gtree = NULL;
	}
}

void assert_delete_all_for_id(uint64_t id, uint64_t *num_deleted)
{
	struct timespec ts = {0, 0};
	history_gluon_result_t ret;
	ret = history_gluon_delete(g_ctx, id, &ts,
	                           HISTORY_GLUON_DELETE_TYPE_EQUAL_OR_GREATER,
	                           num_deleted);
	cut_assert_equal_int(HGL_SUCCESS, ret);
}

void assert_make_context_delete_add_samples(uint64_t id,
                                            void (*add_samples_fn)(void))
{
	create_global_context();
	assert_delete_all_for_id(id, NULL);
	(*add_samples_fn)();
}

void assert_add_uint(uint64_t id, struct timespec *ts, uint64_t value)
{
	history_gluon_result_t ret =
	  history_gluon_add_uint(g_ctx, id, ts, value);
	cut_assert_equal_int(HGL_SUCCESS, ret);
}

void assert_add_float(uint64_t id, struct timespec *ts, double v)
{
	history_gluon_result_t ret = history_gluon_add_float(g_ctx, id, ts, v);
	cut_assert_equal_int(HGL_SUCCESS, ret);
}

void assert_add_string(uint64_t id, struct timespec *ts, char *v)
{
	history_gluon_result_t ret = history_gluon_add_string(g_ctx, id, ts, v);
	cut_assert_equal_int(HGL_SUCCESS, ret);
}

void assert_add_blob(uint64_t id, struct timespec *ts, uint8_t *v, uint64_t len)
{
	history_gluon_result_t ret =
	  history_gluon_add_blob(g_ctx, id, ts, v, len);
	cut_assert_equal_int(HGL_SUCCESS, ret);
}

void assert_add_uint_hgl_data(history_gluon_data_t *gluon_data)
{
	assert_add_uint(gluon_data->id, &gluon_data->ts, gluon_data->v.uint);
}

void assert_add_float_hgl_data(history_gluon_data_t *gluon_data)
{
	assert_add_float(gluon_data->id, &gluon_data->ts, gluon_data->v.fp);
}

void assert_add_string_hgl_data(history_gluon_data_t *gluon_data)
{
	assert_add_string(gluon_data->id, &gluon_data->ts, gluon_data->v.string);
}

void assert_add_blob_hgl_data(history_gluon_data_t *gluon_data)
{
	assert_add_blob(gluon_data->id, &gluon_data->ts, gluon_data->v.blob,
	                gluon_data->length);
}

void assert_add_hgl_data(history_gluon_data_t *gluon_data)
{
	if (gluon_data->type == HISTORY_GLUON_TYPE_FLOAT)
		assert_add_float_hgl_data(gluon_data);
	else if (gluon_data->type == HISTORY_GLUON_TYPE_STRING)
		assert_add_string_hgl_data(gluon_data);
	else if (gluon_data->type == HISTORY_GLUON_TYPE_UINT)
		assert_add_uint_hgl_data(gluon_data);
	else if (gluon_data->type == HISTORY_GLUON_TYPE_BLOB)
		assert_add_blob_hgl_data(gluon_data);
	else
		cut_fail("Unknown type: %d", gluon_data->type);
}

void
assert_add_samples_with_data(uint64_t num, history_gluon_data_t *sample_array)
{
	uint64_t i;
	for (i = 0; i < num; i++)
		assert_add_hgl_data(&sample_array[i]);
}

/* verify */
void assert_equal_hgl_data(history_gluon_data_t *expect,
                           history_gluon_data_t *actual)
{
	cut_assert_equal_int_least64(expect->id, actual->id);
	cut_assert_equal_int_least32(expect->ts.tv_sec, actual->ts.tv_sec);
	cut_assert_equal_int_least32(expect->ts.tv_nsec, actual->ts.tv_nsec);
	cut_assert_equal_int(expect->type, actual->type);
	if (expect->type == HISTORY_GLUON_TYPE_FLOAT) {
		double err = 0.0;
		cut_assert_equal_double(expect->v.fp, err, actual->v.fp);
	} else if(expect->type == HISTORY_GLUON_TYPE_STRING) {
		cut_assert_equal_string(expect->v.string, actual->v.string);
		cut_assert_equal_int_least64(expect->length, actual->length);
	} else if(expect->type == HISTORY_GLUON_TYPE_UINT) {
		cut_assert_equal_int_least64(expect->v.uint, actual->v.uint);
	} else if(expect->type == HISTORY_GLUON_TYPE_BLOB) {
		cut_assert_equal_memory(expect->v.blob, expect->length,
		                        actual->v.blob, actual->length);
	} else
		cut_fail("Unknown type: %d", expect->type);
}

/* query */
void
assert_query(uint64_t id, struct timespec *ts,
             history_gluon_query_t query_type, int expected_result)
{
	history_gluon_result_t ret;
	ret = history_gluon_query(g_ctx, id, ts, query_type, &g_data);
	cut_assert_equal_int(expected_result, ret);
}

void
assert_add_uint_and_query_verify(uint64_t id, struct timespec *ts,
                                 uint64_t value)
{
	assert_add_uint(id, ts, value);
	assert_query(id, ts, HISTORY_GLUON_QUERY_TYPE_ONLY_MATCH, HGL_SUCCESS);
	cut_assert_equal_int_least64(value, g_data->v.uint);
}

void
assert_add_float_and_query_verify(uint64_t id, struct timespec *ts,
                                  double value)
{
	assert_add_float(id, ts, value);
	assert_query(id, ts, HISTORY_GLUON_QUERY_TYPE_ONLY_MATCH, HGL_SUCCESS);
	double err = 0.0;
	cut_assert_equal_double(value, err, g_data->v.fp);
}

void
assert_add_string_and_query_verify(uint64_t id, struct timespec *ts,
                                   char *value)
{
	assert_add_string(id, ts, value);
	assert_query(id, ts, HISTORY_GLUON_QUERY_TYPE_ONLY_MATCH, HGL_SUCCESS);
	cut_assert_equal_string(value, g_data->v.string);
}

void
assert_add_blob_and_query_verify(uint64_t id, struct timespec *ts,
                                 uint8_t *value, uint64_t length)
{
	assert_add_blob(id, ts, value, length);
	assert_query(id, ts, HISTORY_GLUON_QUERY_TYPE_ONLY_MATCH, HGL_SUCCESS);
	cut_assert_equal_memory(value, length,
	                        g_data->v.blob, g_data->length);
}

void
asset_range_query_common(uint64_t id, history_gluon_data_t *samples,
                         struct timespec *ts0, struct timespec *ts1,
                         history_gluon_sort_order_t sort_order,
                         uint64_t num_max_entries,
                         uint64_t num_expected_entries,
                         uint64_t expected_first_idx)
{
	history_gluon_result_t ret;
	ret = history_gluon_range_query(g_ctx, id, ts0, ts1, sort_order,
	                                num_max_entries, &g_array);
	cut_assert_equal_int(HGL_SUCCESS, ret);

	// assertion
	cut_assert_equal_int_least64(num_expected_entries, g_array->num_data);
	uint64_t i;
	for  (i = 0; i < g_array->num_data; i++) {
		int exp_idx = expected_first_idx + i;
		if (sort_order == HISTORY_GLUON_SORT_DESCENDING) {
			exp_idx = expected_first_idx
			          + (g_array->num_data - i - 1);
		}
		history_gluon_data_t *expect_data = &samples[exp_idx];
		history_gluon_data_t *actual_data = g_array->array[i];
		assert_equal_hgl_data(expect_data, actual_data);
	}
}

/* sample comparision function */
gint sample_compare_func(gconstpointer a, gconstpointer b, gpointer user_data)
{
	/* ID is given priory over time */
	history_gluon_data_t *data0 = (history_gluon_data_t *)a;
	history_gluon_data_t *data1 = (history_gluon_data_t *)b;

	if (data0->id < data1->id)
		return -1;
	if (data0->id > data1->id)
		return 1;
	if (data0->ts.tv_sec < data1->ts.tv_sec)
		return -1;
	if (data0->ts.tv_sec > data1->ts.tv_sec)
		return 1;
	if (data0->ts.tv_nsec < data1->ts.tv_nsec)
		return -1;
	if (data0->ts.tv_sec > data1->ts.tv_nsec)
		return 1;
	return 0;
}

void *make_sample_key(history_gluon_data_t *gluon_data)
{
	return gluon_data;
}

void sample_key_destory_func(gpointer data)
{
	/* Nothing to do.
	 * The key is just point the gluon data that is also registered
	 * as the value. the data is freed in sample_value_destroy_func().
	 */
}

void sample_value_destroy_func(gpointer data)
{
	history_gluon_free_data(g_ctx, data);
}
