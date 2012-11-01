#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#include "php.h"
#include "history-gluon.h"
#include "history-gluon-php-ext.h"

#define KEY_DATA_ARRAY_SORT_ORDER "sort_order"
#define KEY_DATA_ARRAY_ARRAY      "array"

#define KEY_DATA_ID               "id"
#define KEY_DATA_TIME_SEC         "sec"
#define KEY_DATA_TIME_NS          "ns"
#define KEY_DATA_TYPE             "type"
#define KEY_DATA_VALUE            "value"
#define KEY_DATA_LENGTH           "length"

static zend_function_entry php_history_gluon_functions[] = {
    PHP_FE(history_gluon_create_context, NULL)
    PHP_FE(history_gluon_free_context, NULL)
    PHP_FE(history_gluon_add_uint, NULL)
    PHP_FE(history_gluon_add_float, NULL)
    PHP_FE(history_gluon_add_string, NULL)
    PHP_FE(history_gluon_add_blob, NULL)
    PHP_FE(history_gluon_range_query, NULL)
    PHP_FE(history_gluon_delete, NULL)
    {NULL, NULL, NULL}
};

zend_module_entry php_history_gluon_module_entry = {
#if ZEND_MODULE_API_NO >= 20010901
    STANDARD_MODULE_HEADER,
#endif
    PHP_HISTORY_GLUON_NAME,
    php_history_gluon_functions,
    PHP_MINIT(history_gluon),
    PHP_MSHUTDOWN(history_gluon),
    PHP_RINIT(history_gluon),
    NULL,
    NULL,
#if ZEND_MODULE_API_NO >= 20010901
    PHP_HISTORY_GLUON_VERSION,
#endif
    STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_HISTORY_GLUON
ZEND_GET_MODULE(php_history_gluon)
#endif

/* -------------------------------------------------------------------------------------
 * Static member and functions
 * ---------------------------------------------------------------------------------- */
static history_gluon_context_t *g_ctx = NULL;;

static history_gluon_result_t validate_context(history_gluon_context_t ctx)
{
	if (!g_ctx || ctx != g_ctx)
		return HGLERR_INVALID_CONTEXT;
	return HGL_SUCCESS;
}

static zval *create_gluon_data_zval(history_gluon_data_t *gluon_data)
{
	zval *data;
	ALLOC_INIT_ZVAL(data);
	array_init(data);
	add_assoc_long(data, KEY_DATA_ID,       gluon_data->id);
	add_assoc_long(data, KEY_DATA_TIME_SEC, gluon_data->ts.tv_sec);
	add_assoc_long(data, KEY_DATA_TIME_NS,  gluon_data->ts.tv_nsec);
	add_assoc_long(data, KEY_DATA_TYPE ,    gluon_data->type);

	static const int DUPLICATE = 1;
	if (gluon_data->type == HISTORY_GLUON_TYPE_FLOAT)
		add_assoc_double(data, KEY_DATA_VALUE, gluon_data->v_float);
	else if (gluon_data->type == HISTORY_GLUON_TYPE_UINT)
		add_assoc_long(data, KEY_DATA_VALUE,   gluon_data->v_uint);
	else if (gluon_data->type == HISTORY_GLUON_TYPE_STRING) {
		add_assoc_stringl(data,KEY_DATA_VALUE, gluon_data->v_string,
		                  gluon_data->length, DUPLICATE);
	} else if (gluon_data->type == HISTORY_GLUON_TYPE_BLOB) {
		add_assoc_stringl(data, KEY_DATA_VALUE, (char*)gluon_data->v_blob,
		                  gluon_data->length, DUPLICATE);
	} else {
		fprintf(stderr, "%s: %d: Unknown data type: %d",
		        __FILE__, __LINE__, gluon_data->type);
	}

	add_assoc_long(data, KEY_DATA_LENGTH ,  gluon_data->length);

	return data;
}

/* -------------------------------------------------------------------------------------
 * Exported functions
 * ---------------------------------------------------------------------------------- */
PHP_MINIT_FUNCTION(history_gluon)
{
	return SUCCESS;
}

PHP_MSHUTDOWN_FUNCTION(history_gluon)
{
	return SUCCESS;
}

PHP_RINIT_FUNCTION(history_gluon)
{
	return SUCCESS;
}

PHP_FUNCTION(history_gluon_create_context)
{
	if (!g_ctx)
		g_ctx = history_gluon_create_context();
	RETURN_LONG((long)g_ctx);
}

PHP_FUNCTION(history_gluon_free_context)
{
	if (!g_ctx) {
		history_gluon_free_context(g_ctx);
		g_ctx = NULL;
	}
}

PHP_FUNCTION(history_gluon_add_uint)
{
	/* get arguments */
	long l_ctx, l_id, l_sec, l_ns, l_data; 
	int pret;
	pret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lllll",
	                             &l_ctx, &l_id, &l_sec, &l_ns, &l_data);
	if (pret == FAILURE)
		RETURN_NULL();
	
	history_gluon_context_t ctx = (history_gluon_context_t *)l_ctx;
	uint64_t id = l_id;
	struct timespec ts = {l_sec, l_ns};
	uint64_t data = l_data;

	int ret = validate_context(ctx);
	if (ret != HGL_SUCCESS)
		RETURN_LONG(ret);

	/* call the library function and return the return. */
	RETURN_LONG(history_gluon_add_uint(ctx, id, &ts, data));
}

PHP_FUNCTION(history_gluon_add_float)
{
	/* get arguments */
	long l_ctx, l_id, l_sec, l_ns;
	double d_data;
	int pret;
	pret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lllld",
	                             &l_ctx, &l_id, &l_sec, &l_ns, &d_data);
	if (pret == FAILURE)
		RETURN_NULL();
	
	history_gluon_context_t ctx = (history_gluon_context_t *)l_ctx;
	uint64_t id = l_id;
	struct timespec ts = {l_sec, l_ns};
	double data = d_data;

	int ret = validate_context(ctx);
	if (ret != HGL_SUCCESS)
		RETURN_LONG(ret);

	/* call the library function and return the return. */
	RETURN_LONG(history_gluon_add_float(ctx, id, &ts, data));
}

PHP_FUNCTION(history_gluon_add_string)
{
	/* get arguments */
	long l_ctx, l_id, l_sec, l_ns;
	int l_length;
	char *s_data; 
	int pret;
	pret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lllls",
	                             &l_ctx, &l_id, &l_sec, &l_ns, &s_data, &l_length);
	if (pret == FAILURE)
		RETURN_NULL();
	
	history_gluon_context_t ctx = (history_gluon_context_t *)l_ctx;
	uint64_t id = l_id;
	struct timespec ts = {l_sec, l_ns};

	int ret = validate_context(ctx);
	if (ret != HGL_SUCCESS)
		RETURN_LONG(ret);

	/* TODO: should we check if the string is UTF-8. Or fails or convert */

	/* call the library function and return the return. */
	RETURN_LONG(history_gluon_add_string(ctx, id, &ts, s_data));
}

PHP_FUNCTION(history_gluon_add_blob)
{
	/* get arguments */
	long l_ctx, l_id, l_sec, l_ns;
	int l_length;
	char *s_data; 
	int pret;
	pret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lllls",
	                             &l_ctx, &l_id, &l_sec, &l_ns, &s_data, &l_length);
	if (pret == FAILURE)
		RETURN_NULL();
	
	history_gluon_context_t ctx = (history_gluon_context_t *)l_ctx;
	uint64_t id = l_id;
	struct timespec ts = {l_sec, l_ns};
	uint8_t *data = (uint8_t*)s_data;
	uint64_t length = l_length;

	int ret = validate_context(ctx);
	if (ret != HGL_SUCCESS)
		RETURN_LONG(ret);

	/* call the library function and return the return. */
	RETURN_LONG(history_gluon_add_blob(ctx, id, &ts, data, length));
}

PHP_FUNCTION(history_gluon_range_query)
{
	/* get arguments */
	long l_ctx, l_id, l_sec0, l_ns0, l_sec1, l_ns1;
	long l_sort_request, l_max_entries;
	zval *z_array;
	int pret;
	pret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "llllllllz",
	                             &l_ctx, &l_id, &l_sec0, &l_ns0,
	                             &l_sec1, &l_ns1, &l_sort_request,
	                             &l_max_entries, &z_array);
	if (pret == FAILURE)
		RETURN_NULL();
	
	history_gluon_context_t ctx = (history_gluon_context_t *)l_ctx;
	uint64_t id = l_id;
	struct timespec ts0 = {l_sec0, l_ns0};
	struct timespec ts1 = {l_sec1, l_ns1};
	history_gluon_sort_order_t sort_request = l_sort_request;
	uint64_t num_max_entries = l_max_entries;
	history_gluon_data_array_t *array;

	history_gluon_result_t ret;
	ret = validate_context(ctx);
	if (ret != HGL_SUCCESS)
		RETURN_LONG(ret);

	/* call the library function. */
	ret = history_gluon_range_query(ctx, id, &ts0, &ts1, sort_request,
	                                num_max_entries, &array);
	if (ret != HGL_SUCCESS)
		RETURN_LONG((long)ret);

	/*
	 * make data for return
	 */

	/* data type */
	array_init(z_array);
	add_assoc_long(z_array, KEY_DATA_ARRAY_SORT_ORDER, array->sort_order);

	/* data array */
	zval *arr_arr;
	ALLOC_INIT_ZVAL(arr_arr);
	array_init(arr_arr);
	uint64_t i;
	for (i = 0; i < array->num_data; i++)
		add_index_zval(arr_arr, i, create_gluon_data_zval(array->array[i]));
	add_assoc_zval(z_array, KEY_DATA_ARRAY_ARRAY, arr_arr);

	/* free gluon_data_array */
	history_gluon_free_data_array(ctx, array);
	
	RETURN_LONG((long)ret);
}

PHP_FUNCTION(history_gluon_delete)
{
	/* get arguments */
	long l_ctx, l_id, l_sec, l_ns, l_del_way;
	zval *z_num_deleted;
	int pret;
	pret = zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "lllllz",
	                             &l_ctx, &l_id, &l_sec, &l_ns, &l_del_way,
	                             &z_num_deleted);
	if (pret == FAILURE)
		RETURN_NULL();
	
	history_gluon_context_t ctx = (history_gluon_context_t *)l_ctx;
	uint64_t id = l_id;
	struct timespec ts = {l_sec, l_ns};
	history_gluon_delete_way_t delete_way = l_del_way;
	uint64_t num_deleted;

	history_gluon_result_t ret;
	ret = validate_context(ctx);
	if (ret != HGL_SUCCESS)
		RETURN_LONG(ret);

	/* call the library function and return the return. */
	ret = history_gluon_delete(ctx, id, &ts, delete_way, &num_deleted);
	ZVAL_LONG(z_num_deleted, num_deleted);
	RETURN_LONG((long)ret);
}
