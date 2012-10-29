#ifndef _history_gluon_h_
#include <stdint.h>
#include <time.h>

typedef void * history_gluon_context_t;

typedef enum {
	HISTORY_GLUON_TYPE_INIT   = -1,
	HISTORY_GLUON_TYPE_FLOAT  = 0,
	HISTORY_GLUON_TYPE_STRING = 1,
	HISTORY_GLUON_TYPE_UINT   = 2,
	HISTORY_GLUON_TYPE_BLOB   = 3,
} history_gluon_data_type_t;

typedef enum {
	HISTORY_GLUON_QUERY_TYPE_ONLY_MATCH   = 0,
	HISTORY_GLUON_QUERY_TYPE_LESS_DATA    = 1,
	HISTORY_GLUON_QUERY_TYPE_GREATER_DATA = 2,
} history_gluon_query_t;

typedef enum {
	HISTORY_GLUON_SORT_ASCENDING  = 0,
	HISTORY_GLUON_SORT_DESDENDING = 1,
	HISTORY_GLUON_SORT_NOT_SORTED = 2,
} history_gluon_sort_order_t;

typedef enum {
	HISTORY_GLUON_DELTE_TYPE_ONLY_MATCH       = 0,
	HISTORY_GLUON_DELTE_TYPE_EQUAL_OR_LESS    = 1,
	HISTORY_GLUON_DELTE_TYPE_LESS             = 2,
	HISTORY_GLUON_DELTE_TYPE_EQUAL_OR_GREATER = 3,
	HISTORY_GLUON_DELTE_TYPE_GREATER          = 4,
} history_gluon_delete_way_t;

enum {
	HISTORY_GLUON_QUERY_NOT_FOUND  = 0,
	HISTORY_GLUON_QUERY_DATA_FOUND = 1,
};

#define HISTORY_GLUON_NUM_ENTRIES_UNLIMITED 0;

typedef struct
{
	uint64_t id;
	struct timespec ts0;
	struct timespec ts1;
	uint64_t count;
	uint64_t min;
	uint64_t max;
	double sum;
	double average;
	double delta;
}
history_gluon_statistics_t;

typedef struct
{
	uint64_t id;
	struct timespec ts;
	history_gluon_data_type_t type;
	union {
		double    v_float;
		char *    v_string;
		uint64_t  v_uint;
		uint8_t * v_blob;
	};

	/**
	 * This variable has a lenght of v_string or v_blob.
	 * When type is HISTORY_GLUON_TYPE_STRING, length doen't include
	 * NULL terminator. */
	uint64_t length;
}
history_gluon_data_t;

typedef struct
{
	uint64_t num_data;
	history_gluon_sort_order_t sort_order;
	history_gluon_data_t **array;
}
history_gluon_data_array_t;

typedef enum {
	HGL_SUCCESS                  = 0,
	HGLERR_UNKNOWN_REASON        = -1,
	HGLERR_NOT_IMPLEMENTED       = -2,
	HGLERR_MEM_ALLOC             = -3,
	HGLERR_NOT_FOUND             = -4,
	HGLERR_READ_STREAM_END       = -100,
	HGLERR_READ_ERROR            = -101,
	HGLERR_WRITE_ERROR           = -200,
	HGLERR_TOO_LONG_STRING       = -300,
	HGLERR_TOO_LARGE_BLOB        = -301,
	HGLERR_GETADDRINFO           = -400,
	HGLERR_FAILED_CONNECT        = -401,
	HGLERR_UNEXPECTED_REPLY_SIZE = -500,
	HGLERR_UNEXPECTED_REPLY_TYPE = -501,
	HGLERR_REPLY_ERROR           = -502,
	HGLERR_INVALID_DATA_TYPE     = -600,
} history_gluon_result_t;

/**
 * Create a History Gluon's context.
 *
 * @return A context used for History Gluon's APIs on success.
 */
history_gluon_context_t
history_gluon_create_context(void);

/**
 * Destroy History Gluon's context.
 *
 * @param context A History Gluon's context to be freed.
 */
void
history_gluon_free_context(history_gluon_context_t context);

/**
 * Add interger type data.
 *
 * @param context A History Gluon's context.
 * @param id A data ID.
 * @param ts A ts of the data
 * @param data A value of the data.
 * @return \HGL_SUCCESS on success. If an error occured, the code is returned.
 */
history_gluon_result_t
history_gluon_add_uint(history_gluon_context_t context,
                       uint64_t id, struct timespec *ts, uint64_t data);

/**
 * Add floating-point type data.
 *
 * @param context A History Gluon's context.
 * @param id A data ID.
 * @param ts A ts of the data.
 * @param data A value of the data.
 * @return \HGL_SUCCESS on success. If an error occured, the code is returned.
 */
history_gluon_result_t
history_gluon_add_float(history_gluon_context_t context,
                        uint64_t id, struct timespec *ts, double data);

/**
 * Add string type data.
 *
 * @param context A History Gluon's context.
 * @param id A data ID.
 * @param ts A ts of the data.
 * @param data A null-terminated string.
 * @return \HGL_SUCCESS on success. If an error occured, the code is returned.
 */
history_gluon_result_t
history_gluon_add_string(history_gluon_context_t context,
                         uint64_t id, struct timespec *ts, char *data);

/**
 * Add blob type data.
 *
 * @param context A History Gluon's context.
 * @param id A data ID.
 * @param ts A ts of the data.
 * @param data A pointer to the data.
 * @param length The size of the data.
 * @return \HGL_SUCCESS on success. If an error occured, the code is returned.
 */
history_gluon_result_t
history_gluon_add_blob(history_gluon_context_t context, uint64_t id,
                       struct timespec *timespec, uint8_t *data, uint64_t length);

/**
 * Query data with the specified ID and the ts.
 *
 * @param context A History Gluon's context.
 * @param id A data ID.
 * @param ts A time to be queried.
 * @param query_type A behavior when there is no matched data. If this
 *                   parameter is \HISORY_GLUON_QUERY_TYPE_ONLY_MATCH,
 *                   none of data is returned.
 *                   If it is \HISTORY_GLUON_QUERY_TYPE_LESS_DATA and
 *                   \HISTORY_GLUON_QUERY_TYPE_GREATER_DATA, the most near
 *                   less and greater data is respectively returned.
 * @param gluon_data An address where the created history_gluon_data_t
 *                   variable's pointer is stored. If the matched data is
 *                   not found, NULL is stored.
 *                   If non NULL \gluon_data was returned,
 *                   the caller must call history_gluon_free_data() when
 *                   no longer needed.
 * @return \HGL_SUCCESS on success. If an error occured, the code is returned.
 */
history_gluon_result_t
history_gluon_query(history_gluon_context_t context, uint64_t id, struct timespec *ts,
                    history_gluon_query_t query_type, history_gluon_data_t **gluon_data);

/**
 * Free history_gluon_data_t variable .
 *
 * @param context A History Gluon's context.
 * @param gluon_data A pointer to a history_gluon_data_t variable that is
 *                   obtained by \history_gluon_query().
 */
void history_gluon_free_data(history_gluon_context_t context,
                             history_gluon_data_t *gluon_data);

/**
 * Get data array with the specified ID and the interval.
 *
 * @param context A History Gluon's context.
 * @param id A data ID.
 * @param ts0 A start time of the interval.
 *                   Data with the \time0 is included.
 * @param ts1 An end time of the interval.
 *                   Data with the \time1 is NOT included.
 * @param sort_request A request of the sort order.
 * @param num_max_entries The number of maximum entries to be returned.
 *                        If HISTORY_GLUON_NUM_ENTRIES_UNLIMITED is specified,
 *                        all entries will be returned.
 * @param array An address where the created history_gluon_data_array_t
 *              variable is stored..
 *              The caller must call history_gluon_free_data_array()
 *              when no longer needed.
 * @return \HGL_SUCCESS on success. If an error occured, the code is returned.
 */
history_gluon_result_t
history_gluon_range_query(history_gluon_context_t context, uint64_t id,
                          struct timespec *ts0, struct timespec *ts1,
                          history_gluon_sort_order_t sort_request,
                          uint64_t num_max_entries,
                          history_gluon_data_array_t **array);
/**
 * Free a history-data array.
 *
 * @param context A History Gluon's context.
 * @param array An array that is obtained by \history_gluon_range_query().
 */
void history_gluon_free_data_array(history_gluon_context_t context,
                                   history_gluon_data_array_t *array);


/**
 * Get the minimum time of the data with the specified ID.
 *
 * @param context A History Gluon's context.
 * @param id A data ID.
 * @param minimum_time A pointer in which the minium time of the data with
 *                     the specfied ID is returned.
 * @return \HGL_SUCCESS on success. If an error occured, the code is returned.
 */
history_gluon_result_t
history_gluon_get_minmum_time(history_gluon_context_t context,
                              uint64_t id, struct timespec *minimum_ts);

/**
 * Get statistical information of the data with the specified ID
 * and the time interval.
 *
 * @param context A History Gluon's context.
 * @param id A data ID.
 * @param ts0 A start time of the interval.
 *                   The item with the time is included.
 * @param ts1 An end time of the interval.
 *                   The item with the time is NOT included.
 * @param statistics A pointer in which the result is returned.
 * @return \HGL_SUCCESS on success. If an error occured, the code is returned.
 */
history_gluon_result_t
history_gluon_get_statistics(history_gluon_context_t context, uint64_t id,
                             struct timespec *ts0, struct timespec *ts1,
                             history_gluon_statistics_t *statistics);

/**
 * Delete data
 *
 * @param context A History Gluon's context.
 * @param id A data ID.
 * @param ts A timestamp to be used to select deleted data.
 * @param delete_way A type of the deletion.
 * @param num_deleted_entries A pointer in which the number of deleted data
 *                            is stored. It can be NULL when the number is
 *                            not needed.
 * @return \HGL_SUCCESS on success. If an error occured, the code is returned.
 */
history_gluon_result_t
history_gluon_delete(history_gluon_context_t context, uint64_t id, struct timespec *ts,
                     history_gluon_delete_way_t delete_way, uint64_t *num_deleted_entries);

#endif // _history_gluon_h_
