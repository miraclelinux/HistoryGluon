#ifndef _history_gluon_h_
#include <stdint.h>
#include <time.h>

typedef void * history_gluon_context_t;

typedef enum {
	HISTORY_GLUON_TYPE_FLOAT  = 0,
	HISTORY_GLUON_TYPE_STRING = 1,
	HISTORY_GLUON_TYPE_UINT64 = 2,
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
		uint64_t  v_uint64;
		uint8_t * v_blob;
	};
	uint64_t length;
}
history_gluon_data_t;

typedef struct
{
	uint64_t num_data;
	history_gluon_sort_order_t sort_order;
	history_gluon_data_t *array;
}
history_gluon_data_array_t;

/**
 * Create a History Gluon's context.
 *
 * @return A context used for History Gluon's APIs on success.
 */
history_gluon_context_t history_gluon_create_context(void);

/**
 * Destroy History Gluon's context.
 *
 * @param context A History Gluon's context to be freed.
 */
void history_gluon_free_context(history_gluon_context_t context);

/**
 * Add interger type data.
 *
 * @param context A History Gluon's context.
 * @param id A data ID.
 * @param ts A ts of the data
 * @param data A value of the data.
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_add_uint64(history_gluon_context_t context,
                             uint64_t id, struct timespec *ts, uint64_t data);

/**
 * Add floating-point type data.
 *
 * @param context A History Gluon's context.
 * @param id A data ID.
 * @param ts A ts of the data.
 * @param data A value of the data.
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_add_float(history_gluon_context_t context,
                            uint64_t id, struct timespec *ts,
                            double data);

/**
 * Add string type data.
 *
 * @param context A History Gluon's context.
 * @param id A data ID.
 * @param ts A ts of the data.
 * @param data A null-terminated string.
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_add_string(history_gluon_context_t context,
                             uint64_t id, struct timespec *ts, char *data);

/**
 * Add blob type data.
 *
 * @param context A History Gluon's context.
 * @param id A data ID.
 * @param ts A ts of the data.
 * @param data A pointer to the data.
 * @param length The size of the data.
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_add_blob(history_gluon_context_t context,
                           uint64_t id, struct timespec *timespec,
                           uint8_t *data, uint64_t length);

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
 *                   variable's pointer is stored.
 *                   The caller must call history_gluon_free_data() when
 *                   no longer needed.
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_query(history_gluon_context_t context,
                        uint64_t id, struct timespec *ts,
                        history_gluon_query_t query_type,
                        history_gluon_data_t **gluon_data);

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
 * @param array An address where the created history_gluon_data_array_t
 *              variable is stored..
 *              The caller must call history_gluon_free_data_array()
 *              when no longer needed.
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_range_query(history_gluon_context_t context, uint64_t id,
                              struct timespec *ts0, struct timespec *ts1,
                              history_gluon_sort_order_t sort_request,
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
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_get_minmum_time(history_gluon_context_t context,
                                  uint64_t id, struct timespec *minimum_time);

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
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_get_statistics(history_gluon_context_t context, uint64_t id,
                                 struct timespec *ts0, struct timespec *ts1,
                                 history_gluon_statistics_t *statistics);

/**
 * Delete data
 *
 * @param context A History Gluon's context.
 * @param id A data ID.
 * @param ts The ts to be used to select deleted data.
 * @param delete_way A type of deletion.
 * @param num_deleted_entries A pointer in which the number of deleted data
 *                            is stored. It can be NULL when the number is
 *                            not needed.
 * @return 0 if success. -1 if error occured.
 *         When the error, \num_deleted_entries is not changed.
 */
int history_gluon_delete(history_gluon_context_t context, uint64_t id,
                         struct timespec *ts,
                         history_gluon_delete_way_t delete_way,
                         uint64_t *num_deleted_entries);


#endif // _history_gluon_h_
