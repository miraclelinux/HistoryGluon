#ifndef _history_gluon_h_
#include <stdint.h>
#include <time.h>

typedef void * history_gluon_context_t;

enum {
	HISTORY_GLUON_TYPE_FLOAT  = 0,
	HISTORY_GLUON_TYPE_STRING = 1,
	HISTORY_GLUON_TYPE_UINT64 = 2,
	HISTORY_GLUON_TYPE_BLOB   = 3,
};

typedef struct
{
	uint64_t itemid;
	struct timespec time0;
	struct timespec time1;
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
	struct timespec time;
	int type;
	union {
		double    v_float;
		char *    v_string;
		uint64_t  v_uint64;
		uint8_t * v_blob;
	};
	int length;
}
history_gluon_value_t;

typedef struct
{
	uint64_t length;
	history_gluon_value_t *array;
}
history_gluon_value_array_t;

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
 * @param id An item ID.
 * @param time A timestamp of the item.
 * @param value A value of the item.
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_add_uint64(history_gluon_context_t context,
                             uint64_t id, struct timespec *time, uint64_t value);

/**
 * Add floating-point type data.
 *
 * @param context A History Gluon's context.
 * @param id An item ID.
 * @param time A timestamp of the item.
 * @param value A value of the item.
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_add_float(history_gluon_context_t context,
                            uint64_t id, struct timespec *time, double value);

/**
 * Add string type data.
 *
 * @param context A History Gluon's context.
 * @param id An item ID.
 * @param time A timestamp of the item.
 * @param value A value of the item.
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_add_string(history_gluon_context_t context,
                             uint64_t id, struct timespec *time, char *valu);

/**
 * Add blob type data.
 *
 * @param context A History Gluon's context.
 * @param id An item ID.
 * @param time A timestamp of the item.
 * @param value A value of the item.
 * @param length A value of the item.
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_add_blob(history_gluon_context_t context,
                           uint64_t id, struct timespec *time, uint8_t *value,
                           uint64_t length);

/**
 * Get the minimum time in items with the specified ID.
 *
 * @param context A History Gluon's context.
 * @param id An item ID.
 * @param minimum_time A pointer in which the minium time of the item with
 * the specfied ID is returned.
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_get_minmum_time(history_gluon_context_t context,
                                  uint64_t id, struct timespec *minimum_time);

/**
 * Delete items whose time is below the specified threshold.
 * The items with the same as the threshold is
 *
 * @param context A History Gluon's context.
 * @param id An item ID.
 * @param threshold The threshold.
 * @param num_deleted_entries A pointer in which the number of deleted items is stored,
 *                            or NULL when it is not needed. 
 * @return 0 if success. -1 if error occured. When the error, \num_deleted_entries is
 *         not changed.
 */
int history_gluon_delete_below_threshold(history_gluon_context_t context,
                                         uint64_t id, struct timespec *threshold,
                                         uint32_t *num_deleted_entries);

/**
 * Get a history-value array with the specified item and the interval.
 *
 * @param context A History Gluon's context.
 * @param id An item ID.
 * @param time0 A start time of the interval. The item with the \time0 is included.
 * @param time1 An end time of the interval. The item with the \time1 is NOT included.
 * @param array A pointer to an allocated history_gluon_value_array_t-type variable.
 *              The caller must call history_gluon_free_value_array()
 *              when no longer needed.
 * @return 0 if success. -1 if error occured. When the error, \num_entries, \type, and
 *         \array are not changed.
 */
int history_gluon_range_query(history_gluon_context_t context, uint64_t id,
                              struct timespec *time0, struct timespec *time1,
                              history_gluon_value_array_t *array);
/**
 * Free a history-value array.
 *
 * @param context A History Gluon's context.
 * @param array An array that is obtained by \history_gluon_range_query().
 */
void history_gluon_free_value_array(history_gluon_context_t context,
                                    history_gluon_value_array_t *array);

/**
 * Get a history value with the specified item and the time.
 *
 * @param context A History Gluon's context.
 * @param id An item ID.
 * @param time A time of the history value.
 * @param search_near If this is 1 and the item with the specified time was not found,
 *                    the item with near time is returned.
 * @param value An pointer to an allocated history_gluon_valut_t-type variable.
 *              The caller must call history_gluon_free_value() when no longer needed.
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_query(history_gluon_context_t context,
                        uint64_t id, struct timespec *time, int search_near,
                        history_gluon_value_t *value);

/**
 * Free a history value.
 *
 * @param context A History Gluon's context.
 * @param value A value that is obtained by \history_gluon_query().
 */
void history_gluon_free_value(history_gluon_context_t context,
                              history_gluon_value_t *value);

/**
 * Get statistical information of the items with given ID and the time interval.
 *
 * @param context A History Gluon's context.
 * @param id An item ID.
 * @param time0 A start time of the interval. The item with the time is included.
 * @param time1 An end time of the interval. The item with the time is NOT included.
 * @param statistics A pointer in which the result is returned.
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_get_statistics(history_gluon_context_t context, uint64_t id,
                                 struct timespec *time0, struct timespec *time1,
                                 history_gluon_statistics_t *statistics);

#endif // _history_gluon_h_
