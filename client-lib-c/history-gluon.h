#ifndef _history_gluon_h_
#include <stdint.h>
#include <time.h>

typedef void * history_gluon_context_t;

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
                             uint64_t id, struct timespec *time, char *value);

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
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_delete_below_threshold(history_gluon_context_t context,
                                         uint64_t id, struct timespec *threshold,
                                         uint32_t *num_deleted_entries);

/**
 * Get statistical information of the items with given ID and the time interval.
 *
 * @param context A History Gluon's context.
 * @param id An item ID.
 * @param result A pointer in which the result is returned.
 * @param time0 A start time of the interval. The item with the time is included.
 * @param time1 An end time of the interval. The item with the time is NOT included.
 * @return 0 if success. -1 if error occured.
 */
int history_gluon_get_statistics(history_gluon_context_t context,
                                 uint64_t id, history_gluon_statistics_t *result,
                                 struct timespec *time0, struct timespec *time1);

#endif // _history_gluon_h_
