#include <cutter.h>
#include "test-utils.h"

/* --------------------------------------------------------------------------------------
 * Global variables
 * ----------------------------------------------------------------------------------- */
history_gluon_context_t g_ctx = NULL;
history_gluon_data_t *g_data = NULL;
history_gluon_data_array_t *g_array = NULL;

/* --------------------------------------------------------------------------------------
 * Private functions
 * ----------------------------------------------------------------------------------- */

/* --------------------------------------------------------------------------------------
 * Public functions
 * ----------------------------------------------------------------------------------- */
void create_global_context(void)
{
	g_ctx = history_gluon_create_context();
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
}

