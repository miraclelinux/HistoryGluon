#include <cutter.h>
#include "history-gluon.h"

void test_create_context (void)
{
	history_gluon_context_t ctx = history_gluon_create_context();
	cut_assert(ctx);
}



