#ifndef _test_uitls_h
#define _test_uitls_h

#include "history-gluon.h"

#define TEST_STD_ID_UINT   0x10
#define TEST_STD_ID_FLOAT  0x65536
#define TEST_STD_ID_STRING 0x87654321
#define TEST_STD_ID_BLOB   0x123456789abcdef

extern history_gluon_context_t g_ctx;
extern history_gluon_data_t *g_data;
extern history_gluon_data_array_t *g_array;

void create_global_context(void);
void free_global_context(void);
void cleanup_global_data(void);

#endif // _test_uitls_h
