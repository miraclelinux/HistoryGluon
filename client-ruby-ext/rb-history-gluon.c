#include <ruby.h>
#include <history-gluon.h>

VALUE rb_cHistoryGluon;
VALUE rb_eHistoryGluonError;
static VALUE sym_data_id, sym_sec, sym_ns, sym_type, sym_value, sym_length;

typedef struct _HglRubyPtr
{
	history_gluon_context_t ctx;
} HglRubyPtr;

typedef struct _HglConstants
{
	char *name;
	int   value;
} HglConstants;

static HglConstants hgl_constants[] = {
	{ "VALUE_TYPE_FLOAT",			HISTORY_GLUON_TYPE_FLOAT },
	{ "VALUE_TYPE_STRING",			HISTORY_GLUON_TYPE_STRING },
	{ "VALUE_TYPE_UINT",			HISTORY_GLUON_TYPE_UINT },
	{ "VALUE_TYPE_BLOB",			HISTORY_GLUON_TYPE_BLOB },
	{ "VALUE_TYPE_CTRL_STREAM_END",		HISTORY_GLUON_TYPE_CTRL_STREAM_END },
	{ "QUERY_TYPE_ONLY_MATCH",		HISTORY_GLUON_QUERY_TYPE_ONLY_MATCH },
	{ "QUERY_TYPE_LESS_DATA",		HISTORY_GLUON_QUERY_TYPE_LESS_DATA },
	{ "QUERY_TYPE_GREATER_DATA",		HISTORY_GLUON_QUERY_TYPE_GREATER_DATA },
	{ "SORT_ASCENDING",			HISTORY_GLUON_SORT_ASCENDING },
	{ "SORT_DESCENDING",			HISTORY_GLUON_SORT_DESCENDING },
	{ "SORT_NOT_SORTED",			HISTORY_GLUON_SORT_NOT_SORTED },
	{ "DELETE_TYPE_EQUAL",			HISTORY_GLUON_DELETE_TYPE_EQUAL },
	{ "DELETE_TYPE_EQUAL_OR_LESS",		HISTORY_GLUON_DELETE_TYPE_EQUAL_OR_LESS },
	{ "DELETE_TYPE_LESS",			HISTORY_GLUON_DELETE_TYPE_LESS },
	{ "DELETE_TYPE_EQUAL_OR_GREATER",	HISTORY_GLUON_DELETE_TYPE_EQUAL_OR_GREATER },
	{ "DELETE_TYPE_GREATER",		HISTORY_GLUON_DELETE_TYPE_GREATER },
	{ "NUM_ENTRIES_UNLIMITED",		HISTORY_GLUON_NUM_ENTRIES_UNLIMITED },
	{ "MAX_DATABASE_NAME_LENGTH",           HISTORY_GLUON_MAX_DATABASE_NAME_LENGTH },
	{ NULL, 0 },
};

typedef struct _HglErrors
{
	char *name;
	int   code;
	VALUE klass;
} HglErrors;

static HglErrors hgl_errors[] = {
	/* { "Success", HGL_SUCCESS }, */
	{ "UnknownReasonError",         HGLERR_UNKNOWN_REASON,            0 },
	{ "NotImplementedError",        HGLERR_NOT_IMPLEMENTED,           0 },
	{ "MemAllocError",              HGLERR_MEM_ALLOC,                 0 },
	{ "InvalidParameterError",      HGLERR_INVALID_PARAMETER,         0 },
	{ "InvalidContextError",        HGLERR_INVALID_CONTEXT,           0 },
	{ "TooLongDBNameError",         HGLERR_TOO_LONG_DB_NAME,          0 },
	{ "InvazlidDBNameError",        HGLERR_INVALID_DB_NAME,           0 },
	{ "ReadStreamEndError",         HGLERR_READ_STREAM_END,           0 },
	{ "ReadError",                  HGLERR_READ_ERROR,                0 },
	{ "WriteError",                 HGLERR_WRITE_ERROR,               0 },
	{ "TooLongStringError",         HGLERR_TOO_LONG_STRING,           0 },
	{ "TooLargeBlobError",          HGLERR_TOO_LARGE_BLOB,            0 },
	{ "NullDataError",              HGLERR_NULL_DATA,                 0 },
	{ "GetaddrinfoError",           HGLERR_GETADDRINFO,               0 },
	{ "FailedConnectError",         HGLERR_FAILED_CONNECT,            0 },
	{ "MagicCodeError",             HGLERR_UNEXPECTED_MAGIC_CODE,     0 },
	{ "ReplySizeError",             HGLERR_UNEXPECTED_REPLY_SIZE,     0 },
	{ "UnexpectedReplyTypeError",   HGLERR_UNEXPECTED_REPLY_TYPE,     0 },
	{ "ReplyError",                 HGLERR_REPLY_ERROR,               0 },
	{ "InvalidDataTypeError",       HGLERR_INVALID_DATA_TYPE,         0 },

	/* { "SvSuccess", HGLSV_SUCCESS, }*/
	{ "SvUnknownError",             HGLSVERR_UKNOWN_ERROR,            0 },
	{ "SvServerError",              HGLSVERR_SERVER_ERROR,            0 },
	{ "SvUnsupportedVersionError",  HGLSVERR_UNSUPPORTED_VERSION,     0 },
	{ "SvNotImplementedError",      HGLSVERR_NOT_IMPLEMENTED,         0 },
	{ "SvAuthentificationError",    HGLSVERR_AUTHENTIFICATION_FAILED, 0 },
	{ "SvTooLongDBNameError",       HGLSVERR_TOO_LONG_DB_NAME,        0 },
	{ "SvInvalidCharinDBNameError", HGLSVERR_INVLIAD_CHARIN_DB_NAME,  0 },
	{ "SvPacketShortError",         HGLSVERR_PACKET_SHORT,            0 },
	{ "SvInvalidDataTypeError",     HGLSVERR_INVALID_DATA_TYPE,       0 },
	{ "SvInvalidSortTypeError",     HGLSVERR_INVALID_SORT_TYPE,       0 },
	{ "SvEntryExistsError",         HGLSVERR_ENTRY_EXISTS,            0 },
	{ "SvNotFoundError",            HGLSVERR_NOT_FOUND,               0 },
	{ "SvTooManyEntriesError",      HGLSVERR_TOO_MANY_ENTRIES,        0 },

	{NULL, 0, 0},
};

static void
deallocate(void *ptr)
{
	HglRubyPtr *hgl = ptr;

	if (hgl->ctx)
		history_gluon_free_context(hgl->ctx);
	free(ptr);
}

static VALUE
allocate(VALUE klass)
{
	HglRubyPtr *hgl = malloc(sizeof(HglRubyPtr));

	hgl->ctx = NULL;

	return Data_Wrap_Struct(klass, NULL, deallocate, hgl);
}

static VALUE
initialize(VALUE self, VALUE database_name, VALUE server_name, VALUE port)
{
	HglRubyPtr *ptr;
	history_gluon_result_t result;

	Data_Get_Struct(self, HglRubyPtr, ptr);
	result = history_gluon_create_context(STR2CSTR(database_name),
					      STR2CSTR(server_name),
					      NUM2INT(port), &ptr->ctx);
	return Qnil;
}

static VALUE
add_uint(VALUE self, VALUE id, VALUE sec, VALUE ns, VALUE data)
{
	HglRubyPtr *hgl;
	struct timespec ts = { NUM2LL(sec), NUM2LL(ns) };
	history_gluon_result_t result;

	Data_Get_Struct(self, HglRubyPtr, hgl);
	result = history_gluon_add_uint(hgl->ctx, NUM2ULL(id), &ts,
					NUM2ULONG(data));

	return INT2NUM(result);
}

static VALUE
add_float(VALUE self, VALUE id, VALUE sec, VALUE ns, VALUE data)
{
	HglRubyPtr *hgl;
	struct timespec ts = { NUM2LL(sec), NUM2LL(ns) };
	history_gluon_result_t result;

	Data_Get_Struct(self, HglRubyPtr, hgl);
	result = history_gluon_add_float(hgl->ctx, NUM2ULL(id), &ts,
					 NUM2DBL(data));

	return INT2NUM(result);
}

static VALUE
add_string(VALUE self, VALUE id, VALUE sec, VALUE ns, VALUE data)
{
	HglRubyPtr *hgl;
	struct timespec ts = { NUM2LL(sec), NUM2LL(ns) };
	history_gluon_result_t result;

	Data_Get_Struct(self, HglRubyPtr, hgl);
	result = history_gluon_add_string(hgl->ctx, NUM2ULL(id), &ts,
					  STR2CSTR(data));

	return INT2NUM(result);
}

static VALUE
hgldata2value(history_gluon_data_t *gluon_data)
{
	VALUE data = rb_hash_new();

	rb_hash_aset(data, sym_data_id, ULL2NUM(gluon_data->id));
	rb_hash_aset(data, sym_sec,     LL2NUM(gluon_data->ts.tv_sec));
	rb_hash_aset(data, sym_ns,      LL2NUM(gluon_data->ts.tv_nsec));
	rb_hash_aset(data, sym_type,    INT2NUM(gluon_data->type));

	switch (gluon_data->type) {
	case HISTORY_GLUON_TYPE_FLOAT:
		rb_hash_aset(data, sym_value, rb_float_new(gluon_data->v.fp));
		break;
	case HISTORY_GLUON_TYPE_UINT:
		rb_hash_aset(data, sym_value, ULL2NUM(gluon_data->v.uint));
		break;
	case HISTORY_GLUON_TYPE_STRING:
		rb_hash_aset(data, sym_value, rb_str_new2(gluon_data->v.string));
		break;
	case HISTORY_GLUON_TYPE_BLOB:
		rb_hash_aset(data, sym_value, rb_str_new2((char*)gluon_data->v.blob));
		break;
	default:
		rb_raise(rb_path2class("HistoryGluon::Error"),
			 "%s: %d: Unknown data type: %d",
			 __FILE__, __LINE__, gluon_data->type);
		break;
	}

	return data;
}

static void
raise_hgl_exception(history_gluon_result_t result, const char *message)
{
	int i;

	if (result == HGL_SUCCESS)
		return;

	for (i = 0; hgl_errors[i].name; i++) {
		if (result != hgl_errors[i].code)
			continue;
		rb_raise(hgl_errors[i].klass, message);
		return;
	}

	rb_raise(rb_path2class("HistoryGluon::Error"), message);
}

static VALUE
range_query(VALUE self, VALUE id, VALUE sec0, VALUE ns0, VALUE sec1, VALUE ns1,
	    VALUE sort_request, VALUE num_max_entries)
{
	HglRubyPtr *hgl;
	struct timespec ts0 = { NUM2LL(sec0), NUM2LL(ns0) };
	struct timespec ts1 = { NUM2LL(sec1), NUM2LL(ns1) };
	history_gluon_result_t result;
	history_gluon_data_array_t *array = NULL;
	unsigned long i;
	VALUE value_array;

	Data_Get_Struct(self, HglRubyPtr, hgl);
	result = history_gluon_range_query(hgl->ctx,
					   NUM2ULL(id),
					   &ts0, &ts1,
					   NUM2INT(sort_request),
					   NUM2ULL(num_max_entries),
					   &array);
	if (result != HGL_SUCCESS) {
		raise_hgl_exception(result,
				    "Faile to call history_gluon_range_query");
		return Qnil;
	}

	value_array = rb_ary_new();
	for (i = 0; i < array->num_data; i++)
		rb_ary_push(value_array, hgldata2value(array->array[i]));
	history_gluon_free_data_array(hgl->ctx, array);

	return value_array;
}

void
Init_historygluon(void)
{
	int i;

	rb_cHistoryGluon = rb_define_class("HistoryGluon", rb_cObject);
	rb_define_alloc_func(rb_cHistoryGluon, allocate);
	rb_define_private_method(rb_cHistoryGluon, "initialize", initialize, 3);
	rb_define_method(rb_cHistoryGluon, "add_uint", add_uint, 4);
	rb_define_method(rb_cHistoryGluon, "add_float", add_float, 4);
	rb_define_method(rb_cHistoryGluon, "add_string", add_string, 4);
	rb_define_method(rb_cHistoryGluon, "range_query", range_query, 7);
	for (i = 0; hgl_constants[i].name; i++) {
		rb_define_const(rb_cHistoryGluon,
				hgl_constants[i].name,
				INT2NUM(hgl_constants[i].value));
	}

	rb_eHistoryGluonError = rb_define_class_under(rb_cHistoryGluon, "Error",
						      rb_eStandardError);
	rb_define_class_variable(rb_eStandardError, "@@code",
				 INT2NUM(HGLERR_UNKNOWN_REASON));
	for (i = 0; hgl_errors[i].name; i++) {
		hgl_errors[i].klass = 
			rb_define_class_under(rb_cHistoryGluon,
					      hgl_errors[i].name,
					      rb_eHistoryGluonError);
		rb_cv_set(rb_eStandardError, "@@code",
			  INT2NUM(hgl_errors[i].code));
	}

	sym_data_id = ID2SYM(rb_intern("id"));
	sym_sec     = ID2SYM(rb_intern("sec"));
	sym_ns      = ID2SYM(rb_intern("ns"));
	sym_type    = ID2SYM(rb_intern("type"));
	sym_value   = ID2SYM(rb_intern("value"));
	sym_length  = ID2SYM(rb_intern("length"));
}
