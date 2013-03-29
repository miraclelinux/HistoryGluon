#include <ruby.h>
#include <history-gluon.h>

VALUE rb_cHistoryGluon;
static VALUE sym_data_id, sym_sec, sym_ns, sym_type, sym_value, sym_length;

typedef struct _HglRubyPtr
{
	history_gluon_context_t ctx;
} HglRubyPtr;

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

	if (gluon_data->type == HISTORY_GLUON_TYPE_FLOAT) {
		rb_hash_aset(data, sym_value, rb_float_new(gluon_data->v.fp));
	} else if (gluon_data->type == HISTORY_GLUON_TYPE_UINT) {
		rb_hash_aset(data, sym_value, ULL2NUM(gluon_data->v.uint));
	} else if (gluon_data->type == HISTORY_GLUON_TYPE_STRING) {
		rb_hash_aset(data, sym_value, rb_str_new2(gluon_data->v.string));
	} else if (gluon_data->type == HISTORY_GLUON_TYPE_BLOB) {
		rb_hash_aset(data, sym_value, rb_str_new2((char*)gluon_data->v.blob));
	} else {
		rb_raise(rb_eRuntimeError,
			 "%s: %d: Unknown data type: %d",
			 __FILE__, __LINE__, gluon_data->type);
	}

	return data;
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
		/* FIXME: raise suitable exception */
		rb_raise(rb_eRuntimeError,
			 "Failed to call history_gluon_range_query: %d",
			 result);
		return Qnil;
	}

	value_array = rb_ary_new();
	for (i = 0; i < array->num_data; i++)
		rb_ary_push(value_array, hgldata2value(array->array[i]));

	return value_array;
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

void
Init_historygluon(void)
{
	rb_cHistoryGluon = rb_define_class("HistoryGluon", rb_cObject);
	rb_define_alloc_func(rb_cHistoryGluon, allocate);
	rb_define_private_method(rb_cHistoryGluon, "initialize", initialize, 3);
	rb_define_method(rb_cHistoryGluon, "add_uint", add_uint, 4);
	rb_define_method(rb_cHistoryGluon, "add_float", add_float, 4);
	rb_define_method(rb_cHistoryGluon, "add_string", add_string, 4);
	rb_define_method(rb_cHistoryGluon, "range_query", range_query, 7);

	sym_data_id = ID2SYM(rb_intern("id"));
	sym_sec     = ID2SYM(rb_intern("sec"));
	sym_ns      = ID2SYM(rb_intern("ns"));
	sym_type    = ID2SYM(rb_intern("type"));
	sym_value   = ID2SYM(rb_intern("value"));
	sym_length  = ID2SYM(rb_intern("length"));
}
