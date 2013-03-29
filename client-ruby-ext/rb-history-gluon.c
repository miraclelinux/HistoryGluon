#include <ruby.h>
#include <history-gluon.h>

VALUE rb_cHistoryGluon;

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
    struct timespec ts = { NUM2LONG(sec), NUM2LONG(ns) };
    history_gluon_result_t result;

    Data_Get_Struct(self, HglRubyPtr, hgl);
    result = history_gluon_add_uint(hgl->ctx, NUM2ULONG(id), &ts,
				    NUM2ULONG(data));

    return INT2NUM(result);
}

static VALUE
add_float(VALUE self, VALUE id, VALUE sec, VALUE ns, VALUE data)
{
    HglRubyPtr *hgl;
    struct timespec ts = { NUM2LONG(sec), NUM2LONG(ns) };
    history_gluon_result_t result;

    Data_Get_Struct(self, HglRubyPtr, hgl);
    result = history_gluon_add_float(hgl->ctx, NUM2ULONG(id), &ts,
				     NUM2DBL(data));

    return INT2NUM(result);
}

static VALUE
add_string(VALUE self, VALUE id, VALUE sec, VALUE ns, VALUE data)
{
    HglRubyPtr *hgl;
    struct timespec ts = { NUM2LONG(sec), NUM2LONG(ns) };
    history_gluon_result_t result;

    Data_Get_Struct(self, HglRubyPtr, hgl);
    result = history_gluon_add_string(hgl->ctx, NUM2ULONG(id), &ts,
				      STR2CSTR(data));

    return INT2NUM(result);
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
}
