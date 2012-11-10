#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>
#include <vector>
using namespace std;

#include <stdint.h>
#include <stdarg.h>
#include <glib.h>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "history-gluon.h"

typedef bool (*command_handler_fn)(const vector<string> &args);
typedef map<string, command_handler_fn> command_map_t;
typedef command_map_t::iterator command_map_itr;

// --------------------------------------------------------------------------
// class: hgl_exception
// --------------------------------------------------------------------------
class hgl_exception {
	string m_descr;
	history_gluon_result_t m_error_code;
public:
	hgl_exception(history_gluon_result_t error_code);
	hgl_exception(const char *fmt, ...);
	history_gluon_result_t get_error_code(void);
	void print(void);
};

hgl_exception::hgl_exception(history_gluon_result_t error_code)
: m_error_code(error_code)
{
}

hgl_exception::hgl_exception(const char *fmt, ...)
: m_error_code(HGLERR_UNKNOWN_REASON)
{
	va_list ap;
	va_start(ap, fmt);
	gchar *msg = g_strdup_vprintf(fmt, ap);
	va_end(ap);
	m_descr = msg;
	g_free(msg);
}

history_gluon_result_t hgl_exception::get_error_code(void)
{
	return m_error_code;
}

void hgl_exception::print(void)
{
	const char *descr = "(No description)";
	if (!m_descr.empty())
		descr = m_descr.c_str();
	printf("<<egl_exception>> [%d] %s\n", m_error_code, descr);
}

// --------------------------------------------------------------------------
// class: hgl_context
// --------------------------------------------------------------------------
class hgl_context_factory {
	string m_db_name;
	history_gluon_context_t m_ctx;
public:
	hgl_context_factory(void);
	~hgl_context_factory();
	void set_database_name(string &name);
	history_gluon_context_t get(void);
};

hgl_context_factory::hgl_context_factory(void)
: m_ctx(NULL)
{
}

hgl_context_factory::~hgl_context_factory()
{
	if (m_ctx) {
		history_gluon_free_context(m_ctx);
		m_ctx = NULL;
	}
}

void hgl_context_factory::set_database_name(string &name)
{
	m_db_name = name;
}

history_gluon_context_t hgl_context_factory::get(void)
{
	if (m_ctx)
		return m_ctx;
	
	if (m_db_name == "")
		throw hgl_exception("m_db_name is not set.");

	history_gluon_result_t ret;
	ret = history_gluon_create_context(m_db_name.c_str(), NULL, 0, &m_ctx);
	if (ret != HGL_SUCCESS)
		throw hgl_exception(ret);
	return m_ctx;
}

// --------------------------------------------------------------------------
// static variables
// --------------------------------------------------------------------------
static command_map_t           g_command_map;
static hgl_context_factory     g_hgl_ctx_factory;

// --------------------------------------------------------------------------
// static functions
// --------------------------------------------------------------------------
static bool command_handler_query_all(history_gluon_context_t ctx, uint64_t id)
{
	history_gluon_data_array_t *array;
	history_gluon_result_t ret;
	ret = history_gluon_range_query(ctx, id,
	                                &HISTORY_GLUON_TIMESPEC_START,
	                                &HISTORY_GLUON_TIMESPEC_END,
	                                HISTORY_GLUON_SORT_ASCENDING,
	                                HISTORY_GLUON_NUM_ENTRIES_UNLIMITED,
	                                &array);
	if (ret != HGL_SUCCESS) {
		printf("Error: history_gluon_range_query: %d\n", ret);
		return false;
	}

	// free data array
	history_gluon_free_data_array(ctx, array);
	return true;
}

static bool command_handler_query(const vector<string> &args)
{
	if (args.size() < 2) {
		printf("Error: query command needs args.\n");
		return false;
	}

	// set database name
	string db_name = args[0];
	g_hgl_ctx_factory.set_database_name(db_name);
	history_gluon_context_t ctx = g_hgl_ctx_factory.get();

	// parse ID
	uint64_t id;
	const string &id_str = args[1];
	if (sscanf(id_str.c_str(), "%"PRIu64, &id) < 1) {
		printf("Error: sscanf(): %s\n", id_str.c_str());
		return false;
	}
	return command_handler_query_all(ctx, id);
}

static void print_usage(void)
{
	printf("Usage:\n");
	printf("\n");
	printf(" $ history-gluon-cli command args\n");
	printf("\n");
	printf("*** command list ***\n");
	printf("  query db_name id\n");
	printf("\n");
}

static void init(void)
{
	g_command_map["query"] = command_handler_query;
}

int main(int argc, char **argv)
{
	init();

	if (argc < 2) {
		print_usage();
		return EXIT_FAILURE;;
	}

	char *command = argv[1];
	vector<string> args;
	for (int i = 2; i < argc; i++)
		args.push_back(argv[i]);
	command_map_itr it = g_command_map.find(command);
	if (it == g_command_map.end()) {
		printf("Error: unknown command: %s\n", command);
		return EXIT_FAILURE;
	}

	bool result = false;
	command_handler_fn command_handler = it->second;
	try {
		result = (*command_handler)(args);
	} catch (hgl_exception e) {
		e.print();
	}
	if (!result)
		return EXIT_FAILURE;

	return EXIT_SUCCESS;
}
