#include <cutter.h>
#include "utils.h"
#include "history-gluon.h"

static void
assert_create_context(const char *dbname, const char *server_name, int port,
                      history_gluon_result_t expected)
{
	history_gluon_result_t ret;
	ret = history_gluon_create_context(dbname, server_name, port, &g_ctx);
	cut_assert_equal_int(expected, ret);
	if (expected == HGL_SUCCESS)
		cut_assert(g_ctx);
}

/* ---------------------------------------------------------------------------
 * Teset cases
 * ------------------------------------------------------------------------- */
void setup(void)
{
}

void teardown(void)
{
	cleanup_global_data();
}

void test_context_localhost(void)
{
	assert_create_context("test", "localhost", 0, HGL_SUCCESS);
}

void test_context_127_0_0_1(void)
{
	assert_create_context("test", "127.0.0.1", 0, HGL_SUCCESS);
}

void test_context_port_30010(void)
{
	assert_create_context("test", NULL, 30010, HGL_SUCCESS);
}

void test_context_port_max_length(void)
{
	int i;
	char name[HISTORY_GLUON_MAX_DATABASE_NAME_LENGTH+1];
	for (i = 0; i < HISTORY_GLUON_MAX_DATABASE_NAME_LENGTH; i++)
		name[i] = 'A';
	name[HISTORY_GLUON_MAX_DATABASE_NAME_LENGTH] = '\0';
	assert_create_context(name, NULL, 0, HGL_SUCCESS);
}

void test_context_port_too_long_length(void)
{
	int i;
	char name[HISTORY_GLUON_MAX_DATABASE_NAME_LENGTH+2];
	for (i = 0; i < HISTORY_GLUON_MAX_DATABASE_NAME_LENGTH+1; i++)
		name[i] = 'A';
	name[HISTORY_GLUON_MAX_DATABASE_NAME_LENGTH+1] = '\0';
	assert_create_context(name, NULL, 0, HGLERR_TOO_LONG_DB_NAME);
}

void test_context_valid_chars(void)
{
	char *dbname = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	               "012345689.-_@/";
	assert_create_context(dbname, NULL, 0, HGL_SUCCESS);
}

void test_context_invalid_Ex(void)
{
	char *dbname = "name!";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_Sharp(void)
{
	char *dbname = "name#";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_dollar(void)
{
	char *dbname = "name$";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_percent(void)
{
	char *dbname = "name%";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_hat(void)
{
	char *dbname = "name^";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_anpersand(void)
{
	char *dbname = "name&";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_asterisk(void)
{
	char *dbname = "name*";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_parenthesis_open(void)
{
	char *dbname = "name(";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_parenthesis_close(void)
{
	char *dbname = "name)";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_brace_open(void)
{
	char *dbname = "name{";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_brace_close(void)
{
	char *dbname = "name}";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_square_open(void)
{
	char *dbname = "name[";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_square_close(void)
{
	char *dbname = "name]";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_angle_open(void)
{
	char *dbname = "name<";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_angle_close(void)
{
	char *dbname = "name>";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_back_slash(void)
{
	char *dbname = "name\\";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_bar(void)
{
	char *dbname = "name|";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_plus(void)
{
	char *dbname = "name+";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_question(void)
{
	char *dbname = "name?";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_colon(void)
{
	char *dbname = "name:";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_semicolon(void)
{
	char *dbname = "name;";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_quation(void)
{
	char *dbname = "name'";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_back_quation(void)
{
	char *dbname = "name`";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_double_quation(void)
{
	char *dbname = "name\"";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_comma(void)
{
	char *dbname = "name,";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_tilde(void)
{
	char *dbname = "name~";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}

void test_context_invalid_0x10(void)
{
	char *dbname = "name\x10";
	assert_create_context(dbname, NULL, 0, HGLERR_INVALID_DB_NAME);
}
