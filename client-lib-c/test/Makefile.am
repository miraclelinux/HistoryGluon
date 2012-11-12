#SUBDIRS = fixtures

EXTRA_DIST = run-test.sh

if WITH_CUTTER
TESTS = run-test.sh
TESTS_ENVIRONMENT = NO_MAKE=yes CUTTER="$(CUTTER)"

noinst_LTLIBRARIES = \
	test-client-lib.la \
	test-context.la \
	test-client-bug-follow-up.la \
	libutils.la

endif

# default
AM_CFLAGS = -g -Wall $(GLIB_CFLAGS)
AM_LDFLAGS = -module -rpath $(libdir) -avoid-version -no-undefined
INCLUDES = $(CUTTER_CFLAGS) -I ../src
LIBS = $(CUTTER_LIBS) ../src/libhistory-gluon.la

# utility
libutils_la_SOURCES = utils.c
libutils_la_LDFLAGS = 

# Test cases
test_client_lib_la_SOURCES = test-client-lib.c
test_client_lib_la_LIBADD = ./libutils.la

test_context_la_SOURCES = test-context.c
test_context_la_LIBADD = ./libutils.la

test_client_bug_follow_up_la_SOURCES = test-client-bug-follow-up.c
test_client_bug_follow_up_la_LIBADD = ./libutils.la

echo-cutter:
	@echo $(CUTTER)
