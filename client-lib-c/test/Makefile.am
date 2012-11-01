#SUBDIRS = fixtures

EXTRA_DIST = run-test.sh

if WITH_CUTTER
TESTS = run-test.sh
TESTS_ENVIRONMENT = NO_MAKE=yes CUTTER="$(CUTTER)"

noinst_LTLIBRARIES = \
	test-client-lib.la \
	test-client-bug-follow-up.la \
	libtest-utils.la

endif

# default
AM_CFLAGS = -g -Wall
AM_LDFLAGS = -module -rpath $(libdir) -avoid-version -no-undefined
INCLUDES = $(CUTTER_CFLAGS) -I ../client-lib-c
LIBS = $(CUTTER_LIBS) ../client-lib-c/libhistory-gluon.la

# utility
libtest_utils_la_SOURCES = test-utils.c
libtest_utils_la_LDFLAGS = 

# Test cases
test_client_lib_la_SOURCES = test-client-lib.c
test_client_lib_la_LIBADD = ./libtest-utils.la

test_client_bug_follow_up_la_SOURCES = test-client-bug-follow-up.c
test_client_bug_follow_up_la_LIBADD = ./libtest-utils.la

echo-cutter:
	@echo $(CUTTER)
