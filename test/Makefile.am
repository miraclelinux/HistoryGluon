#SUBDIRS = fixtures

EXTRA_DIST = run-test.sh

if WITH_CUTTER
TESTS = run-test.sh
TESTS_ENVIRONMENT = NO_MAKE=yes CUTTER="$(CUTTER)"

noinst_LTLIBRARIES = \
	test-client-lib.la
endif

test_client_lib_la_SOURCES = test-client-lib.c
AM_LDFLAGS = -module -rpath $(libdir) -avoid-version -no-undefined
AM_CFLAGS = -g -Wall
INCLUDES = $(CUTTER_CFLAGS) -I ../client-lib-c
LIBS = $(CUTTER_LIBS) ../client-lib-c/libhistory-gluon.la

echo-cutter:
	@echo $(CUTTER)
