bin_PROGRAMS = history-gluon-cli
history_gluon_cli_SOURCES = history-gluon-cli.cc

AM_CXXFLAGS = -Wall -g $(GLIB_CFLAGS) -I../../client-lib-c/src
history_gluon_cli_LDADD = \
$(GLIB_LIBS) \
../../client-lib-c/src/libhistory-gluon.la
