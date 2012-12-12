lib_LTLIBRARIES = \
	libhistory-gluon.la

include_HEADERS = \
	history-gluon.h \
	message.h

libhistory_gluon_la_SOURCES = \
	$(include_HEADERS) \
	history-gluon.c \
	message.c

AM_CFLAGS = \
	$(GLIB_CFLAGS) -Wall -g

LIBADD = \
	$(GLIB_LIBS) \
	../../client-lib-c/src/libhistory-gluon.la
