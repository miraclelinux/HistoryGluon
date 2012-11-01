lib_LTLIBRARIES = libhistory-gluon.la
libhistory_gluon_la_SOURCES = history-gluon.c message.c
include_HEADERS = history-gluon.h
AM_CFLAGS = $(GLIB_CFLAGS) -Wall -g
LIBADD = $(GLIB_LIBS)

EXTRA_DIST = history-gluon-1.0.pc.in

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = history-gluon-1.0.pc
