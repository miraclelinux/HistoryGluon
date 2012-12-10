AC_INIT(history-gluon, 0.2.0)
AM_INIT_AUTOMAKE()

AC_DISABLE_STATIC
AC_PROG_LIBTOOL
AC_PROG_CXX

dnl **************************************************************
dnl Check for Java
dnl **************************************************************
AC_PATH_PROG(JAVAC, javac)
if test -z "$JAVAC"; then
  AC_MSG_ERROR(Unable to find "javac" in your path. Please install JDK 1.6 or later to build Java sources.)
fi

AC_PATH_PROG(ANT, ant)
if test -z "$ANT"; then
  AC_MSG_ERROR(Unable to find "ant" in your path. Please install Apache Ant to build Java sources.)
fi

dnl **************************************************************
dnl Check for GLib
dnl **************************************************************
GLIB_REQUIRED=2.22
PKG_CHECK_MODULES(GLIB, glib-2.0 >= $GLIB_REQUIRED)

dnl **************************************************************
dnl Check for Cutter
dnl **************************************************************
CUTTER_REQUIRED=1.1.7
have_cutter=no

m4_ifdef([AC_CHECK_GCUTTER], [
AC_CHECK_GCUTTER(>= $CUTTER_REQUIRED)
],
[ac_cv_use_cutter="no"])

if test "$ac_cv_use_cutter" != "no"; then
  m4_ifdef([AC_CHECK_CPPCUTTER], [
  AC_CHECK_CPPCUTTER(>= $CUTTER_REQUIRED)
  ],
  [ac_cv_use_cutter="no"])
fi

AM_CONDITIONAL([WITH_CUTTER], [test "$ac_cv_use_cutter" != "no"])
if test "$ac_cv_use_cutter" != "no"; then
  AC_DEFINE(WITH_CUTTER, 1, [Define to 1 if you use Cutter])
  have_cutter=yes
fi


AC_CONFIG_MACRO_DIR([m4])

AC_OUTPUT([
Makefile
server/Makefile
client-lib-c/Makefile
client-lib-c/history-gluon-1.0.pc
client-lib-c/src/Makefile
client-lib-c/test/Makefile
client-php-ext/Makefile
cli/Makefile
cli/src/Makefile
])

dnl **************************************************************
dnl Show configure result
dnl **************************************************************

echo
echo "Configure Result:"
echo
echo "  Ant path             : ${ANT}"
echo "  C Unit test (cutter) : ${have_cutter}"
echo
