AC_INIT(history-gluon, 0.2.0)
AM_INIT_AUTOMAKE()

AC_DISABLE_STATIC
AC_PROG_LIBTOOL
AC_PROG_CXX

AC_PATH_PROG(ANT, ant)
if test -z "$ANT"; then
  AC_MSG_ERROR(Test for Ant failed. Please install Ant to build Java sources.)
fi
AC_SUBST(ANT)

dnl glib
GLIB_REQUIRED=2.22
PKG_CHECK_MODULES(GLIB, glib-2.0 >= $GLIB_REQUIRED)

dnl cutter
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
