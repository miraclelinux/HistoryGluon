AC_INIT(history-gluon, 0.1.0)
AM_INIT_AUTOMAKE()

AC_DISABLE_STATIC
AC_PROG_LIBTOOL

dnl cutter
CUTTER_REQUIRED=1.1.3
have_cutter=no

AC_CHECK_CUTTER
AM_CONDITIONAL([WITH_CUTTER], [test "$ac_cv_use_cutter" != "no"])
if test "$ac_cv_use_cutter" != "no"; then
  AC_DEFINE(WITH_CUTTER, 1, [Define to 1 if you use Cutter])
  have_cutter=yes
fi


AC_CONFIG_MACRO_DIR([m4])

AC_OUTPUT([
Makefile
client-lib-c/Makefile
history-gluon-1.0.pc
test/Makefile
])

dnl **************************************************************
dnl Show configure result
dnl **************************************************************

echo
echo "Configure Result:"
echo
echo "  unit test : ${have_cutter}"
echo
