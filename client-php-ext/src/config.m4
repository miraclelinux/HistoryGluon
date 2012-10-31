PHP_ARG_ENABLE(history-gluon, whether to enable History Gluon support,
[  --enable-history-gluon  Enable History Gluon support])
if test "$PHP_HISTORY_GLUON" = "yes"; then
  AC_DEFINE(HAVE_HISTORY_GLUON, 1, [Whether you have History Gluon])
  PHP_NEW_EXTENSION(history_gluon, history-gluon-php-ext.c, $ext_shared)
fi
