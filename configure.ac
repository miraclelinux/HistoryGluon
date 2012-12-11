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
dnl Check for JUnit4
dnl **************************************************************
JUNIT4_PATH="/usr/share/java/junit4.jar"
m4_define([_JUNIT4_PATH_], [/usr/share/java/junit4.jar])

AC_ARG_WITH([junit4],
            AS_HELP_STRING([--with-junit4@<:@=PATH@:>@],
                           [build JUnit4 storage driver
                            @<:@default=_JUNIT4_PATH_@:>@]),
            [with_junit4="$withval"],
            [with_junit4="$JUNIT4_PATH"])

if test -n $JUNIT4_PATH -a "$with_junit4" != "no"; then
  JUNIT4_PATH=$with_junit4
fi

if test "!" -e "$JUNIT4_PATH"; then
  JUNIT4_PATH=""
  with_junit4=no
fi

AC_SUBST(JUNIT4_PATH)

dnl **************************************************************
dnl Check for HBase
dnl **************************************************************
HBASE_PATH="/usr/local/hbase-0.94.1"
m4_define([_HBASE_PATH_], [/usr/local/hbase-0.94.1])

AC_ARG_WITH([hbase],
            AS_HELP_STRING([--with-hbase@<:@=PATH@:>@],
                           [build HBase storage driver
                            @<:@default=_HBASE_PATH_@:>@]),
            [with_hbase="$withval"],
            [with_hbase="$HBASE_PATH"])

if test -n $HBASE_PATH -a "$with_hbase" != "no"; then
  HBASE_PATH=$with_hbase
fi

if test -e "$HBASE_PATH"; then
  jar_files="`find -L $HBASE_PATH -type f -name "hbase-*.jar"`"
  if test -z "$jar_files"; then
    HBASE_PATH=""
  fi
else
  HBASE_PATH=""
fi

if test -z "$HBASE_PATH"; then
  with_hbase=no
fi

AC_SUBST(HBASE_PATH)

dnl **************************************************************
dnl Check for Cassandra
dnl **************************************************************
CASSANDRA_PATH="/usr/local/apache-cassandra-1.1.6"
m4_define([_CASSANDRA_PATH_], [/usr/local/apache-cassandra-1.1.6])

AC_ARG_WITH([cassandra],
            AS_HELP_STRING([--with-cassandra@<:@=PATH@:>@],
                           [build Apache Cassandra storage driver
                            @<:@default=_CASSANDRA_PATH_@:>@]),
            [with_cassandra="$withval"],
            [with_cassandra="$CASSANDRA_PATH"])

if test -n $CASSANDRA_PATH -a "$with_cassandra" != "no"; then
  CASSANDRA_PATH=$with_cassandra
fi

if test -e "$CASSANDRA_PATH"; then
  jar_files="`find -L $CASSANDRA_PATH/lib -type f -name "apache-cassandra-*.jar"`"
  if test -z "$jar_files"; then
    CASSANDRA_PATH=""
  fi
else
  CASSANDRA_PATH=""
fi

if test -z "$CASSANDRA_PATH"; then
  with_cassandra=no
fi

AC_SUBST(CASSANDRA_PATH)

dnl **************************************************************
dnl Check for Riak
dnl **************************************************************
RIAK_CLIENT_PATH="/usr/local/riak-client"
m4_define([_RIAK_CLIENT_PATH_], [/usr/local/riak-client])

AC_ARG_WITH([riak_client],
            AS_HELP_STRING([--with-riak-client@<:@=PATH@:>@],
                           [build Riak storage driver
                            @<:@default=_RIAK_CLIENT_PATH_@:>@]),
            [with_riak_client="$withval"],
            [with_riak_client="$RIAK_CLIENT_PATH"])

if test -n $RIAK_CLIENT_PATH -a "$with_riak_client" != "no"; then
  RIAK_CLIENT_PATH=$with_riak_client
fi

if test -e "$RIAK_CLIENT_PATH"; then
  jar_files="`find -L $RIAK_CLIENT_PATH -type f -name "riak-*.jar"`"
  if test -z "$jar_files"; then
    RIAK_CLIENT_PATH=""
  fi
else
  RIAK_CLIENT_PATH=""
fi

if test -z "$RIAK_CLIENT_PATH"; then
  with_riak_client=no
fi

AC_SUBST(RIAK_CLIENT_PATH)

dnl **************************************************************
dnl Check for Commons-Logging
dnl **************************************************************
AC_ARG_WITH([commons-logging],
            AS_HELP_STRING([--with-commons-logging@<:@=PATH@:>@],
                           [Specify Commons-Logging path
                            @<:@default=auto@:>@]),
            [COMMONS_LOGGING_PATH="$withval"],
            [COMMONS_LOGGING_PATH=""])

if test "$COMMONS_LOGGING_PATH" = "no"; then
  COMMONS_LOGGING_PATH=""
fi

if test -z "$COMMONS_LOGGING_PATH" -a -e "$HBASE_PATH"; then
  files="`find -L $HBASE_PATH/lib -type f -name "commons-logging-?.?.?.jar"`"
  for file in $files; do
    COMMONS_LOGGING_PATH=$file
    break
  done
fi

if test -z "$COMMONS_LOGGING_PATH"; then
  COMMONS_LOGGING_PATH="/usr/share/java/commons-logging.jar"
  if test "!" -e "$COMMONS_LOGGING_PATH"; then
    AC_MSG_ERROR(Unable to find Commons-Logging library.)
  fi
fi

AC_SUBST(COMMONS_LOGGING_PATH)

dnl **************************************************************
dnl Check for PHP
dnl **************************************************************
AC_PATH_PROG(PHPIZE, phpize)
AM_CONDITIONAL(ENABLE_PHP_EXT, [test -n "$PHPIZE"])

if test -n "$PHPIZE"; then
  enable_php_ext=yes
else
  enable_php_ext=no
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
server/build.xml
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
echo "  JUnit4:              : ${with_junit4}"
echo "  Commons-Logging      : ${COMMONS_LOGGING_PATH}"
echo "  HBase                : ${with_hbase}"
echo "  Cassandra            : ${with_cassandra}"
echo "  Riak Client          : ${with_riak_client}"
echo "  PHP extension        : ${enable_php_ext}"
echo "  C Unit test (cutter) : ${have_cutter}"
echo
