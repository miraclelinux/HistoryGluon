AC_PREREQ(2.63)

AC_INIT(history-gluon, 0.2.0)
AM_INIT_AUTOMAKE([1.9 no-dist-gzip dist-bzip2 tar-pax])

AC_DISABLE_STATIC
AC_PROG_LIBTOOL
AC_PROG_CXX

AC_CONFIG_MACRO_DIR([m4])

dnl **************************************************************
dnl Checks for Java
dnl **************************************************************
AC_PATH_PROG(JAVA, java)
if test -z "$JAVA"; then
  AC_MSG_ERROR(Unable to find "java" in your path. Please install JDK 1.6 or later.)
fi

AC_PATH_PROG(JAVAC, javac)
if test -z "$JAVAC"; then
  AC_MSG_ERROR(Unable to find "javac" in your path. Please install JDK 1.6 or later.)
fi

AC_PATH_PROG(ANT, ant)
if test -z "$ANT"; then
  AC_MSG_ERROR(Unable to find "ant" in your path. Please install Apache Ant.)
fi

dnl **************************************************************
dnl Checks for JUnit4
dnl **************************************************************
m4_define([DEFAULT_JUNIT4_PATH], [/usr/share/java/junit4.jar])

AC_ARG_WITH([junit4],
            AS_HELP_STRING([--with-junit4@<:@=PATH@:>@],
                           [use JUnit4 for testing Java codes
                            @<:@default=DEFAULT_JUNIT4_PATH@:>@]),
            [JUNIT4_PATH="$withval"],
            [JUNIT4_PATH=DEFAULT_JUNIT4_PATH])

if test "$JUNIT4_PATH" = "yes"; then
  JUNIT4_PATH=DEFAULT_JUNIT4_PATH
  with_junit4="$JUNIT4_PATH"
elif test "$JUNIT4_PATH" = "no"; then
  JUNIT4_PATH=""
  with_junit4=no
else
  with_junit4="$JUNIT4_PATH"
fi

if test "!" -e "$JUNIT4_PATH"; then
  JUNIT4_PATH=""
  with_junit4=no
fi

AC_SUBST(JUNIT4_PATH)

dnl **************************************************************
dnl Checks for HBase
dnl **************************************************************
m4_define([DEFAULT_HBASE_PATH], [/usr/local/hbase])

AC_ARG_WITH([hbase],
            AS_HELP_STRING([--with-hbase@<:@=PATH@:>@],
                           [build HBase storage driver
                            @<:@default=DEFAULT_HBASE_PATH@:>@]),
            [HBASE_PATH="$withval"],
            [HBASE_PATH=DEFAULT_HBASE_PATH])

if test "$HBASE_PATH" = "yes"; then
  HBASE_PATH=DEFAULT_HBASE_PATH
elif test "$HBASE_PATH" = "no"; then
  HBASE_PATH=""
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
else
  with_hbase="$HBASE_PATH"
fi

AC_SUBST(HBASE_PATH)

dnl **************************************************************
dnl Checks for Cassandra
dnl **************************************************************
m4_define([DEFAULT_CASSANDRA_PATH], [/usr/local/apache-cassandra])

AC_ARG_WITH([cassandra],
            AS_HELP_STRING([--with-cassandra@<:@=PATH@:>@],
                           [build Apache Cassandra storage driver
                            @<:@default=DEFAULT_CASSANDRA_PATH@:>@]),
            [CASSANDRA_PATH="$withval"],
            [CASSANDRA_PATH=DEFAULT_CASSANDRA_PATH])

if test "$CASSANDRA_PATH" = "yes"; then
  CASSANDRA_PATH=DEFAULT_CASSANDRA_PATH
elif test "$CASSANDRA_PATH" = "no"; then
  CASSANDRA_PATH=""
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
else
  with_cassandra="$CASSANDRA_PATH"
fi

AC_SUBST(CASSANDRA_PATH)

dnl **************************************************************
dnl Checks for Riak
dnl **************************************************************
m4_define([DEFAULT_RIAK_CLIENT_PATH], [/usr/local/riak-client])

AC_ARG_WITH([riak_client],
            AS_HELP_STRING([--with-riak-client@<:@=PATH@:>@],
                           [build Riak storage driver
                            @<:@default=DEFAULT_RIAK_CLIENT_PATH@:>@]),
            [RIAK_CLIENT_PATH="$withval"],
            [RIAK_CLIENT_PATH=DEFAULT_RIAK_CLIENT_PATH])

if test "$RIAK_CLIENT_PATH" = "yes"; then
  RIAK_CLIENT_PATH=DEFAULT_RIAK_CLIENT_PATH
elif test "$RIAK_CLIENT_PATH" = "no"; then
  RIAK_CLIENT_PATH=""
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
else
  with_riak_client="$RIAK_CLIENT_PATH"
fi

AC_SUBST(RIAK_CLIENT_PATH)

dnl **************************************************************
dnl Checks for Commons-Logging
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
  files="`find -L $HBASE_PATH/lib -type f -name "commons-logging-*.*.*.jar"`"
  for file in $files; do
    COMMONS_LOGGING_PATH=$file
    break
  done
fi

if test -z "$COMMONS_LOGGING_PATH" -o "!" -e "$COMMONS_LOGGING_PATH"; then
  COMMONS_LOGGING_PATH="/usr/share/java/commons-logging.jar"
  if test "!" -e "$COMMONS_LOGGING_PATH"; then
    AC_MSG_ERROR(Unable to find Commons-Logging library.)
  fi
fi

AC_SUBST(COMMONS_LOGGING_PATH)

dnl **************************************************************
dnl Checks for PHP
dnl **************************************************************
AC_PATH_PROG(PHPIZE, phpize)
AM_CONDITIONAL(ENABLE_PHP_EXT, [test -n "$PHPIZE"])

if test -n "$PHPIZE"; then
  enable_php_ext=yes
else
  enable_php_ext=no
fi

dnl **************************************************************
dnl Checks for GLib
dnl **************************************************************
GLIB_REQUIRED=2.22
PKG_CHECK_MODULES(GLIB, glib-2.0 >= $GLIB_REQUIRED)

dnl **************************************************************
dnl Checks for Cutter
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


dnl **************************************************************
dnl Results
dnl **************************************************************

AC_OUTPUT([
Makefile
server/Makefile
server/build.xml
server/history-gluon-server
server/run-test.sh
server/conf/Makefile
client-lib-c/Makefile
client-lib-c/history-gluon-1.0.pc
client-lib-c/src/Makefile
client-lib-c/test/Makefile
client-php-ext/Makefile
cli/Makefile
cli/src/Makefile
])

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
