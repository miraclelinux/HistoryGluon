.PHONY: history-gluon.jar

history-gluon.jar:
	$(ANT) package

clean-local:
	-rm -rf classes classes.test

CLEANFILES = \
	TEST-com.miraclelinux.historygluon.* \
	history-gluon.jar

TESTS_ENVIRONMENT = $(SHELL)
TESTS = run-test.sh

jardir = $(datadir)/$(PACKAGE_NAME)
jar_DATA = \
	history-gluon.jar

sbin_SCRIPTS = \
	history-gluon-server

java_FILES = \
	src/com/miraclelinux/historygluon/BasicStorageDriver.java \
	src/com/miraclelinux/historygluon/BridgeWorker.java \
	src/com/miraclelinux/historygluon/CassandraDriver.java \
	src/com/miraclelinux/historygluon/ConcurrentHistoryDataSet.java \
	src/com/miraclelinux/historygluon/ConnectionThread.java \
	src/com/miraclelinux/historygluon/DeleteType.java \
	src/com/miraclelinux/historygluon/ErrorCode.java \
	src/com/miraclelinux/historygluon/ExecTimeObserver.java \
	src/com/miraclelinux/historygluon/HBaseDriver.java \
	src/com/miraclelinux/historygluon/HistoryData.java \
	src/com/miraclelinux/historygluon/HistoryDataComparatorPreferId.java \
	src/com/miraclelinux/historygluon/HistoryDataSet.java \
	src/com/miraclelinux/historygluon/HistoryGluon.java \
	src/com/miraclelinux/historygluon/HistoryStreamElement.java \
	src/com/miraclelinux/historygluon/HistoryStreamer.java \
	src/com/miraclelinux/historygluon/InternalCheckException.java \
	src/com/miraclelinux/historygluon/MemDriver.java \
	src/com/miraclelinux/historygluon/QueryType.java \
	src/com/miraclelinux/historygluon/RiakDriver.java \
	src/com/miraclelinux/historygluon/Statistics.java \
	src/com/miraclelinux/historygluon/StorageDriver.java \
	src/com/miraclelinux/historygluon/Utils.java

java_test_FILES = \
	test/com/miraclelinux/historygluon/BasicStorageDriverTest.java \
	test/com/miraclelinux/historygluon/HistoryDataTest.java \
	test/com/miraclelinux/historygluon/UtilsTest.java \
	test/com/miraclelinux/historygluon/AllTests.java

EXTRA_DIST = ${java_FILES} ${java_test_FILES}
