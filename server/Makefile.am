.PHONY: history-gluon.jar test
history-gluon.jar:
	$(ANT) package

test:
	$(ANT) test

clean-local:
	-rm -rf classes classes.test

CLEANFILES = \
	TEST-com.miraclelinux.historygluon.* \
	history-gluon.jar

jardir = $(datadir)/$(PACKAGE_NAME)
jar_DATA = \
	history-gluon.jar
