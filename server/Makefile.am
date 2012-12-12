.PHONY: all compile package test clean-local
all: package

compile:
	$(ANT) compile

package:
	$(ANT) package

test:
	$(ANT) test

clean-local:
	-rm -rf classes classes.test

CLEANFILES = \
	TEST-com.miraclelinux.historygluon.* \
	history-gluon.jar
