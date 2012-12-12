.PHONY: all compile package test clean
all: package

compile:
	$(ANT) compile

package:
	$(ANT) package

clean:
	rm -fr classes classes.test
	rm -f TEST-com.miraclelinux.historygluon.*
	rm -f history-gluon.jar

test:
	$(ANT) test
