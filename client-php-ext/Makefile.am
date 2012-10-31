all: src/configure make_in_src

.PHONY: make_in_src
make_in_src: src/Makefile
	make -C src

src/Makefile: src/configure
	cd src; export CFLAGS="-I ../../client-lib-c -g -Wall"; export LDFLAGS="-L../../client-lib-c/.libs -lhistory-gluon"; ./configure

src/configure: src/config.m4
	cd src; phpize

