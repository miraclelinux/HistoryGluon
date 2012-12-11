if ENABLE_PHP_EXT
all: src/configure make_in_src

.PHONY: make_in_src
make_in_src: src/Makefile
	make -C src

src/Makefile: src/configure
	cd src; export CFLAGS="-I ../../client-lib-c/src -g -Wall"; export LDFLAGS="-L../../client-lib-c/src/.libs -lhistory-gluon"; export LD_LIBRARY_PATH=../../client-lib-c/src/.libs; ./configure

src/configure: src/config.m4
	cd src; phpize

.PHONY: install
install:
	make -C src install

.PHONY: clean
clean:
	-make -C src clean
	rm -f src/configure
endif
