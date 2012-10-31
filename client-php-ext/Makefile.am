all: src/configure make_in_src

.PHONY: make_in_src
make_in_src: src/Makefile
	make -C src

src/Makefile: src/configure
	cd src; ./configure

src/configure: src/config.m4
	cd src; phpize
