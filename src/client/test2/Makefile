apiDir = ../../../../naeem-gate-api
cppflags = -Wall -g -c -std=c++11 \
           -I/usr/local/include/hot \
           -I$(apiDir)/client/include \
           -I../../../../corelibs-api/include \
           -I../../../../corelibs++-api/include

all:
	mkdir -p .lib
	rm -rfv .lib/*

	g++ $(cppflags) main.cc -o .lib/main.o

	g++ -L$(apiDir)/client/lib \
	    -L../../../../corelibs-api/lib \
	    -L../../../../corelibs++-api/lib \
	    .lib/main.o \
	    -lnaeem-gate-client \
	    -lhotd \
	    -lpthread \
	    -lnaeem-os \
	    -lnaeem++-conf \
	    -lnaeem++-string \
	    -lnaeem++-os \
	    -o test2.out