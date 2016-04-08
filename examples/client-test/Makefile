cppflags=-Wall -g -c -fPIC -std=c++11 \
         -I../../../../naeem-gate-api/hot \
         -I../../../../naeem-gate-api/client/include \
         -I../../../../corelibs-api/include \
         -I../../../../corelibs++-api/include
PWD=$(shell pwd)

all:
	mkdir -p .lib
	rm -rfv .lib/*
  
	g++ $(cppflags) simple_client_test.cc -o .lib/simple_client_test.o

	g++ -L../../../../naeem-gate-api/client/lib \
      .lib/simple_client_test.o \
      -lnaeem-gate-client \
      -lhotd \
      -lpthread \
      -o simple_client_test.out