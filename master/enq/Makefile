apiDir = ../../../../naeem-gate-api
cppflags = -Wall -g -c -std=c++11 \
           -I/usr/local/include/hot \
           -I$(apiDir)/hot \
           -I../../../../corelibs-api/include \
           -I../../../../corelibs++-api/include

all:
	mkdir -p .lib
	rm -rfv .lib/*

	g++ $(cppflags) $(apiDir)/hot/gate/message.cc -o .lib/message.o
	g++ $(cppflags) $(apiDir)/hot/gate/proxy/gate_service_proxy.cc -o .lib/gate_service_proxy.o
	g++ $(cppflags) $(apiDir)/hot/gate/proxy/gate_service_proxy_builder.cc -o .lib/gate_service_proxy_builder.o

	g++ $(cppflags) main.cc -o .lib/main.o

	g++ -L../../../../corelibs-api/lib \
	    -L../../../../corelibs++-api/lib \
	    .lib/gate_service_proxy.o \
	    .lib/gate_service_proxy_builder.o \
	    .lib/message.o \
	    .lib/main.o \
	    -lhotd \
	    -lpthread \
	    -lnaeem-os \
	    -lnaeem++-os \
	    -o naeem-gate-master-enq.out
