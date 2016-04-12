apiDir = ../../../naeem-gate-api
cppflags = -Wall -g -c -std=c++11 \
           -I/usr/local/include/hot \
           -I$(apiDir)/hot

all:
	mkdir -p .lib
	rm -rfv .lib/*

	g++ $(cppflags) $(apiDir)/hot/transport/transport_message.cc -o .lib/transport_message.o
	g++ $(cppflags) $(apiDir)/hot/transport/proxy/transport_service_proxy.cc -o .lib/transport_service_proxy.o
	g++ $(cppflags) $(apiDir)/hot/transport/proxy/transport_service_proxy_builder.cc -o .lib/transport_service_proxy_builder.o

	g++ $(cppflags) main.cc -o .lib/main.o

	g++ .lib/transport_service_proxy.o \
	    .lib/transport_service_proxy_builder.o \
	    .lib/transport_message.o \
	    .lib/main.o \
	    -lhotd \
	    -lpthread \
	    -o naeem-gate-master-spop.out
