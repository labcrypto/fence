apiDir = ../../../../naeem-gate-api
cppflags = -Wall -g -c -std=c++11 \
           -I/usr/local/include/hot \
           -I$(apiDir)/hot

all:
	mkdir -p .lib
	rm -rfv .lib/*

	g++ $(cppflags) $(apiDir)/hot/gate/proxy/gate_monitor_service_proxy.cc -o .lib/gate_monitor_service_proxy.o
	g++ $(cppflags) $(apiDir)/hot/gate/proxy/gate_monitor_service_proxy_builder.cc -o .lib/gate_monitor_service_proxy_builder.o

	g++ $(cppflags) main.cc -o .lib/main.o

	g++ .lib/gate_monitor_service_proxy.o \
	    .lib/gate_monitor_service_proxy_builder.o \
	    .lib/main.o \
	    -lhotd \
	    -lpthread \
	    -o naeem-gate-slave-stat.out
