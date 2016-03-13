cppflags=-Wall -g -c -std=c++11 \
         -I/usr/local/include/hot \
         -I../../../naeem-gate-api/hot \
         -I../../../corelibs-api/include \
         -I../../../corelibs++-api/include

all:
	mkdir -p .lib
	rm -rfv .lib/*
	g++ $(cppflags) runtime.cc -o .lib/runtime.o
	g++ $(cppflags) gate_service_impl.cc -o .lib/gate_service_impl.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/gate/message.cc -o .lib/message.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/transport/accept_report.cc -o .lib/accept_report.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/gate/message_status.cc -o .lib/message_status.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/transport/transport_message.cc -o .lib/transport_message.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/transport/transport_message_status.cc -o .lib/transport_message_status.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/gate/service/gate_service_request_handler.cc -o .lib/gate_service_request_handler.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/transport/proxy/transport_service_proxy.cc -o .lib/transport_service_proxy.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/transport/proxy/transport_service_proxy_builder.cc -o .lib/transport_service_proxy_builder.o
	g++ $(cppflags) slave_thread.cc -o .lib/slave_thread.o
	g++ $(cppflags) main.cc -o .lib/main.o

	g++ -L../../../corelibs-api/lib \
	    -L../../../corelibs++-api/lib \
	    .lib/runtime.o \
	    .lib/gate_service_impl.o \
	    .lib/message.o \
	    .lib/accept_report.o \
	    .lib/message_status.o \
	    .lib/transport_message.o \
	    .lib/transport_message_status.o \
	    .lib/gate_service_request_handler.o \
	    .lib/transport_service_proxy.o \
	    .lib/transport_service_proxy_builder.o \
	    .lib/slave_thread.o \
	    .lib/main.o \
	    -lhotd \
	    -lpthread \
	    -lnaeem-os \
	    -lnaeem++-conf \
	    -lnaeem++-string \
	    -o naeem-gate-slave-node.out
