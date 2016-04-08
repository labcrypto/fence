apiDir = ../../../naeem-gate-api
cppflags = -Wall -g -c -std=c++11 \
           -I/usr/local/include/hot \
           -I$(apiDir)/hot \
           -I../../../corelibs-api/include \
           -I../../../corelibs++-api/include

all: tools
	mkdir -p .lib
	rm -rfv .lib/*

	g++ $(cppflags) $(apiDir)/hot/gate/message.cc -o .lib/message.o
	g++ $(cppflags) $(apiDir)/hot/gate/service/gate_service_request_handler.cc -o .lib/gate_service_request_handler.o
	g++ $(cppflags) $(apiDir)/hot/gate/service/gate_monitor_service_request_handler.cc -o .lib/gate_monitor_service_request_handler.o
	g++ $(cppflags) $(apiDir)/hot/gate/service/gate_test_service_request_handler.cc -o .lib/gate_test_service_request_handler.o

	g++ $(cppflags) $(apiDir)/hot/transport/enqueue_report.cc -o .lib/enqueue_report.o
	g++ $(cppflags) $(apiDir)/hot/transport/transport_message.cc -o .lib/transport_message.o
	g++ $(cppflags) $(apiDir)/hot/transport/proxy/transport_service_proxy.cc -o .lib/transport_service_proxy.o
	g++ $(cppflags) $(apiDir)/hot/transport/proxy/transport_service_proxy_builder.cc -o .lib/transport_service_proxy_builder.o
	
	g++ $(cppflags) runtime.cc -o .lib/runtime.o
	g++ $(cppflags) gate_service_impl.cc -o .lib/gate_service_impl.o
	g++ $(cppflags) gate_monitor_service_impl.cc -o .lib/gate_monitor_service_impl.o
	g++ $(cppflags) gate_test_service_impl.cc -o .lib/gate_test_service_impl.o
	g++ $(cppflags) slave_thread.cc -o .lib/slave_thread.o
	g++ $(cppflags) main.cc -o .lib/main.o

	g++ -L../../../corelibs-api/lib \
	    -L../../../corelibs++-api/lib \
	    .lib/message.o \
	    .lib/gate_service_request_handler.o \
	    .lib/gate_monitor_service_request_handler.o \
	    .lib/gate_test_service_request_handler.o \
	    .lib/enqueue_report.o \
	    .lib/transport_message.o \
	    .lib/transport_service_proxy.o \
	    .lib/transport_service_proxy_builder.o \
	    .lib/runtime.o \
	    .lib/gate_service_impl.o \
	    .lib/gate_monitor_service_impl.o \
	    .lib/gate_test_service_impl.o \
	    .lib/slave_thread.o \
	    .lib/main.o \
	    -lhotd \
	    -lpthread \
	    -lnaeem-os \
	    -lnaeem++-conf \
	    -lnaeem++-string \
	    -lnaeem++-os \
	    -o naeem-gate-slave-node.out

tools:
	make -C stat
	make -C mstat
	make -C enq
	make -C ack
	make -C pop
	