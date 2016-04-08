cppflags = -Wall -g -c -std=c++11 \
           -I/usr/local/include/hot \
           -I../../../naeem-gate-api/hot \
           -I../../../corelibs-api/include \
           -I../../../corelibs++-api/include

all: tools
	mkdir -p .lib
	rm -rfv .lib/*

	g++ $(cppflags) ../../../naeem-gate-api/hot/gate/message.cc -o .lib/message.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/gate/service/gate_service_request_handler.cc -o .lib/gate_service_request_handler.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/gate/service/gate_monitor_service_request_handler.cc -o .lib/gate_monitor_service_request_handler.o

	g++ $(cppflags) ../../../naeem-gate-api/hot/transport/transport_message.cc -o .lib/transport_message.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/transport/enqueue_report.cc -o .lib/enqueue_report.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/transport/service/transport_service_request_handler.cc -o .lib/transport_service_request_handler.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/transport/service/transport_monitor_service_request_handler.cc -o .lib/transport_monitor_service_request_handler.o

	g++ $(cppflags) runtime.cc -o .lib/runtime.o
	g++ $(cppflags) gate_service_impl.cc -o .lib/gate_service_impl.o
	g++ $(cppflags) gate_monitor_service_impl.cc -o .lib/gate_monitor_service_impl.o
	g++ $(cppflags) transport_service_impl.cc -o .lib/transport_service_impl.o
	g++ $(cppflags) transport_monitor_service_impl.cc -o .lib/transport_monitor_service_impl.o
	g++ $(cppflags) master_thread.cc -o .lib/master_thread.o
	g++ $(cppflags) main.cc -o .lib/main.o
	
	g++ -L../../../corelibs-api/lib \
	    -L../../../corelibs++-api/lib \
	    .lib/runtime.o \
	    .lib/gate_service_impl.o \
	    .lib/transport_service_impl.o \
	    .lib/gate_monitor_service_impl.o \
	    .lib/transport_monitor_service_impl.o \
	    .lib/message.o \
	    .lib/transport_message.o \
	    .lib/enqueue_report.o \
	    .lib/gate_service_request_handler.o \
	    .lib/transport_service_request_handler.o \
	    .lib/gate_monitor_service_request_handler.o \
	    .lib/transport_monitor_service_request_handler.o \
	    .lib/master_thread.o \
	    .lib/main.o \
	    -lhotd \
	    -lpthread \
	    -lnaeem-os \
	    -lnaeem++-conf \
	    -lnaeem++-string \
	    -lnaeem++-os \
	    -o naeem-gate-master-node.out

tools:
	make -C stat
	make -C pop
	make -C enq
	make -C ack
	make -C spop
	make -C sack