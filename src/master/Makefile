all:
	mkdir -p .lib
	rm -rfv .lib/*
	g++ -c -std=c++11 -I/usr/local/include/hot runtime.cc -o .lib/runtime.o
	g++ -c -std=c++11 -I/usr/local/include/hot gate_service_impl.cc -o .lib/gate_service_impl.o
	g++ -c -std=c++11 -I/usr/local/include/hot ../common/gate/message.cc -o .lib/message.o
	g++ -c -std=c++11 -I/usr/local/include/hot ../common/gate/status.cc -o .lib/status.o
	g++ -c -std=c++11 -I/usr/local/include/hot ../common/transport/transport_message.cc -o .lib/transport_message.o
	g++ -c -std=c++11 -I/usr/local/include/hot ../common/transport/transport_status.cc -o .lib/transport_status.o
	g++ -c -std=c++11 -I/usr/local/include/hot ../common/gate/service/gate_service_request_handler.cc -o .lib/gate_service_request_handler.o
	g++ -c -std=c++11 -I/usr/local/include/hot master_thread.cc -o .lib/master_thread.o
	g++ -c -std=c++11 -I/usr/local/include/hot main.cc -o .lib/main.o
	g++ .lib/runtime.o \
	    .lib/gate_service_impl.o \
	    .lib/message.o \
	    .lib/status.o \
	    .lib/transport_message.o \
	    .lib/transport_status.o \
	    .lib/gate_service_request_handler.o \
	    .lib/master_thread.o \
	    .lib/main.o \
	    -lhotd \
	    -lpthread \
	    -o naeem-gate-master-node.out
