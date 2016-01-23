all:
	g++ -c -std=c++11 -I/usr/local/include/hot ../common/runtime.cc
	g++ -c -std=c++11 -I/usr/local/include/hot ../common/gate_service_impl.cc
	g++ -c -std=c++11 -I/usr/local/include/hot ../common/gate/message.cc
	g++ -c -std=c++11 -I/usr/local/include/hot ../common/gate/status.cc
	g++ -c -std=c++11 -I/usr/local/include/hot ../common/transport/transport_message.cc
	g++ -c -std=c++11 -I/usr/local/include/hot ../common/transport/transport_status.cc
	g++ -c -std=c++11 -I/usr/local/include/hot ../common/gate/service/gate_service_request_handler.cc
#	g++ -c -std=c++11 -I/usr/local/include/hot ../common/transport/proxy/transport_service_proxy.cc
#	g++ -c -std=c++11 -I/usr/local/include/hot ../common/transport/proxy/transport_service_proxy_builder.cc
#	g++ -c -std=c++11 -I/usr/local/include/hot slave_thread.cc
	g++ -c -std=c++11 -I/usr/local/include/hot main.cc
	g++ runtime.o \
	    gate_service_impl.o \
	    message.o \
	    status.o \
	    transport_message.o \
	    transport_status.o \
	    gate_service_request_handler.o \
	    main.o \
	    -lhotd \
	    -lpthread \
	    -o master.out