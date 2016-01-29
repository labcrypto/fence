all:
	mkdir -p bin
	mkdir -p lib
	g++ -std=c++11 -Wall -c -I/usr/local/include/hot ../../common/gate/message_status.cc -o lib/message_status.o
	g++ -std=c++11 -Wall -c -I/usr/local/include/hot ../../common/gate/message.cc -o lib/message.o
	g++ -std=c++11 -Wall -c -I/usr/local/include/hot ../../common/gate/proxy/gate_service_proxy.cc -o lib/gate_service_proxy.o
	g++ -std=c++11 -Wall -c -I/usr/local/include/hot ../../common/gate/proxy/gate_service_proxy_builder.cc -o lib/gate_service_proxy_builder.o
	g++ -std=c++11 -Wall -c -I/usr/local/include/hot ../../common/gate/service/gate_service_request_handler.cc -o lib/gate_service_request_handler.o
	g++ -std=c++11 -Wall -c -I/usr/local/include/hot client.cc -o lib/client.o
	g++ -std=c++11 -Wall -c -I/usr/local/include/hot client2.cc -o lib/client2.o
	g++ -std=c++11 -Wall -c -I/usr/local/include/hot server.cc -o lib/server.o
	g++ -std=c++11 -Wall -c -I/usr/local/include/hot server2.cc -o lib/server2.o
	g++ -std=c++11 -Wall -c -I/usr/local/include/hot hotgen/echo_request.cc -o lib/echo_request.o
	g++ lib/message_status.o \
          lib/message.o \
          lib/gate_service_proxy.o \
          lib/gate_service_proxy_builder.o \
          lib/gate_service_request_handler.o \
          lib/client.o \
          lib/echo_request.o \
          -lhotd \
          -lpthread \
          -o echoer-client.out
	g++ lib/message_status.o \
          lib/message.o \
          lib/gate_service_proxy.o \
          lib/gate_service_proxy_builder.o \
          lib/gate_service_request_handler.o \
          lib/client2.o \
          lib/echo_request.o \
          -lhotd \
          -lpthread \
          -o echoer-client2.out
	g++ lib/message_status.o \
          lib/message.o \
          lib/gate_service_proxy.o \
          lib/gate_service_proxy_builder.o \
          lib/gate_service_request_handler.o \
          lib/server.o \
          lib/echo_request.o \
          -lhotd \
          -lpthread \
          -o echoer-server.out
	g++ lib/message_status.o \
          lib/message.o \
          lib/gate_service_proxy.o \
          lib/gate_service_proxy_builder.o \
          lib/gate_service_request_handler.o \
          lib/server2.o \
          lib/echo_request.o \
          -lhotd \
          -lpthread \
          -o echoer-server2.out
