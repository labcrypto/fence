all:
	mkdir -p bin
	mkdir -p lib
	mkdir -p common
	mkdir -p lib/master
	mkdir -p lib/slave
	g++ -c -I/usr/local/include/hot envelope.cc -o lib/common/envelope.o
	g++ -c -I/usr/local/include/hot proxy/gate_service_proxy.cc -o lib/common/gate_service_proxy.o
	g++ -c -I/usr/local/include/hot proxy/gate_service_proxy_builder.cc -o lib/common/gate_service_proxy_builder.o
	g++ -c -I/usr/local/include/hot service/gate_service_request_handler.cc -o lib/common/gate_service_request_handler.o
	g++ lib/common/envelope.o lib/common/gate_service_proxy.o lib/common/gate_service_proxy_builder.o lib/common/gate_service_request_handler.o  lib/client/gate_service_client.o  -lhotd -lpthread -o bin/gate_service_client.out
	g++ -c -I/usr/local/include/hot stub/gate_service_impl.cc -o lib/stub/gate_service_impl.o
	g++ -c -I/usr/local/include/hot stub/gate_service_server.cc -o lib/stub/gate_service_server.o
	g++ lib/envelope.o lib/proxy/gate_service_proxy.o lib/proxy/gate_service_proxy_builder.o lib/service/gate_service_request_handler.o  lib/stub/gate_service_impl.o lib/stub/gate_service_server.o  -lhotd -lpthread -o bin/gate_service_server.out
