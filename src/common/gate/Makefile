all:
	mkdir -p bin
	mkdir -p lib
	mkdir -p lib/proxy
	mkdir -p lib/service
	mkdir -p lib/client
	mkdir -p lib/stub
	g++ -c -I/usr/local/include/hot request.cc -o lib/request.o
	g++ -c -I/usr/local/include/hot response.cc -o lib/response.o
	g++ -c -I/usr/local/include/hot proxy/gate_service_proxy.cc -o lib/proxy/gate_service_proxy.o
	g++ -c -I/usr/local/include/hot proxy/gate_service_proxy_builder.cc -o lib/proxy/gate_service_proxy_builder.o
	g++ -c -I/usr/local/include/hot service/gate_service_request_handler.cc -o lib/service/gate_service_request_handler.o
	g++ -c -I/usr/local/include/hot client/gate_service_client.cc -o lib/client/gate_service_client.o
	g++ lib/request.o lib/response.o lib/proxy/gate_service_proxy.o lib/proxy/gate_service_proxy_builder.o lib/service/gate_service_request_handler.o  lib/client/gate_service_client.o  -lhotd -lpthread -o bin/gate_service_client.out
	g++ -c -I/usr/local/include/hot stub/gate_service_impl.cc -o lib/stub/gate_service_impl.o
	g++ -c -I/usr/local/include/hot stub/gate_service_server.cc -o lib/stub/gate_service_server.o
	g++ lib/request.o lib/response.o lib/proxy/gate_service_proxy.o lib/proxy/gate_service_proxy_builder.o lib/service/gate_service_request_handler.o  lib/stub/gate_service_impl.o lib/stub/gate_service_server.o  -lhotd -lpthread -o bin/gate_service_server.out
