all:
	mkdir -p bin
	mkdir -p lib
	mkdir -p lib/proxy
	mkdir -p lib/service
	mkdir -p lib/client
	mkdir -p lib/stub
	g++ -c -I/usr/local/include/hot message.cc -o lib/message.o
	g++ -c -I/usr/local/include/hot proxy/transport_service_proxy.cc -o lib/proxy/transport_service_proxy.o
	g++ -c -I/usr/local/include/hot proxy/transport_service_proxy_builder.cc -o lib/proxy/transport_service_proxy_builder.o
	g++ -c -I/usr/local/include/hot service/transport_service_request_handler.cc -o lib/service/transport_service_request_handler.o
	g++ -c -I/usr/local/include/hot client/transport_service_client.cc -o lib/client/transport_service_client.o
	g++ lib/message.o lib/proxy/transport_service_proxy.o lib/proxy/transport_service_proxy_builder.o lib/service/transport_service_request_handler.o  lib/client/transport_service_client.o  -lhotd -lpthread -o bin/transport_service_client.out
	g++ -c -I/usr/local/include/hot stub/transport_service_impl.cc -o lib/stub/transport_service_impl.o
	g++ -c -I/usr/local/include/hot stub/transport_service_server.cc -o lib/stub/transport_service_server.o
	g++ lib/message.o lib/proxy/transport_service_proxy.o lib/proxy/transport_service_proxy_builder.o lib/service/transport_service_request_handler.o  lib/stub/transport_service_impl.o lib/stub/transport_service_server.o  -lhotd -lpthread -o bin/transport_service_server.out
	g++ -c -I/usr/local/include/hot proxy/gate_service_proxy.cc -o lib/proxy/gate_service_proxy.o
	g++ -c -I/usr/local/include/hot proxy/gate_service_proxy_builder.cc -o lib/proxy/gate_service_proxy_builder.o
	g++ -c -I/usr/local/include/hot service/gate_service_request_handler.cc -o lib/service/gate_service_request_handler.o
	g++ -c -I/usr/local/include/hot client/gate_service_client.cc -o lib/client/gate_service_client.o
	g++ lib/message.o lib/proxy/transport_service_proxy.o lib/proxy/transport_service_proxy_builder.o lib/service/transport_service_request_handler.o lib/proxy/gate_service_proxy.o lib/proxy/gate_service_proxy_builder.o lib/service/gate_service_request_handler.o  lib/client/gate_service_client.o  -lhotd -lpthread -o bin/gate_service_client.out
	g++ -c -I/usr/local/include/hot stub/gate_service_impl.cc -o lib/stub/gate_service_impl.o
	g++ -c -I/usr/local/include/hot stub/gate_service_server.cc -o lib/stub/gate_service_server.o
	g++ lib/message.o lib/proxy/transport_service_proxy.o lib/proxy/transport_service_proxy_builder.o lib/service/transport_service_request_handler.o lib/proxy/gate_service_proxy.o lib/proxy/gate_service_proxy_builder.o lib/service/gate_service_request_handler.o  lib/stub/gate_service_impl.o lib/stub/gate_service_server.o  -lhotd -lpthread -o bin/gate_service_server.out
