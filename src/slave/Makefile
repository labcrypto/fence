all:
	g++ -c -std=c++11 -I/usr/local/include/hot ../common/runtime.cc
	g++ -c -std=c++11 -I/usr/local/include/hot ../common/gate_service_impl.cc
	g++ -c -std=c++11 -I/usr/local/include/hot ../common/gate/message.cc
	g++ -c -std=c++11 -I/usr/local/include/hot ../common/gate/service/gate_service_request_handler.cc
	g++ -c -std=c++11 -I/usr/local/include/hot main.cc
	g++ runtime.o gate_service_impl.o message.o gate_service_request_handler.o main.o -lhotd -lpthread -o slave.out