cppflags=-Wall -g -c -fPIC -std=c++11 \
         -I/usr/local/include/hot \
         -I../../../naeem-gate-api/hot \
         -I../../../naeem-gate-api/client/include \
         -I../../../corelibs-api/include \
         -I../../../corelibs++-api/include
PWD=$(shell pwd)
apiDir=$(PWD)/../../../naeem-gate-api
libName=libnaeem-gate-client
majorVersion=1
minorVersion=0


all:
	mkdir -p .lib
	rm -rfv .lib/*
	
	g++ $(cppflags) ../../../naeem-gate-api/hot/gate/message.cc -o .lib/message.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/transport/accept_report.cc -o .lib/accept_report.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/gate/message_status.cc -o .lib/message_status.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/transport/transport_message.cc -o .lib/transport_message.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/transport/transport_message_status.cc -o .lib/transport_message_status.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/gate/service/gate_service_request_handler.cc -o .lib/gate_service_request_handler.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/transport/proxy/transport_service_proxy.cc -o .lib/transport_service_proxy.o
	g++ $(cppflags) ../../../naeem-gate-api/hot/transport/proxy/transport_service_proxy_builder.cc -o .lib/transport_service_proxy_builder.o
	g++ $(cppflags) runtime.cc -o .lib/runtime.o
	g++ $(cppflags) simple_gate_client.cc -o .lib/simple_gate_client.o

	rm -rf .lib/$(libName).a.$(majorVersion).$(minorVersion)
	ar -cq .lib/$(libName).a.$(majorVersion).$(minorVersion) \
         .lib/runtime.o \
         .lib/message.o \
         .lib/accept_report.o \
         .lib/message_status.o \
         .lib/transport_message.o \
         .lib/transport_message_status.o \
         .lib/gate_service_request_handler.o \
         .lib/transport_service_proxy.o \
         .lib/transport_service_proxy_builder.o \
         .lib/simple_gate_client.o
	cp -v .lib/$(libName).a.$(majorVersion).$(minorVersion) $(apiDir)/client/lib
	rm -rf .lib/$(libName).a.$(majorVersion)
	rm -rf .lib/$(libName).a 
	ln -s $(PWD)/.lib/$(libName).a.$(majorVersion).$(minorVersion) .lib/$(libName).a.$(majorVersion)
	ln -s $(PWD)/.lib/$(libName).a.$(majorVersion).$(minorVersion) .lib/$(libName).a
	rm -rf $(apiDir)/client/lib/$(libName).a.$(majorVersion)
	rm -rf $(apiDir)/client/lib/$(libName).a
	ln -s $(apiDir)/client/lib/$(libName).a.$(majorVersion).$(minorVersion) $(apiDir)/client/lib/$(libName).a.$(majorVersion)
	ln -s $(apiDir)/client/lib/$(libName).a.$(majorVersion).$(minorVersion) $(apiDir)/client/lib/$(libName).a

	rm -rf .lib/$(libName).so.$(majorVersion).$(minorVersion)
	g++ -shared -Wl,-soname,$(libName).so.$(majorVersion) -o .lib/$(libName).so.$(majorVersion).$(minorVersion) \
      	 .lib/runtime.o \
         .lib/message.o \
         .lib/accept_report.o \
         .lib/message_status.o \
         .lib/transport_message.o \
         .lib/transport_message_status.o \
         .lib/gate_service_request_handler.o \
         .lib/transport_service_proxy.o \
         .lib/transport_service_proxy_builder.o \
         .lib/simple_gate_client.o
	cp -v .lib/$(libName).so.$(majorVersion).$(minorVersion) $(apiDir)/client/lib
	rm -rf .lib/$(libName).so.$(majorVersion)
	rm -rf .lib/$(libName).so
	ln -s $(PWD)/.lib/$(libName).so.$(majorVersion).$(minorVersion) .lib/$(libName).so.$(majorVersion)
	ln -s $(PWD)/.lib/$(libName).so.$(majorVersion).$(minorVersion) .lib/$(libName).so
	rm -rf $(apiDir)/client/lib/$(libName).so.$(majorVersion)
	rm -rf $(apiDir)/client/lib/$(libName).so
	ln -s $(apiDir)/client/lib/$(libName).so.$(majorVersion).$(minorVersion) $(apiDir)/client/lib/$(libName).so.$(majorVersion)
	ln -s $(apiDir)/client/lib/$(libName).so.$(majorVersion).$(minorVersion) $(apiDir)/client/lib/$(libName).so
