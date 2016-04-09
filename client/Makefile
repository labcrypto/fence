cppflags=-Wall -g -c -fPIC -std=c++11 \
         -I/usr/local/include/hot \
         -I../../naeem-gate-api/hot \
         -I../../naeem-gate-api/client/include \
         -I../../corelibs-api/include \
         -I../../corelibs++-api/include
PWD=$(shell pwd)
apiDir=$(PWD)/../../naeem-gate-api
libName=libnaeem-gate-client
majorVersion=1
minorVersion=0

all:
	mkdir -p .lib
	rm -rfv .lib/*
	
	g++ $(cppflags) $(apiDir)/hot/gate/message.cc -o .lib/message.o
	g++ $(cppflags) $(apiDir)/hot/gate/proxy/gate_service_proxy.cc -o .lib/gate_service_proxy.o
	g++ $(cppflags) $(apiDir)/hot/gate/proxy/gate_service_proxy_builder.cc -o .lib/gate_service_proxy_builder.o

	g++ $(cppflags) runtime.cc -o .lib/runtime.o
	g++ $(cppflags) default_message_receiver.cc -o .lib/default_message_receiver.o
	g++ $(cppflags) default_message_submitter.cc -o .lib/default_message_submitter.o
	g++ $(cppflags) submitter_thread.cc -o .lib/submitter_thread.o
	g++ $(cppflags) receiver_thread.cc -o .lib/receiver_thread.o

	rm -rf .lib/$(libName).a.$(majorVersion).$(minorVersion)
	ar -cq .lib/$(libName).a.$(majorVersion).$(minorVersion) \
         .lib/message.o \
         .lib/gate_service_proxy.o \
         .lib/gate_service_proxy_builder.o \
         .lib/runtime.o \
         .lib/default_message_receiver.o \
         .lib/default_message_submitter.o \
         .lib/submitter_thread.o \
         .lib/receiver_thread.o
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
      	 .lib/message.o \
         .lib/gate_service_proxy.o \
         .lib/gate_service_proxy_builder.o \
         .lib/runtime.o \
         .lib/default_message_receiver.o \
         .lib/default_message_submitter.o \
         .lib/submitter_thread.o \
         .lib/receiver_thread.o
	cp -v .lib/$(libName).so.$(majorVersion).$(minorVersion) $(apiDir)/client/lib
	rm -rf .lib/$(libName).so.$(majorVersion)
	rm -rf .lib/$(libName).so
	ln -s $(PWD)/.lib/$(libName).so.$(majorVersion).$(minorVersion) .lib/$(libName).so.$(majorVersion)
	ln -s $(PWD)/.lib/$(libName).so.$(majorVersion).$(minorVersion) .lib/$(libName).so
	rm -rf $(apiDir)/client/lib/$(libName).so.$(majorVersion)
	rm -rf $(apiDir)/client/lib/$(libName).so
	ln -s $(apiDir)/client/lib/$(libName).so.$(majorVersion).$(minorVersion) $(apiDir)/client/lib/$(libName).so.$(majorVersion)
	ln -s $(apiDir)/client/lib/$(libName).so.$(majorVersion).$(minorVersion) $(apiDir)/client/lib/$(libName).so
