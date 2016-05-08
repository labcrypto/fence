#  The MIT License (MIT)
#
#  Copyright (c) 2015 LabCrypto Org.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#  
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#  
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
#

apiDir = ../../LabCryptoOrg-fence-api
cppflags = -Wall -g -c -std=c++11 \
           -I/usr/local/include/hot \
           -I$(apiDir)/hot \
           -I../../LabCryptoOrg-abettor-c-api/include \
           -I../../LabCryptoOrg-abettor-cc-api/include

all: tools
	mkdir -p .lib
	rm -rfv .lib/*

	g++ $(cppflags) $(apiDir)/hot/fence/message.cc -o .lib/message.o
	g++ $(cppflags) $(apiDir)/hot/fence/service/fence_service_request_handler.cc -o .lib/fence_service_request_handler.o
	g++ $(cppflags) $(apiDir)/hot/fence/service/fence_monitor_service_request_handler.cc -o .lib/fence_monitor_service_request_handler.o
	g++ $(cppflags) $(apiDir)/hot/fence/service/fence_test_service_request_handler.cc -o .lib/fence_test_service_request_handler.o

	g++ $(cppflags) $(apiDir)/hot/transport/enqueue_report.cc -o .lib/enqueue_report.o
	g++ $(cppflags) $(apiDir)/hot/transport/transport_message.cc -o .lib/transport_message.o
	g++ $(cppflags) $(apiDir)/hot/transport/proxy/transport_service_proxy.cc -o .lib/transport_service_proxy.o
	g++ $(cppflags) $(apiDir)/hot/transport/proxy/transport_service_proxy_builder.cc -o .lib/transport_service_proxy_builder.o
	
	g++ $(cppflags) runtime.cc -o .lib/runtime.o
	g++ $(cppflags) fence_service_impl.cc -o .lib/fence_service_impl.o
	g++ $(cppflags) fence_monitor_service_impl.cc -o .lib/fence_monitor_service_impl.o
	g++ $(cppflags) fence_test_service_impl.cc -o .lib/fence_test_service_impl.o
	g++ $(cppflags) slave_thread.cc -o .lib/slave_thread.o
	g++ $(cppflags) main.cc -o .lib/main.o

	g++ -L../../LabCryptoOrg-abettor-c-api/lib/gcc-amd64 \
	    -L../../LabCryptoOrg-abettor-cc-api/lib/gcc-amd64 \
	    .lib/message.o \
	    .lib/fence_service_request_handler.o \
	    .lib/fence_monitor_service_request_handler.o \
	    .lib/fence_test_service_request_handler.o \
	    .lib/enqueue_report.o \
	    .lib/transport_message.o \
	    .lib/transport_service_proxy.o \
	    .lib/transport_service_proxy_builder.o \
	    .lib/runtime.o \
	    .lib/fence_service_impl.o \
	    .lib/fence_monitor_service_impl.o \
	    .lib/fence_test_service_impl.o \
	    .lib/slave_thread.o \
	    .lib/main.o \
	    -lhotd \
	    -lpthread \
	    -labettor-fs \
	    -labettor++-conf \
	    -labettor++-string \
	    -labettor++-os \
	    -labettor++-date \
	    -o fence-slave-node.out

tools:
	make -C stat
	make -C mstat
	make -C enq
	make -C ack
	make -C pop
	
