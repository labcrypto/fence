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

cppflags=-Wall -g -c -fPIC -std=c++11 \
         -I/usr/local/include/hot \
         -I../../LabCryptoOrg-fence-api/hot \
         -I../../LabCryptoOrg-fence-api/client/include \
         -I../../LabCryptoOrg-abettor-c-api/include \
         -I../../LabCryptoOrg-abettor-cc-api/include
PWD=$(shell pwd)
apiDir=$(PWD)/../../LabCryptoOrg-fence-api
libName=libfence-client
majorVersion=1
minorVersion=0

all:
	mkdir -p .lib
	rm -rfv .lib/*
	
	g++ $(cppflags) $(apiDir)/hot/fence/message.cc -o .lib/message.o
	g++ $(cppflags) $(apiDir)/hot/fence/proxy/fence_service_proxy.cc -o .lib/fence_service_proxy.o
	g++ $(cppflags) $(apiDir)/hot/fence/proxy/fence_service_proxy_builder.cc -o .lib/fence_service_proxy_builder.o

	g++ $(cppflags) runtime.cc -o .lib/runtime.o
	g++ $(cppflags) default_message_receiver.cc -o .lib/default_message_receiver.o
	g++ $(cppflags) default_message_submitter.cc -o .lib/default_message_submitter.o
	g++ $(cppflags) submitter_thread.cc -o .lib/submitter_thread.o
	g++ $(cppflags) receiver_thread.cc -o .lib/receiver_thread.o

	rm -rf .lib/$(libName).a.$(majorVersion).$(minorVersion)
	ar -cq .lib/$(libName).a.$(majorVersion).$(minorVersion) \
         .lib/message.o \
         .lib/fence_service_proxy.o \
         .lib/fence_service_proxy_builder.o \
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
         .lib/fence_service_proxy.o \
         .lib/fence_service_proxy_builder.o \
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
