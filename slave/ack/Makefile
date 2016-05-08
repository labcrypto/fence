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

apiDir = ../../../LabCryptoOrg-fence-api
cppflags = -Wall -g -c -std=c++11 \
           -I/usr/local/include/hot \
           -I$(apiDir)/hot

all:
	mkdir -p .lib
	rm -rfv .lib/*

	g++ $(cppflags) $(apiDir)/hot/fence/proxy/fence_service_proxy.cc -o .lib/fence_service_proxy.o
	g++ $(cppflags) $(apiDir)/hot/fence/proxy/fence_service_proxy_builder.cc -o .lib/fence_service_proxy_builder.o

	g++ $(cppflags) main.cc -o .lib/main.o

	g++ .lib/fence_service_proxy.o \
	    .lib/fence_service_proxy_builder.o \
	    .lib/main.o \
	    -lhotd \
	    -lpthread \
	    -o fence-slave-ack.out
