#include <thread>
#include <chrono>
#include <iostream>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>
#include <naeem/hottentot/runtime/proxy/proxy.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include <transport/proxy/transport_service.h>
#include <transport/proxy/transport_service_proxy_builder.h>


void PrintHelpMessage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "  ./naeem-gate-master-spop [ARGUMENTS]" << std::endl;
  std::cout << std::endl;
  std::cout << "  ARGUMENTS:" << std::endl;
  std::cout << "        -h | --host                Master gate host address [Mandatory]" << std::endl;
  std::cout << "        -p | --port                Master gate port [Mandatory]" << std::endl;
  std::cout << "        -s | --slave-id            Slave id [Mandatory]" << std::endl;
  std::cout << "        -v                         Verbose mode [Optional]" << std::endl;
}

int 
main(int argc, char **argv) {
  try {
    ::naeem::hottentot::runtime::Logger::Init();
    ::naeem::hottentot::runtime::Logger::GetOut() << "NTNAEEM CO." << std::endl;
    ::naeem::hottentot::runtime::Logger::GetOut() << "COPYRIGHT 2015-2016" << std::endl;
    ::naeem::hottentot::runtime::Logger::GetOut() << "NAEEM GATE MASTER S-POP CLIENT" << std::endl;
    ::naeem::hottentot::runtime::Logger::GetOut() << std::endl;
    ::naeem::hottentot::runtime::Configuration::Init(argc, argv);
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (!::naeem::hottentot::runtime::Configuration::Exists("h", "host")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Master gate host is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::naeem::hottentot::runtime::Configuration::HasValue("h", "host")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Master gate host is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::naeem::hottentot::runtime::Configuration::Exists("p", "port")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Master gate port is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::naeem::hottentot::runtime::Configuration::HasValue("p", "port")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Master gate port is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::naeem::hottentot::runtime::Configuration::Exists("s", "slave-id")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Slave id is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::naeem::hottentot::runtime::Configuration::HasValue("s", "slave-id")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Slave id is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    std::string host = ::naeem::hottentot::runtime::Configuration::AsString("h", "host");
    uint16_t port = ::naeem::hottentot::runtime::Configuration::AsUInt32("p", "port");
    uint32_t slaveId = ::naeem::hottentot::runtime::Configuration::AsUInt32("s", "slave-id");
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy runtime is initialized." << std::endl;
    }
    ::ir::ntnaeem::gate::transport::proxy::TransportService *proxy = 
      ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Create(host, port);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is created." << std::endl;
      ::naeem::hottentot::runtime::Logger::GetOut() << "Target is " << host << ":" << port << std::endl;
    }
    try {
      //=============================================
      if (dynamic_cast< ::naeem::hottentot::runtime::proxy::Proxy*>(proxy)->IsServerAlive()) {
        ::naeem::hottentot::runtime::types::UInt32 slaveIdVar(slaveId);
        ::naeem::hottentot::runtime::types::List< 
          ::ir::ntnaeem::gate::transport::TransportMessage> transportMessages;
        proxy->Retrieve(slaveIdVar, transportMessages);
        ::naeem::hottentot::runtime::Logger::GetOut() << 
          "NUMBER OF MESSAGES: " << transportMessages.Size() << std::endl;
        /* if (!hasMore.GetValue()) {
          ::naeem::hottentot::runtime::Logger::GetOut() << "NO MESSAGE IS FOUND WITH THIS LABEL." << std::endl;
        } else {
          ::ir::ntnaeem::gate::Message message;
          proxy->PopNext(labelString, message);
          ::naeem::hottentot::runtime::Logger::GetOut() << 
            "Message is popped with id: " << message.GetId().GetValue() << std::endl;
          ::naeem::hottentot::runtime::Logger::GetOut() << 
            "Id: " << message.GetId().GetValue() << std::endl;
          ::naeem::hottentot::runtime::Logger::GetOut() << 
            "Related Id: " << message.GetRelId().GetValue() << std::endl;
          ::naeem::hottentot::runtime::Logger::GetOut() << 
            "Label: " << message.GetLabel().ToStdString() << std::endl;
          ::naeem::hottentot::runtime::Logger::GetOut() << 
            "Content: " << std::endl << message.GetContent() << std::endl;
        } */
      } else {
        ::naeem::hottentot::runtime::Logger::GetOut() << "ERROR: Gate is not available." << std::endl;
      } 
      //=============================================
      ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Destroy(proxy);
      if (::naeem::hottentot::runtime::Configuration::Verbose()) {
        ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::naeem::hottentot::runtime::Logger::Shutdown();
    } catch (std::exception &e) {
      ::naeem::hottentot::runtime::Logger::GetError() << e.what() << std::endl;
      ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Destroy(proxy);
      if (::naeem::hottentot::runtime::Configuration::Verbose()) {
        ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::naeem::hottentot::runtime::Logger::Shutdown();
      exit(1);
    } catch (...) {
      ::naeem::hottentot::runtime::Logger::GetError() << "Error." << std::endl;
      ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Destroy(proxy);
      if (::naeem::hottentot::runtime::Configuration::Verbose()) {
        ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::naeem::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
  } catch (...) {
    ::naeem::hottentot::runtime::Logger::GetError() << "Error." << std::endl;
    exit(1);
  }
  return 0;
}