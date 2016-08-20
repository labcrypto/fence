#include <thread>
#include <chrono>
#include <iostream>

#include <org/labcrypto/hottentot/runtime/configuration.h>
#include <org/labcrypto/hottentot/runtime/logger.h>
#include <org/labcrypto/hottentot/runtime/utils.h>
#include <org/labcrypto/hottentot/runtime/proxy/proxy.h>
#include <org/labcrypto/hottentot/runtime/proxy/proxy_runtime.h>

#include <fence/proxy/fence_service.h>
#include <fence/proxy/fence_service_proxy_builder.h>


void PrintHelpMessage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "  ./fence-slave-pop [ARGUMENTS]" << std::endl;
  std::cout << std::endl;
  std::cout << "  ARGUMENTS:" << std::endl;
  std::cout << "        -h | --host                Slave fence host address [Mandatory]" << std::endl;
  std::cout << "        -p | --port                Slave fence port [Mandatory]" << std::endl;
  std::cout << "        -l | --label               Pop from queue with this label [Mandatory]" << std::endl;
  std::cout << "        -v                         Verbose mode [Optional]" << std::endl;
}

int 
main(int argc, char **argv) {
  try {
    ::org::labcrypto::hottentot::runtime::Logger::Init();
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "LABCRYPTO ORG." << std::endl;
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "COPYRIGHT 2015-2016" << std::endl;
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "FENCE SLAVE POP CLIENT" << std::endl;
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << std::endl;
    ::org::labcrypto::hottentot::runtime::Configuration::Init(argc, argv);
    ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (!::org::labcrypto::hottentot::runtime::Configuration::Exists("h", "host")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << "ERROR: Slave fence host is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::HasValue("h", "host")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << "ERROR: Slave fence host is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::Exists("p", "port")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << "ERROR: Slave fence port is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::HasValue("p", "port")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << "ERROR: Slave fence port is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::Exists("l", "label")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << "ERROR: Message label is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::HasValue("l", "label")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << "ERROR: Message label is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    std::string host = ::org::labcrypto::hottentot::runtime::Configuration::AsString("h", "host");
    uint16_t port = ::org::labcrypto::hottentot::runtime::Configuration::AsUInt32("p", "port");
    std::string label = ::org::labcrypto::hottentot::runtime::Configuration::AsString("l", "label");
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Proxy runtime is initialized." << std::endl;
    }
    ::org::labcrypto::fence::proxy::FenceService *proxy = 
      ::org::labcrypto::fence::proxy::FenceServiceProxyBuilder::Create(host, port);
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Proxy object is created." << std::endl;
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Target is " << host << ":" << port << std::endl;
    }
    try {
      //=============================================
      if (dynamic_cast< ::org::labcrypto::hottentot::runtime::proxy::Proxy*>(proxy)->IsServerAlive()) {
        ::org::labcrypto::hottentot::Boolean hasMore;
        ::org::labcrypto::hottentot::Utf8String labelString(label);
        proxy->HasMore(labelString, hasMore);
        if (!hasMore.GetValue()) {
          ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "NO MESSAGE IS FOUND WITH THIS LABEL." << std::endl;
        } else {
          ::org::labcrypto::fence::Message message;
          proxy->PopNext(labelString, message);
          ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
            "Message is popped with id: " << message.GetId().GetValue() << std::endl;
          ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
            "Id: " << message.GetId().GetValue() << std::endl;
          ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
            "Related Id: " << message.GetRelId().GetValue() << std::endl;
          ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
            "Label: " << message.GetLabel().ToStdString() << std::endl;
          ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
            "Content: " << std::endl << message.GetContent() << std::endl;
        }
      } else {
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "ERROR: Fence is not available." << std::endl;
      }
      //=============================================
      ::org::labcrypto::fence::proxy::FenceServiceProxyBuilder::Destroy(proxy);
      if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
    } catch (std::exception &e) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << e.what() << std::endl;
      ::org::labcrypto::fence::proxy::FenceServiceProxyBuilder::Destroy(proxy);
      if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
      exit(1);
    } catch (...) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << "Error." << std::endl;
      ::org::labcrypto::fence::proxy::FenceServiceProxyBuilder::Destroy(proxy);
      if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
  } catch (...) {
    ::org::labcrypto::hottentot::runtime::Logger::GetError() << "Error." << std::endl;
    exit(1);
  }
  return 0;
}