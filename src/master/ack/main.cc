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
  std::cout << "  ./fence-master-ack [ARGUMENTS]" << std::endl;
  std::cout << std::endl;
  std::cout << "  ARGUMENTS:" << std::endl;
  std::cout << "        -h | --host                Master fence host address [Mandatory]" << std::endl;
  std::cout << "        -p | --port                Master fence port [Mandatory]" << std::endl;
  std::cout << "        -i | --id                  Message id [Mandatory]" << std::endl;
  std::cout << "        -v                         Verbose mode [Optional]" << std::endl;
}

int 
main(int argc, char **argv) {
  try {
    ::org::labcrypto::hottentot::runtime::Logger::Init();
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "LABCRYPTO ORG." << std::endl;
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "COPYRIGHT 2015-2016" << std::endl;
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "FENCE MASTER ACK CLIENT" << std::endl;
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << std::endl;
    ::org::labcrypto::hottentot::runtime::Configuration::Init(argc, argv);
    ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (!::org::labcrypto::hottentot::runtime::Configuration::Exists("h", "host")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "ERROR: Master fence host is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::HasValue("h", "host")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "ERROR: Master fence host is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::Exists("p", "port")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "ERROR: Master fence port is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::HasValue("p", "port")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "ERROR: Master fence port is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::Exists("i", "id")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "ERROR: Message id is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::HasValue("i", "id")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "ERROR: Message id is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    std::string host = ::org::labcrypto::hottentot::runtime::Configuration::AsString("h", "host");
    uint16_t port = ::org::labcrypto::hottentot::runtime::Configuration::AsUInt32("p", "port");
    uint64_t messageId = ::org::labcrypto::hottentot::runtime::Configuration::AsUInt64("i", "id");
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
        ::org::labcrypto::hottentot::UInt64 id(messageId);
        proxy->Ack(id);
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "DONE." << std::endl;
      } else {
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "ERROR: Master fence is not available." << std::endl;
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