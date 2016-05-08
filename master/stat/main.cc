#include <thread>
#include <chrono>
#include <iostream>

#include <org/labcrypto/hottentot/runtime/configuration.h>
#include <org/labcrypto/hottentot/runtime/logger.h>
#include <org/labcrypto/hottentot/runtime/utils.h>
#include <org/labcrypto/hottentot/runtime/proxy/proxy.h>
#include <org/labcrypto/hottentot/runtime/proxy/proxy_runtime.h>

#include <transport/proxy/transport_monitor_service.h>
#include <transport/proxy/transport_monitor_service_proxy_builder.h>


void PrintHelpMessage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "  ./fence-master-stat [ARGUMENTS]" << std::endl;
  std::cout << std::endl;
  std::cout << "  ARGUMENTS:" << std::endl;
  std::cout << "        -h | --host                Master fence host address [Mandatory]" << std::endl;
  std::cout << "        -p | --port                Master fence port [Mandatory]" << std::endl;
  std::cout << "        -v                         Verbose mode [Optional]" << std::endl;
}

int 
main(int argc, char **argv) {
  try {
    ::org::labcrypto::hottentot::runtime::Logger::Init();
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "LABCRYPTO ORG." << std::endl;
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "COPYRIGHT 2015-2016" << std::endl;
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "FENCE MASTER STAT CLIENT" << std::endl;
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << std::endl;
    ::org::labcrypto::hottentot::runtime::Configuration::Init(argc, argv);
    ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (!::org::labcrypto::hottentot::runtime::Configuration::Exists("h", "host")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "ERROR: Gate host is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::HasValue("h", "host")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "ERROR: Gate host is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::Exists("p", "port")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "ERROR: Gate port is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::HasValue("p", "port")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "ERROR: Gate port is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    std::string host = ::org::labcrypto::hottentot::runtime::Configuration::AsString("h", "host");
    uint16_t port = ::org::labcrypto::hottentot::runtime::Configuration::AsUInt32("p", "port");
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Proxy runtime is initialized." << std::endl;
    }
    ::org::labcrypto::fence::transport::proxy::TransportMonitorService *proxy = 
      ::org::labcrypto::fence::transport::proxy::TransportMonitorServiceProxyBuilder::Create(host, port);
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Proxy object is created." << std::endl;
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Target is " << host << ":" << port << std::endl;
    }
    try {
      //=============================================
      if (dynamic_cast< ::org::labcrypto::hottentot::runtime::proxy::Proxy*>(proxy)->IsServerAlive()) {
        ::org::labcrypto::hottentot::Utf8String stat;
        proxy->GetCurrentStat(stat);
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << stat;
      } else {
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "ERROR: Gate is not available." << std::endl;
      }
      //=============================================
      ::org::labcrypto::fence::transport::proxy::TransportMonitorServiceProxyBuilder::Destroy(proxy);
      if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
    } catch (std::exception &e) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << e.what() << std::endl;
      ::org::labcrypto::fence::transport::proxy::TransportMonitorServiceProxyBuilder::Destroy(proxy);
      if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
      exit(1);
    } catch (...) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << "Error." << std::endl;
      ::org::labcrypto::fence::transport::proxy::TransportMonitorServiceProxyBuilder::Destroy(proxy);
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