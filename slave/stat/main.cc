#include <thread>
#include <chrono>
#include <iostream>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>
#include <naeem/hottentot/runtime/proxy/proxy.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include <gate/proxy/gate_monitor_service.h>
#include <gate/proxy/gate_monitor_service_proxy_builder.h>


void PrintHelpMessage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "  ./naeem-gate-slave-stat [ARGUMENTS]" << std::endl;
  std::cout << std::endl;
  std::cout << "  ARGUMENTS:" << std::endl;
  std::cout << "        -h | --host                Slave gate host address [Mandatory]" << std::endl;
  std::cout << "        -p | --port                Slave gate port [Mandatory]" << std::endl;
  std::cout << "        -v                         Verbose mode [Optional]" << std::endl;
}

int 
main(int argc, char **argv) {
  try {
    ::naeem::hottentot::runtime::Logger::Init();
    ::naeem::hottentot::runtime::Logger::GetOut() << "NTNAEEM CO." << std::endl;
    ::naeem::hottentot::runtime::Logger::GetOut() << "COPYRIGHT 2015-2016" << std::endl;
    ::naeem::hottentot::runtime::Logger::GetOut() << "NAEEM GATE SLAVE STAT CLIENT" << std::endl;
    ::naeem::hottentot::runtime::Logger::GetOut() << std::endl;
    ::naeem::hottentot::runtime::Configuration::Init(argc, argv);
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (!::naeem::hottentot::runtime::Configuration::Exists("h", "host")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Slave gate host is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::naeem::hottentot::runtime::Configuration::HasValue("h", "host")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Slave gate host is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::naeem::hottentot::runtime::Configuration::Exists("p", "port")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Slave gate port is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    if (!::naeem::hottentot::runtime::Configuration::HasValue("p", "port")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Slave gate port is not specified." << std::endl;
      PrintHelpMessage();
      exit(1);
    }
    std::string host = ::naeem::hottentot::runtime::Configuration::AsString("h", "host");
    uint16_t port = ::naeem::hottentot::runtime::Configuration::AsUInt32("p", "port");
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy runtime is initialized." << std::endl;
    }
    ::ir::ntnaeem::gate::proxy::GateMonitorService *proxy = 
      ::ir::ntnaeem::gate::proxy::GateMonitorServiceProxyBuilder::Create(host, port);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is created." << std::endl;
      ::naeem::hottentot::runtime::Logger::GetOut() << "Target is " << host << ":" << port << std::endl;
    }
    try {
      //=============================================
      if (dynamic_cast< ::naeem::hottentot::runtime::proxy::Proxy*>(proxy)->IsServerAlive()) {
        ::naeem::hottentot::runtime::types::Utf8String stat;
        proxy->GetCurrentStat(stat);
        ::naeem::hottentot::runtime::Logger::GetOut() << stat;
      } else {
        ::naeem::hottentot::runtime::Logger::GetOut() << "ERROR: Slave gate is not available." << std::endl;
      }
      //=============================================
      ::ir::ntnaeem::gate::proxy::GateMonitorServiceProxyBuilder::Destroy(proxy);
      if (::naeem::hottentot::runtime::Configuration::Verbose()) {
        ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::naeem::hottentot::runtime::Logger::Shutdown();
    } catch (std::exception &e) {
      ::naeem::hottentot::runtime::Logger::GetError() << e.what() << std::endl;
      ::ir::ntnaeem::gate::proxy::GateMonitorServiceProxyBuilder::Destroy(proxy);
      if (::naeem::hottentot::runtime::Configuration::Verbose()) {
        ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::naeem::hottentot::runtime::Logger::Shutdown();
      exit(1);
    } catch (...) {
      ::naeem::hottentot::runtime::Logger::GetError() << "Error." << std::endl;
      ::ir::ntnaeem::gate::proxy::GateMonitorServiceProxyBuilder::Destroy(proxy);
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