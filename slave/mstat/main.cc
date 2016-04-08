#include <thread>
#include <chrono>
#include <sstream>
#include <iostream>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>
#include <naeem/hottentot/runtime/proxy/proxy.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include <gate/proxy/gate_service.h>
#include <gate/proxy/gate_service_proxy_builder.h>


void PrintHelpMessage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "  ./naeem-gate-slave-mstatus [ARGUMENTS]" << std::endl;
  std::cout << std::endl;
  std::cout << "  ARGUMENTS:" << std::endl;
  std::cout << "        -h | --host                Slave gate host address [Mandatory]" << std::endl;
  std::cout << "        -p | --port                Slave gate port [Mandatory]" << std::endl;
  std::cout << "        -i | --id                  Message id [Mandatory]" << std::endl;
  std::cout << "        -v                         Verbose mode [Optional]" << std::endl;
  ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
  ::naeem::hottentot::runtime::Logger::Shutdown();
  exit(1);
}

int 
main(int argc, char **argv) {
  try {
    ::naeem::hottentot::runtime::Logger::Init();
    ::naeem::hottentot::runtime::Logger::GetOut() << "NTNAEEM CO." << std::endl;
    ::naeem::hottentot::runtime::Logger::GetOut() << "COPYRIGHT 2015-2016" << std::endl;
    ::naeem::hottentot::runtime::Logger::GetOut() << "NAEEM GATE SLAVE MESSAGE STATUS CLIENT" << std::endl;
    ::naeem::hottentot::runtime::Logger::GetOut() << std::endl;
    ::naeem::hottentot::runtime::Configuration::Init(argc, argv);
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (!::naeem::hottentot::runtime::Configuration::Exists("h", "host")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Slave gate host is not specified." << std::endl;
      PrintHelpMessage();
    }
    if (!::naeem::hottentot::runtime::Configuration::HasValue("h", "host")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Slave gate host is not specified." << std::endl;
      PrintHelpMessage();
    }
    if (!::naeem::hottentot::runtime::Configuration::Exists("p", "port")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Slave gate port is not specified." << std::endl;
      PrintHelpMessage();
    }
    if (!::naeem::hottentot::runtime::Configuration::HasValue("p", "port")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Slave gate port is not specified." << std::endl;
      PrintHelpMessage();
    }
    if (!::naeem::hottentot::runtime::Configuration::Exists("i", "id")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Message id is not specified." << std::endl;
      PrintHelpMessage();
    }
    if (!::naeem::hottentot::runtime::Configuration::HasValue("i", "id")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Message id is not specified." << std::endl;
      PrintHelpMessage();
    }
    std::string host = ::naeem::hottentot::runtime::Configuration::AsString("h", "host");
    uint16_t port = ::naeem::hottentot::runtime::Configuration::AsUInt32("p", "port");
    uint64_t id = ::naeem::hottentot::runtime::Configuration::AsUInt64("i", "id");
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy runtime is initialized." << std::endl;
    }
    ::ir::ntnaeem::gate::proxy::GateService *proxy = 
      ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Create(host, port);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is created." << std::endl;
      ::naeem::hottentot::runtime::Logger::GetOut() << "Target is " << host << ":" << port << std::endl;
    }
    try {
      //=============================================
      if (dynamic_cast< ::naeem::hottentot::runtime::proxy::Proxy*>(proxy)->IsServerAlive()) {
        ::naeem::hottentot::runtime::types::UInt64 idVar(id);
        ::naeem::hottentot::runtime::types::UInt16 status;
        proxy->GetStatus(idVar, status);
        if (status.GetValue() == ::ir::ntnaeem::gate::kMessageStatus___Unknown) {
          ::naeem::hottentot::runtime::Logger::GetOut() << "Status: UNKNOWN" << std::endl;
        } else if (status.GetValue() == ::ir::ntnaeem::gate::kMessageStatus___EnqueuedForTransmission) {
          ::naeem::hottentot::runtime::Logger::GetOut() << "Status: ENQUEUED FOR TRANSMISSION" << std::endl;
        } else if (status.GetValue() == ::ir::ntnaeem::gate::kMessageStatus___Transmitted) {
          ::naeem::hottentot::runtime::Logger::GetOut() << "Status: TRANSMITTED" << std::endl;
        } else if (status.GetValue() == ::ir::ntnaeem::gate::kMessageStatus___TransmissionFailed) {
          ::naeem::hottentot::runtime::Logger::GetOut() << "Status: TRANSMISSION FAILED" << std::endl;
        } else {
          std::stringstream ss;
          ss << "Not known status: " << status.GetValue();
          throw std::runtime_error(ss.str());
        }
      } else {
        ::naeem::hottentot::runtime::Logger::GetOut() << "ERROR: Slave gate is not available." << std::endl;
      }
      //=============================================
      ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
      if (::naeem::hottentot::runtime::Configuration::Verbose()) {
        ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::naeem::hottentot::runtime::Logger::Shutdown();
    } catch (std::exception &e) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: " << e.what() << std::endl;
      ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
      if (::naeem::hottentot::runtime::Configuration::Verbose()) {
        ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::naeem::hottentot::runtime::Logger::Shutdown();
      exit(1);
    } catch (...) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR!" << std::endl;
      ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
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