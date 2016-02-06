#include <iostream>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/service/service_runtime.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include "../common/gate/message.h"

#include "gate_service_impl.h"
#include "transport_service_impl.h"
#include "master_thread.h"
#include "runtime.h"


int
main(int argc, char **argv) {
  try {
    ::naeem::hottentot::runtime::Logger::Init();
    ::naeem::hottentot::runtime::Configuration::Init(argc, argv);
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy runtime is initialized." << std::endl;
    }
    ::naeem::hottentot::runtime::service::ServiceRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Starting server ..." << std::endl;
    }
    ::ir::ntnaeem::gate::master::Runtime::Init();
    ::ir::ntnaeem::gate::master::GateServiceImpl *gateService =
      new ::ir::ntnaeem::gate::master::GateServiceImpl;
    ::ir::ntnaeem::gate::master::TransportServiceImpl *transportService =
      new ::ir::ntnaeem::gate::master::TransportServiceImpl;
    ::ir::ntnaeem::gate::master::MasterThread::Start();
    ::naeem::hottentot::runtime::service::ServiceRuntime::Register("0.0.0.0", 8767, gateService);
    ::naeem::hottentot::runtime::service::ServiceRuntime::Register("0.0.0.0", 8766, transportService);
    ::naeem::hottentot::runtime::service::ServiceRuntime::Start();
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::naeem::hottentot::runtime::service::ServiceRuntime::Shutdown();
    ::ir::ntnaeem::gate::master::Runtime::Shutdown();
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Service runtime is shutdown." << std::endl;
      ::naeem::hottentot::runtime::Logger::GetOut() << "About to disable logging system ..." << std::endl;
    }
    ::naeem::hottentot::runtime::Logger::Shutdown();
  } catch (...) {
    std::cout << "Error." << std::endl;
    return 1;
  }
  return 0;
}
