#include <iostream>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/service/service_runtime.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include "../common/gate/message.h"
#include "../common/gate_service_impl.h"


// #include "slave_thread.h"


int
main(int argc, char **argv) {
  try {
    ::naeem::hottentot::runtime::Logger::Init();
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy runtime is initialized." << std::endl;
    }
    ::naeem::hottentot::runtime::service::ServiceRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Starting server ..." << std::endl;
    }
    ::ir::ntnaeem::gate::GateServiceImpl *service =
        new ::ir::ntnaeem::gate::GateServiceImpl;
    // ::ir::ntnaeem::gate::slave::SlaveThread::Start();
    ::naeem::hottentot::runtime::service::ServiceRuntime::Register("0.0.0.0", 8765, service);
    ::naeem::hottentot::runtime::service::ServiceRuntime::Start();
  } catch (...) {
    std::cout << "Error." << std::endl;
    return 1;
  }
  return 0;
}
