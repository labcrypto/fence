#include <iostream>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/service/service_runtime.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include "../common/gate/message.h"

#include "gate_service_impl.h"
#include "slave_thread.h"


int
main(int argc, char **argv) {
  try {
    ::naeem::hottentot::runtime::Logger::Init();
    ::naeem::hottentot::runtime::Configuration::Init(argc, argv);
    if (!::naeem::hottentot::runtime::Configuration::Exists("sid", "slave-id")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Slave id is mandatory. (-sid | --slave-id)" << std::endl;
      return 1;
    }
    if (!::naeem::hottentot::runtime::Configuration::Exists("m", "master")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Master host address is mandatory. (-m | --master)" << std::endl;
      return 1;
    }
    if (!::naeem::hottentot::runtime::Configuration::HasValue("sid", "slave-id")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Slave id is not specified. (-sid | --slave-id)" << std::endl;
      return 1;
    }
    if (!::naeem::hottentot::runtime::Configuration::HasValue("m", "master")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Master host address is not specified. (-m | --master)" << std::endl;
      return 1;
    }
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy runtime is initialized." << std::endl;
    }
    ::naeem::hottentot::runtime::service::ServiceRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Starting server ..." << std::endl;
    }
    ::ir::ntnaeem::gate::slave::SlaveThread::Start();
    ::ir::ntnaeem::gate::slave::GateServiceImpl *service =
        new ::ir::ntnaeem::gate::slave::GateServiceImpl;
    ::naeem::hottentot::runtime::service::ServiceRuntime::Register("0.0.0.0", 8765, service);
    ::naeem::hottentot::runtime::service::ServiceRuntime::Start();
  } catch (...) {
    ::naeem::hottentot::runtime::Logger::GetError() << "Error." << std::endl;
    return 1;
  }
  return 0;
}
