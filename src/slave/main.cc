#include <iostream>

#include <naeem/os.h>

#include <naeem++/conf/config_manager.h>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/service/service_runtime.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include <gate/message.h>

#include "gate_service_impl.h"
#include "slave_thread.h"
#include "runtime.h"


int
main(int argc, char **argv) {
  try {
    ::naeem::hottentot::runtime::Logger::Init();
    ::naeem::hottentot::runtime::Configuration::Init(argc, argv);
    /* if (!::naeem::hottentot::runtime::Configuration::Exists("sid", "slave-id")) {
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
    } */
    if (!NAEEM_os__file_exists((NAEEM_path)"/opt/naeem/gate", (NAEEM_string)"slave.conf")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: 'slave.conf' does not exist in /opt/naeem/gate directory." << std::endl;
      exit(1);
    }
    ::naeem::conf::ConfigManager::LoadFromFile("/opt/naeem/gate/slave.conf");
    if (!::naeem::conf::ConfigManager::HasSection("slave")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration section 'slave' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasSection("service")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration section 'service' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasSection("master")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration section 'master' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("slave", "id")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration value 'slave.id' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("service", "bind_ip")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration value 'service.bind_ip' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("service", "bind_port")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration value 'service.bind_port' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("master", "ip")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration value 'master.ip' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("master", "port")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration value 'master.port' is not found." << std::endl;
      exit(1);
    }
    std::cout << "NTNAEEM CO." << std::endl;
    std::cout << "COPYRIGHT 2015-2016" << std::endl;
    std::cout << "NAEEM GATE SLAVE SERVICE" << std::endl;
    ::naeem::conf::ConfigManager::Print();
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy runtime is initialized." << std::endl;
    }
    ::naeem::hottentot::runtime::service::ServiceRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Starting server ..." << std::endl;
    }
    ::ir::ntnaeem::gate::slave::Runtime::Init();
    ::ir::ntnaeem::gate::slave::SlaveThread::Start();
    ::ir::ntnaeem::gate::slave::GateServiceImpl *service =
        new ::ir::ntnaeem::gate::slave::GateServiceImpl;
    ::naeem::hottentot::runtime::service::ServiceRuntime::Register(
      ::naeem::conf::ConfigManager::GetValueAsString("service", "bind_ip"), 
      ::naeem::conf::ConfigManager::GetValueAsUInt32("service", "bind_port"), 
      service
    );
    ::naeem::hottentot::runtime::service::ServiceRuntime::Start();
    ::naeem::hottentot::runtime::service::ServiceRuntime::Shutdown();
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::ir::ntnaeem::gate::slave::Runtime::Shutdown();
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Service runtime is shutdown." << std::endl;
      ::naeem::hottentot::runtime::Logger::GetOut() << "About to disable logging system ..." << std::endl;
    }
    ::naeem::hottentot::runtime::Logger::Shutdown();
  } catch (...) {
    ::naeem::hottentot::runtime::Logger::GetError() << "Error." << std::endl;
    return 1;
  }
  return 0;
}
