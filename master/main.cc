#include <iostream>

#include <naeem/os.h>

#include <naeem++/os/proc.h>
#include <naeem++/conf/config_manager.h>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/service/service_runtime.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include <gate/message.h>

#include "gate_service_impl.h"
#include "gate_monitor_service_impl.h"
#include "transport_service_impl.h"
#include "transport_monitor_service_impl.h"
#include "master_thread.h"
#include "runtime.h"


int
main(int argc, char **argv) {
  try {
    std::string execDir = ::naeem::os::GetExecDir();
    ::naeem::hottentot::runtime::Logger::Init();
    ::naeem::hottentot::runtime::Configuration::Init(argc, argv);
    if (!NAEEM_os__file_exists((NAEEM_path)execDir.c_str(), (NAEEM_string)"master.conf")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: 'master.conf' does not exist in " << execDir << " directory." << std::endl;
      exit(1);
    }
    ::naeem::conf::ConfigManager::LoadFromFile(execDir + "/master.conf");
    if (!::naeem::conf::ConfigManager::HasSection("master")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration section 'master' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasSection("transport_service")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration section 'transport_service' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasSection("gate_service")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration section 'gate_service' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("transport_service", "bind_ip")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration value 'transport_service.bind_ip' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("transport_service", "bind_port")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration value 'transport_service.bind_port' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("gate_service", "bind_ip")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration value 'gate_service.bind_ip' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("gate_service", "bind_port")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration value 'gate_service.bind_port' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("master", "work_dir")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration value 'master.work_dir' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("master", "ack_timeout")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration value 'master.ack_timeout' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("master", "transfer_interval")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "ERROR: Configuration value 'master.transfer_interval' is not found." << std::endl;
      exit(1);
    }
    std::cout << "NTNAEEM CO." << std::endl;
    std::cout << "COPYRIGHT 2015-2016" << std::endl;
    std::cout << "NAEEM GATE MASTER SERVICE" << std::endl;
    ::naeem::conf::ConfigManager::Print();
    ::ir::ntnaeem::gate::master::Runtime::Init();
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy runtime is initialized." << std::endl;
    }
    ::naeem::hottentot::runtime::service::ServiceRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Starting server ..." << std::endl;
    }
    ::ir::ntnaeem::gate::master::TransportServiceImpl *transportService =
      new ::ir::ntnaeem::gate::master::TransportServiceImpl;
    ::naeem::hottentot::runtime::service::ServiceRuntime::Register(
      ::naeem::conf::ConfigManager::GetValueAsString("transport_service", "bind_ip"), 
      ::naeem::conf::ConfigManager::GetValueAsUInt32("transport_service", "bind_port"), 
      transportService
    );
    ::ir::ntnaeem::gate::master::TransportMonitorServiceImpl *transportMonitorService =
      new ::ir::ntnaeem::gate::master::TransportMonitorServiceImpl;
    ::naeem::hottentot::runtime::service::ServiceRuntime::Register(
      ::naeem::conf::ConfigManager::GetValueAsString("transport_service", "bind_ip"), 
      ::naeem::conf::ConfigManager::GetValueAsUInt32("transport_service", "bind_port"), 
      transportMonitorService
    );
    ::ir::ntnaeem::gate::master::GateServiceImpl *gateService =
      new ::ir::ntnaeem::gate::master::GateServiceImpl;
    ::naeem::hottentot::runtime::service::ServiceRuntime::Register(
      ::naeem::conf::ConfigManager::GetValueAsString("gate_service", "bind_ip"), 
      ::naeem::conf::ConfigManager::GetValueAsUInt32("gate_service", "bind_port"), 
      gateService
    );
    ::ir::ntnaeem::gate::master::GateMonitorServiceImpl *gateMonitorService =
      new ::ir::ntnaeem::gate::master::GateMonitorServiceImpl;
    ::naeem::hottentot::runtime::service::ServiceRuntime::Register(
      ::naeem::conf::ConfigManager::GetValueAsString("gate_service", "bind_ip"), 
      ::naeem::conf::ConfigManager::GetValueAsUInt32("gate_service", "bind_port"), 
      gateMonitorService
    );
    ::ir::ntnaeem::gate::master::MasterThread::Start();
    ::naeem::hottentot::runtime::service::ServiceRuntime::Start();
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::naeem::hottentot::runtime::service::ServiceRuntime::Shutdown();
    ::ir::ntnaeem::gate::master::Runtime::Shutdown();
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Service runtime is shutdown." << std::endl;
      ::naeem::hottentot::runtime::Logger::GetOut() << "About to disable logging system ..." << std::endl;
    }
    ::naeem::conf::ConfigManager::Clear();
    ::naeem::hottentot::runtime::Logger::Shutdown();
  } catch (std::exception &e) {
    std::cout << "ERROR: " << e.what() << std::endl;
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::naeem::hottentot::runtime::service::ServiceRuntime::Shutdown();
    ::ir::ntnaeem::gate::master::Runtime::Shutdown();
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Service runtime is shutdown." << std::endl;
      ::naeem::hottentot::runtime::Logger::GetOut() << "About to disable logging system ..." << std::endl;
    }
    ::naeem::conf::ConfigManager::Clear();
    ::naeem::hottentot::runtime::Logger::Shutdown();
    return 1;
  } catch (...) {
    std::cout << "UNKNOWN ERROR!" << std::endl;
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::naeem::hottentot::runtime::service::ServiceRuntime::Shutdown();
    ::ir::ntnaeem::gate::master::Runtime::Shutdown();
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Service runtime is shutdown." << std::endl;
      ::naeem::hottentot::runtime::Logger::GetOut() << "About to disable logging system ..." << std::endl;
    }
    ::naeem::conf::ConfigManager::Clear();
    ::naeem::hottentot::runtime::Logger::Shutdown();
    return 1;
  }
  return 0;
}
