#include <iostream>

#include <naeem/os.h>

#include <naeem++/conf/config_manager.h>
#include <naeem++/os/proc.h>
#include <naeem++/date/helper.h>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/service/service_runtime.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include <gate/message.h>

#include "gate_service_impl.h"
#include "gate_monitor_service_impl.h"
#include "gate_test_service_impl.h"
#include "slave_thread.h"
#include "runtime.h"


int
main(int argc, char **argv) {
  try {
    std::string execDir = ::naeem::os::GetExecDir();
    ::naeem::hottentot::runtime::Logger::Init();
    ::naeem::hottentot::runtime::Configuration::Init(argc, argv);
    if (!NAEEM_os__file_exists((NAEEM_path)execDir.c_str(), (NAEEM_string)"slave.conf")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: 'slave.conf' does not exist in " << execDir << " directory." << std::endl;
      exit(1);
    }
    ::naeem::conf::ConfigManager::LoadFromFile(execDir + "/slave.conf");
    if (!::naeem::conf::ConfigManager::HasSection("slave")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration section 'slave' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasSection("master")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration section 'slave' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasSection("service")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration section 'service' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("slave", "id")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'slave.id' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("slave", "work_dir")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'slave.work_dir' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("slave", "ack_timeout")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'slave.ack_timeout' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("slave", "transfer_interval")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'slave.transfer_interval' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("service", "bind_ip")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'service.bind_ip' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("service", "bind_port")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'service.bind_port' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("master", "ip")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'master.ip' is not found." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("master", "port")) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'master.port' is not found." << std::endl;
      exit(1);
    }
    std::cout << "NTNAEEM CO." << std::endl;
    std::cout << "COPYRIGHT 2015-2016" << std::endl;
    std::cout << "NAEEM GATE SLAVE SERVICE" << std::endl;
    ::naeem::conf::ConfigManager::Print();
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Proxy runtime is initialized." << std::endl;
    }
    ::naeem::hottentot::runtime::service::ServiceRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Starting server ..." << std::endl;
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
    ::ir::ntnaeem::gate::slave::GateMonitorServiceImpl *monitorService =
        new ::ir::ntnaeem::gate::slave::GateMonitorServiceImpl;
    ::naeem::hottentot::runtime::service::ServiceRuntime::Register(
      ::naeem::conf::ConfigManager::GetValueAsString("service", "bind_ip"), 
      ::naeem::conf::ConfigManager::GetValueAsUInt32("service", "bind_port"), 
      monitorService
    );
    ::ir::ntnaeem::gate::slave::GateTestServiceImpl *testService =
        new ::ir::ntnaeem::gate::slave::GateTestServiceImpl;
    ::naeem::hottentot::runtime::service::ServiceRuntime::Register(
      ::naeem::conf::ConfigManager::GetValueAsString("service", "bind_ip"), 
      ::naeem::conf::ConfigManager::GetValueAsUInt32("service", "bind_port"), 
      testService
    );
    if (::naeem::conf::ConfigManager::HasValue("service", "bind_ip2") &&
        ::naeem::conf::ConfigManager::HasValue("service", "bind_port2")) {
      ::ir::ntnaeem::gate::slave::GateServiceImpl *service2 =
        new ::ir::ntnaeem::gate::slave::GateServiceImpl;
      ::naeem::hottentot::runtime::service::ServiceRuntime::Register(
        ::naeem::conf::ConfigManager::GetValueAsString("service", "bind_ip2"), 
        ::naeem::conf::ConfigManager::GetValueAsUInt32("service", "bind_port2"), 
        service2
      );
      ::ir::ntnaeem::gate::slave::GateMonitorServiceImpl *monitorService2 =
          new ::ir::ntnaeem::gate::slave::GateMonitorServiceImpl;
      ::naeem::hottentot::runtime::service::ServiceRuntime::Register(
        ::naeem::conf::ConfigManager::GetValueAsString("service", "bind_ip2"), 
        ::naeem::conf::ConfigManager::GetValueAsUInt32("service", "bind_port2"), 
        monitorService2
      );
      ::ir::ntnaeem::gate::slave::GateTestServiceImpl *testService2 =
          new ::ir::ntnaeem::gate::slave::GateTestServiceImpl;
      ::naeem::hottentot::runtime::service::ServiceRuntime::Register(
        ::naeem::conf::ConfigManager::GetValueAsString("service", "bind_ip2"), 
        ::naeem::conf::ConfigManager::GetValueAsUInt32("service", "bind_port2"), 
        testService2
      );
    }
    if (::naeem::conf::ConfigManager::HasValue("service", "bind_ip3") &&
        ::naeem::conf::ConfigManager::HasValue("service", "bind_port3")) {
      ::ir::ntnaeem::gate::slave::GateServiceImpl *service3 =
        new ::ir::ntnaeem::gate::slave::GateServiceImpl;
      ::naeem::hottentot::runtime::service::ServiceRuntime::Register(
        ::naeem::conf::ConfigManager::GetValueAsString("service", "bind_ip3"), 
        ::naeem::conf::ConfigManager::GetValueAsUInt32("service", "bind_port3"), 
        service3
      );
      ::ir::ntnaeem::gate::slave::GateMonitorServiceImpl *monitorService3 =
          new ::ir::ntnaeem::gate::slave::GateMonitorServiceImpl;
      ::naeem::hottentot::runtime::service::ServiceRuntime::Register(
        ::naeem::conf::ConfigManager::GetValueAsString("service", "bind_ip3"), 
        ::naeem::conf::ConfigManager::GetValueAsUInt32("service", "bind_port3"), 
        monitorService3
      );
      ::ir::ntnaeem::gate::slave::GateTestServiceImpl *testService3 =
          new ::ir::ntnaeem::gate::slave::GateTestServiceImpl;
      ::naeem::hottentot::runtime::service::ServiceRuntime::Register(
        ::naeem::conf::ConfigManager::GetValueAsString("service", "bind_ip3"), 
        ::naeem::conf::ConfigManager::GetValueAsUInt32("service", "bind_port3"), 
        testService3
      );
    }
    ::naeem::hottentot::runtime::service::ServiceRuntime::Start();
    ::naeem::hottentot::runtime::service::ServiceRuntime::Shutdown();
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::ir::ntnaeem::gate::slave::Runtime::Shutdown();
    ::naeem::conf::ConfigManager::Clear();
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Service runtime is shutdown." << std::endl;
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "About to disable logging system ..." << std::endl;
    }
    ::naeem::hottentot::runtime::Logger::Shutdown();
  } catch (...) {
    ::naeem::hottentot::runtime::Logger::GetError() << 
      "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
        "Error." << std::endl;
    return 1;
  }
  return 0;
}
