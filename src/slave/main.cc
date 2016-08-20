#include <iostream>

#include <org/labcrypto/abettor/fs.h>

#include <org/labcrypto/hottentot/runtime/configuration.h>
#include <org/labcrypto/hottentot/runtime/logger.h>
#include <org/labcrypto/hottentot/runtime/service/service_runtime.h>
#include <org/labcrypto/hottentot/runtime/proxy/proxy_runtime.h>

#include <org/labcrypto/abettor++/conf/config_manager.h>
#include <org/labcrypto/abettor++/os/proc.h>
#include <org/labcrypto/abettor++/date/helper.h>

#include <fence/message.h>

#include "fence_service_impl.h"
#include "fence_monitor_service_impl.h"
#include "fence_test_service_impl.h"
#include "slave_thread.h"
#include "runtime.h"


int
main(int argc, char **argv) {
  try {
    std::string execDir = ::org::labcrypto::abettor::os::GetExecDir();
    ::org::labcrypto::hottentot::runtime::Logger::Init();
    ::org::labcrypto::hottentot::runtime::Configuration::Init(argc, argv);
    if (!ORG_LABCRYPTO_ABETTOR__fs__file_exists((ORG_LABCRYPTO_ABETTOR_path)execDir.c_str(), (ORG_LABCRYPTO_ABETTOR_string)"slave.conf")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: 'slave.conf' does not exist in " << execDir << " directory." << std::endl;
      exit(1);
    }
    ::org::labcrypto::abettor::conf::ConfigManager::LoadFromFile(execDir + "/slave.conf");
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasSection("slave")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration section 'slave' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasSection("master")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration section 'slave' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasSection("service")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration section 'service' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasValue("slave", "id")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'slave.id' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasValue("slave", "work_dir")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'slave.work_dir' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasValue("slave", "ack_timeout")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'slave.ack_timeout' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasValue("slave", "transfer_interval")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'slave.transfer_interval' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasValue("service", "bind_ip")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'service.bind_ip' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasValue("service", "bind_port")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'service.bind_port' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasValue("master", "ip")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'master.ip' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasValue("master", "port")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'master.port' is not found." << std::endl;
      exit(1);
    }
    std::cout << "LABCRYPTO ORG." << std::endl;
    std::cout << "COPYRIGHT 2015-2016" << std::endl;
    std::cout << "FENCE SLAVE SERVICE" << std::endl;
    ::org::labcrypto::abettor::conf::ConfigManager::Print();
    ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Proxy runtime is initialized." << std::endl;
    }
    ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Init(argc, argv);
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Starting server ..." << std::endl;
    }
    ::org::labcrypto::fence::slave::Runtime::Init();
    ::org::labcrypto::fence::slave::SlaveThread::Start();
    ::org::labcrypto::fence::slave::FenceServiceImpl *service =
        new ::org::labcrypto::fence::slave::FenceServiceImpl;
    ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Register(
      ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("service", "bind_ip"), 
      ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("service", "bind_port"), 
      service
    );
    ::org::labcrypto::fence::slave::FenceMonitorServiceImpl *monitorService =
        new ::org::labcrypto::fence::slave::FenceMonitorServiceImpl;
    ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Register(
      ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("service", "bind_ip"), 
      ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("service", "bind_port"), 
      monitorService
    );
    ::org::labcrypto::fence::slave::FenceTestServiceImpl *testService =
        new ::org::labcrypto::fence::slave::FenceTestServiceImpl;
    ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Register(
      ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("service", "bind_ip"), 
      ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("service", "bind_port"), 
      testService
    );
    if (::org::labcrypto::abettor::conf::ConfigManager::HasValue("service", "bind_ip2") &&
        ::org::labcrypto::abettor::conf::ConfigManager::HasValue("service", "bind_port2")) {
      ::org::labcrypto::fence::slave::FenceServiceImpl *service2 =
        new ::org::labcrypto::fence::slave::FenceServiceImpl;
      ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Register(
        ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("service", "bind_ip2"), 
        ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("service", "bind_port2"), 
        service2
      );
      ::org::labcrypto::fence::slave::FenceMonitorServiceImpl *monitorService2 =
          new ::org::labcrypto::fence::slave::FenceMonitorServiceImpl;
      ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Register(
        ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("service", "bind_ip2"), 
        ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("service", "bind_port2"), 
        monitorService2
      );
      ::org::labcrypto::fence::slave::FenceTestServiceImpl *testService2 =
          new ::org::labcrypto::fence::slave::FenceTestServiceImpl;
      ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Register(
        ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("service", "bind_ip2"), 
        ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("service", "bind_port2"), 
        testService2
      );
    }
    if (::org::labcrypto::abettor::conf::ConfigManager::HasValue("service", "bind_ip3") &&
        ::org::labcrypto::abettor::conf::ConfigManager::HasValue("service", "bind_port3")) {
      ::org::labcrypto::fence::slave::FenceServiceImpl *service3 =
        new ::org::labcrypto::fence::slave::FenceServiceImpl;
      ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Register(
        ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("service", "bind_ip3"), 
        ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("service", "bind_port3"), 
        service3
      );
      ::org::labcrypto::fence::slave::FenceMonitorServiceImpl *monitorService3 =
          new ::org::labcrypto::fence::slave::FenceMonitorServiceImpl;
      ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Register(
        ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("service", "bind_ip3"), 
        ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("service", "bind_port3"), 
        monitorService3
      );
      ::org::labcrypto::fence::slave::FenceTestServiceImpl *testService3 =
          new ::org::labcrypto::fence::slave::FenceTestServiceImpl;
      ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Register(
        ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("service", "bind_ip3"), 
        ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("service", "bind_port3"), 
        testService3
      );
    }
    ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Start();
    ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Shutdown();
    ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::org::labcrypto::fence::slave::Runtime::Shutdown();
    ::org::labcrypto::abettor::conf::ConfigManager::Clear();
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Service runtime is shutdown." << std::endl;
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "About to disable logging system ..." << std::endl;
    }
    ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
  } catch (...) {
    ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
        "Error." << std::endl;
    return 1;
  }
  return 0;
}
