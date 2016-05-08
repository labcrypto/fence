#include <iostream>

#include <org/labcrypto/abettor/fs.h>

#include <org/labcrypto/abettor++/os/proc.h>
#include <org/labcrypto/abettor++/conf/config_manager.h>
#include <org/labcrypto/abettor++/date/helper.h>

#include <org/labcrypto/hottentot/runtime/configuration.h>
#include <org/labcrypto/hottentot/runtime/logger.h>
#include <org/labcrypto/hottentot/runtime/service/service_runtime.h>
#include <org/labcrypto/hottentot/runtime/proxy/proxy_runtime.h>

#include <fence/message.h>

#include "fence_service_impl.h"
#include "fence_monitor_service_impl.h"
#include "transport_service_impl.h"
#include "transport_monitor_service_impl.h"
#include "master_thread.h"
#include "runtime.h"


int
main(int argc, char **argv) {
  try {
    std::string execDir = ::org::labcrypto::abettor::os::GetExecDir();
    ::org::labcrypto::hottentot::runtime::Logger::Init();
    ::org::labcrypto::hottentot::runtime::Configuration::Init(argc, argv);
    if (!ORG_LABCRYPTO_ABETTOR__fs__file_exists((ORG_LABCRYPTO_ABETTOR_path)execDir.c_str(), (ORG_LABCRYPTO_ABETTOR_string)"master.conf")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: 'master.conf' does not exist in " << execDir << " directory." << std::endl;
      exit(1);
    }
    ::org::labcrypto::abettor::conf::ConfigManager::LoadFromFile(execDir + "/master.conf");
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasSection("master")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration section 'master' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasSection("transport_service")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration section 'transport_service' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasSection("fence_service")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration section 'fence_service' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasValue("transport_service", "bind_ip")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'transport_service.bind_ip' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasValue("transport_service", "bind_port")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'transport_service.bind_port' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasValue("fence_service", "bind_ip")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'fence_service.bind_ip' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasValue("fence_service", "bind_port")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'fence_service.bind_port' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasValue("master", "work_dir")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'master.work_dir' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasValue("master", "ack_timeout")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'master.ack_timeout' is not found." << std::endl;
      exit(1);
    }
    if (!::org::labcrypto::abettor::conf::ConfigManager::HasValue("master", "transfer_interval")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "ERROR: Configuration value 'master.transfer_interval' is not found." << std::endl;
      exit(1);
    }
    std::cout << "LABCRYPTO ORG." << std::endl;
    std::cout << "COPYRIGHT 2015-2016" << std::endl;
    std::cout << "FENCE MASTER SERVICE" << std::endl;
    ::org::labcrypto::abettor::conf::ConfigManager::Print();
    ::org::labcrypto::fence::master::Runtime::Init();
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
    ::org::labcrypto::fence::master::TransportServiceImpl *transportService =
      new ::org::labcrypto::fence::master::TransportServiceImpl;
    ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Register(
      ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("transport_service", "bind_ip"), 
      ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("transport_service", "bind_port"), 
      transportService
    );
    ::org::labcrypto::fence::master::TransportMonitorServiceImpl *transportMonitorService =
      new ::org::labcrypto::fence::master::TransportMonitorServiceImpl;
    ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Register(
      ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("transport_service", "bind_ip"), 
      ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("transport_service", "bind_port"), 
      transportMonitorService
    );
    ::org::labcrypto::fence::master::FenceServiceImpl *fenceService =
      new ::org::labcrypto::fence::master::FenceServiceImpl;
    ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Register(
      ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("fence_service", "bind_ip"), 
      ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("fence_service", "bind_port"), 
      fenceService
    );
    ::org::labcrypto::fence::master::FenceMonitorServiceImpl *fenceMonitorService =
      new ::org::labcrypto::fence::master::FenceMonitorServiceImpl;
    ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Register(
      ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("fence_service", "bind_ip"), 
      ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("fence_service", "bind_port"), 
      fenceMonitorService
    );
    ::org::labcrypto::fence::master::MasterThread::Start();
    ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Start();
    ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Shutdown();
    ::org::labcrypto::fence::master::Runtime::Shutdown();
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Service runtime is shutdown." << std::endl;
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "About to disable logging system ..." << std::endl;
    }
    ::org::labcrypto::abettor::conf::ConfigManager::Clear();
    ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
  } catch (std::exception &e) {
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
        "ERROR: " << e.what() << std::endl;
    ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Shutdown();
    ::org::labcrypto::fence::master::Runtime::Shutdown();
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Service runtime is shutdown." << std::endl;
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " <<
          "About to disable logging system ..." << std::endl;
    }
    ::org::labcrypto::abettor::conf::ConfigManager::Clear();
    ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
    return 1;
  } catch (...) {
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() <<  
      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " <<
        "UNKNOWN ERROR!" << std::endl;
    ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::org::labcrypto::hottentot::runtime::service::ServiceRuntime::Shutdown();
    ::org::labcrypto::fence::master::Runtime::Shutdown();
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " <<
          "Service runtime is shutdown." << std::endl;
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " <<
          "About to disable logging system ..." << std::endl;
    }
    ::org::labcrypto::abettor::conf::ConfigManager::Clear();
    ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
    return 1;
  }
  return 0;
}
