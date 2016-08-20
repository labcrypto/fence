#include <thread>
#include <chrono>
#include <iostream>

#include <org/labcrypto/hottentot/runtime/configuration.h>
#include <org/labcrypto/hottentot/runtime/logger.h>
#include <org/labcrypto/hottentot/runtime/utils.h>
#include <org/labcrypto/hottentot/runtime/proxy/proxy.h>
#include <org/labcrypto/hottentot/runtime/proxy/proxy_runtime.h>

#include <org/labcrypto/abettor/fs.h>

#include <fence/proxy/fence_service.h>
#include <fence/proxy/fence_service_proxy_builder.h>


void PrintHelpMessage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "  ./fence-slave-enq [ARGUMENTS]" << std::endl;
  std::cout << std::endl;
  std::cout << "  ARGUMENTS:" << std::endl;
  std::cout << "        -h | --host                Slave fence host address [Mandatory]" << std::endl;
  std::cout << "        -p | --port                Slave fence port [Mandatory]" << std::endl;
  std::cout << "        -l | --label               Label for enqueued message [Mandatory]" << std::endl;
  std::cout << "        -i | --input               Path to input file [Optional, if not specified stdin will be used]" << std::endl;
  std::cout << "        -v                         Verbose mode [Optional]" << std::endl;
}

int 
main(int argc, char **argv) {
  try {
    ::org::labcrypto::hottentot::runtime::Logger::Init();
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "LABCRYPTO ORG." << std::endl;
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "COPYRIGHT 2015-2016" << std::endl;
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "FENCE SLAVE ENQUEUE CLIENT" << std::endl;
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << std::endl;
    ::org::labcrypto::hottentot::runtime::Configuration::Init(argc, argv);
    ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (!::org::labcrypto::hottentot::runtime::Configuration::Exists("h", "host")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << "ERROR: Slave fence host is not specified." << std::endl;
      PrintHelpMessage();
      ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::HasValue("h", "host")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << "ERROR: Slave fence host is not specified." << std::endl;
      PrintHelpMessage();
      ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::Exists("p", "port")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << "ERROR: Slave fence port is not specified." << std::endl;
      PrintHelpMessage();
      ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::HasValue("p", "port")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << "ERROR: Slave fence port is not specified." << std::endl;
      PrintHelpMessage();
      ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::Exists("l", "label")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << "ERROR: Message label is not specified." << std::endl;
      PrintHelpMessage();
      ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
    if (!::org::labcrypto::hottentot::runtime::Configuration::HasValue("l", "label")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << "ERROR: Message label is not specified." << std::endl;
      PrintHelpMessage();
      ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
    if (::org::labcrypto::hottentot::runtime::Configuration::Exists("i", "input") &&
        !::org::labcrypto::hottentot::runtime::Configuration::HasValue("i", "input")) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << "ERROR: Input file is not specified." << std::endl;
      PrintHelpMessage();
      ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
    std::string host = ::org::labcrypto::hottentot::runtime::Configuration::AsString("h", "host");
    uint16_t port = ::org::labcrypto::hottentot::runtime::Configuration::AsUInt32("p", "port");
    std::string label = ::org::labcrypto::hottentot::runtime::Configuration::AsString("l", "label");
    ORG_LABCRYPTO_ABETTOR_byte data[1024 * 1024];
    ORG_LABCRYPTO_ABETTOR_length dataLength = 0;
    if (::org::labcrypto::hottentot::runtime::Configuration::Exists("i", "input")) {
      std::string inputFilePath = ::org::labcrypto::hottentot::runtime::Configuration::AsString("i", "input");
      ORG_LABCRYPTO_ABETTOR__fs__read_file_with_full_path (
        (ORG_LABCRYPTO_ABETTOR_path)inputFilePath.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_data_ptr)&data, 
        &dataLength
      );
    } else {
      ORG_LABCRYPTO_ABETTOR_byte buffer[128];
      uint32_t j = 0, i = 0;
      uint32_t n = 0;
      while (1) {
        n = fread(buffer, sizeof(char), 128, stdin);
        if (n == 0) {
          break;
        }
        dataLength += n;
        for(i = 0; i < n; i++) {
          data[j] = buffer[i];
          j++;
        }
      }
      if (j != dataLength) {
        printf("Error in reading from pipe\r\n");
        exit(1);
      }
    }
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Proxy runtime is initialized." << std::endl;
    }
    ::org::labcrypto::fence::proxy::FenceService *proxy = 
      ::org::labcrypto::fence::proxy::FenceServiceProxyBuilder::Create(host, port);
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Proxy object is created." << std::endl;
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Target is " << host << ":" << port << std::endl;
    }
    try {
      //=============================================
      if (dynamic_cast< ::org::labcrypto::hottentot::runtime::proxy::Proxy*>(proxy)->IsServerAlive()) {
        ::org::labcrypto::fence::Message message;
        message.SetId(0);
        message.SetLabel(label);
        message.SetRelId(0);
        message.SetContent(::org::labcrypto::hottentot::ByteArray(data, dataLength));
        ::org::labcrypto::hottentot::UInt64 id;
        proxy->Enqueue(message, id);
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Message is enqueued with id : " << id << std::endl;
      } else {
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "ERROR: Slave fence is not available." << std::endl;
      }
      //=============================================
      ::org::labcrypto::fence::proxy::FenceServiceProxyBuilder::Destroy(proxy);
      if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
    } catch (std::exception &e) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << e.what() << std::endl;
      ::org::labcrypto::fence::proxy::FenceServiceProxyBuilder::Destroy(proxy);
      if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
      exit(1);
    } catch (...) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << "Error." << std::endl;
      ::org::labcrypto::fence::proxy::FenceServiceProxyBuilder::Destroy(proxy);
      if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::org::labcrypto::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
  } catch (...) {
    ::org::labcrypto::hottentot::runtime::Logger::GetError() << "Error." << std::endl;
    exit(1);
  }
  return 0;
}