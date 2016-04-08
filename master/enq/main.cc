#include <thread>
#include <chrono>
#include <iostream>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>
#include <naeem/hottentot/runtime/proxy/proxy.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include <naeem/os.h>

#include <gate/proxy/gate_service.h>
#include <gate/proxy/gate_service_proxy_builder.h>


void PrintHelpMessage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "  ./naeem-gate-master-enq [ARGUMENTS]" << std::endl;
  std::cout << std::endl;
  std::cout << "  ARGUMENTS:" << std::endl;
  std::cout << "        -h | --host                Master gate host address [Mandatory]" << std::endl;
  std::cout << "        -p | --port                Master gate port [Mandatory]" << std::endl;
  std::cout << "        -l | --label               Label for enqueued message [Mandatory]" << std::endl;
  std::cout << "        -i | --input               Path to input file [Optional, if not specified stdin will be used]" << std::endl;
  std::cout << "        -r | --related-id          Related message id [Mandatory]" << std::endl;
  std::cout << "        -v                         Verbose mode [Optional]" << std::endl;
}

int 
main(int argc, char **argv) {
  try {
    ::naeem::hottentot::runtime::Logger::Init();
    ::naeem::hottentot::runtime::Logger::GetOut() << "NTNAEEM CO." << std::endl;
    ::naeem::hottentot::runtime::Logger::GetOut() << "COPYRIGHT 2015-2016" << std::endl;
    ::naeem::hottentot::runtime::Logger::GetOut() << "NAEEM GATE MASTER ENQUEUE CLIENT" << std::endl;
    ::naeem::hottentot::runtime::Logger::GetOut() << std::endl;
    ::naeem::hottentot::runtime::Configuration::Init(argc, argv);
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (!::naeem::hottentot::runtime::Configuration::Exists("h", "host")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Master gate host is not specified." << std::endl;
      PrintHelpMessage();
      ::naeem::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
    if (!::naeem::hottentot::runtime::Configuration::HasValue("h", "host")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Master gate host is not specified." << std::endl;
      PrintHelpMessage();
      ::naeem::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
    if (!::naeem::hottentot::runtime::Configuration::Exists("p", "port")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Master gate port is not specified." << std::endl;
      PrintHelpMessage();
      ::naeem::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
    if (!::naeem::hottentot::runtime::Configuration::HasValue("p", "port")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Master gate port is not specified." << std::endl;
      PrintHelpMessage();
      ::naeem::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
    if (!::naeem::hottentot::runtime::Configuration::Exists("l", "label")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Master label is not specified." << std::endl;
      PrintHelpMessage();
      ::naeem::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
    if (!::naeem::hottentot::runtime::Configuration::HasValue("l", "label")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Master label is not specified." << std::endl;
      PrintHelpMessage();
      ::naeem::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
    if (!::naeem::hottentot::runtime::Configuration::Exists("r", "related-id")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Related message id is not specified." << std::endl;
      PrintHelpMessage();
      ::naeem::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
    if (!::naeem::hottentot::runtime::Configuration::HasValue("r", "related-id")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Related message id is not specified." << std::endl;
      PrintHelpMessage();
      ::naeem::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
    if (::naeem::hottentot::runtime::Configuration::Exists("i", "input") &&
        !::naeem::hottentot::runtime::Configuration::HasValue("i", "input")) {
      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Input file is not specified." << std::endl;
      PrintHelpMessage();
      ::naeem::hottentot::runtime::Logger::Shutdown();
      exit(1);
    }
    std::string host = ::naeem::hottentot::runtime::Configuration::AsString("h", "host");
    uint16_t port = ::naeem::hottentot::runtime::Configuration::AsUInt32("p", "port");
    std::string label = ::naeem::hottentot::runtime::Configuration::AsString("l", "label");
    uint64_t relatedId = ::naeem::hottentot::runtime::Configuration::AsUInt64("r", "related-id");
    NAEEM_byte data[1024 * 1024];
    NAEEM_length dataLength = 0;
    if (::naeem::hottentot::runtime::Configuration::Exists("i", "input")) {
      std::string inputFilePath = ::naeem::hottentot::runtime::Configuration::AsString("i", "input");
      NAEEM_os__read_file2((NAEEM_path)inputFilePath.c_str(), (NAEEM_data_ptr)&data, &dataLength);
    } else {
      NAEEM_byte buffer[128];
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
        ::naeem::hottentot::runtime::types::UInt64 id;
        ::naeem::hottentot::runtime::types::UInt64 relatedIdVar(relatedId);
        ::ir::ntnaeem::gate::Message message;
        message.SetId(0);
        message.SetLabel(label);
        message.SetRelId(relatedIdVar);
        message.SetContent(::naeem::hottentot::runtime::types::ByteArray(data, dataLength));
        proxy->Enqueue(message, id);
        ::naeem::hottentot::runtime::Logger::GetOut() << "Message is enqueued with id : " << id << std::endl;
      } else {
        ::naeem::hottentot::runtime::Logger::GetOut() << "ERROR: Master gate is not available." << std::endl;
      }
      //=============================================
      ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
      if (::naeem::hottentot::runtime::Configuration::Verbose()) {
        ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::naeem::hottentot::runtime::Logger::Shutdown();
    } catch (std::exception &e) {
      ::naeem::hottentot::runtime::Logger::GetError() << e.what() << std::endl;
      ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
      if (::naeem::hottentot::runtime::Configuration::Verbose()) {
        ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
      ::naeem::hottentot::runtime::Logger::Shutdown();
      exit(1);
    } catch (...) {
      ::naeem::hottentot::runtime::Logger::GetError() << "Error." << std::endl;
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