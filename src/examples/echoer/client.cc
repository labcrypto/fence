#include <thread>
#include <chrono>
#include <iostream>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>
#include <naeem/hottentot/runtime/proxy/proxy.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include "../../common/gate/message_status.h"
#include "../../common/gate/message.h"
#include "../../common/gate/gate_service.h"
#include "../../common/gate/proxy/gate_service_proxy_builder.h"

#include "hotgen/echo_request.h"


int 
main(int argc, char **argv) {
  try {
    ::naeem::hottentot::runtime::Logger::Init();  
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy runtime is initialized." << std::endl;
    }
    ::ir::ntnaeem::gate::GateService *proxy = 
      ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Create("127.0.0.1", 8765);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is created." << std::endl;
    }
    //=============================================
    if (dynamic_cast< ::naeem::hottentot::runtime::proxy::Proxy*>(proxy)->IsServerAlive()) {
      for (uint32_t i = 0; i < 100; i++) {
        ::ir::ntnaeem::gate::examples::echoer::EchoRequest echoRequest;
        echoRequest.SetName("Kamran");
        uint32_t length = 0;
        unsigned char *data = echoRequest.Serialize(&length);
        // ::naeem::hottentot::runtime::Utils::PrintArray("S", data, length);
        ::ir::ntnaeem::gate::Message message;
        message.SetId(0);
        message.SetLabel("echo-request");
        message.SetRelLabel("");
        message.SetRelId(0);
        message.SetContent(::naeem::hottentot::runtime::types::ByteArray(data, length));
        ::naeem::hottentot::runtime::types::UInt64 id;
        proxy->EnqueueMessage(message, id);
        ::naeem::hottentot::runtime::Logger::GetOut() << "Message is sent." << std::endl;
        ::naeem::hottentot::runtime::Logger::GetOut() << "Assigned id: " << id.GetValue() << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(3));
      }
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << "ERROR: Server is not available." << std::endl;
    }
    //=============================================
    ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
    }
    // Delete allocated objects
  } catch (...) {
    ::naeem::hottentot::runtime::Logger::GetOut() << "Error." << std::endl;
    return 1;
  }
  return 0;
}