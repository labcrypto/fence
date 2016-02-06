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
#include "../../common/gate/proxy/gate_service.h"
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
    ::ir::ntnaeem::gate::proxy::GateService *proxy = 
      ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Create(argv[1], 8765);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is created." << std::endl;
    }
    //=============================================
    if (dynamic_cast< ::naeem::hottentot::runtime::proxy::Proxy*>(proxy)->IsServerAlive()) {
      ::naeem::hottentot::runtime::types::Utf8String label("echo-response");
      ::naeem::hottentot::runtime::types::Boolean hasMoreMessage;
      proxy->HasMoreMessage(label, hasMoreMessage);
      if (!hasMoreMessage.GetValue()) {
        std::cout << "No messages." << std::endl;
        return 0;
      }
      while (hasMoreMessage.GetValue()) {
        ::ir::ntnaeem::gate::Message message;
        proxy->NextMessage(label, message);
        if (message.GetId().GetValue() > 0) {
          std::cout << "Message is retrieved with id: " << 
            message.GetId().GetValue() << ", label: '" << 
            message.GetLabel().Serialize(NULL) << "', relId: " << 
            message.GetRelId().GetValue() << /*", content: '" << 
            message.GetContent().Serialize(NULL) << "'" << */ std::endl;
          ::naeem::hottentot::runtime::Utils::PrintArray("CONTENT", message.GetContent().GetValue(), message.GetContent().GetLength());
          // ::ir::ntnaeem::gate::Message replyMessage;
          // replyMessage.SetLabel("echo-response");
          // replyMessage.SetRelLabel(message.GetLabel());
          // replyMessage.SetRelId(message.GetId());
          // ::naeem::hottentot::runtime::types::ByteArray replyContent((unsigned char *)"Hello Kamran!", 13);
          // replyMessage.SetContent(replyContent);
          // ::naeem::hottentot::runtime::types::UInt64 id;
          // proxy->EnqueueMessage(replyMessage, id);
          // std::cout << "Reply is enqueued with id: " << id.GetValue() << std::endl;
        }
        break;
        proxy->HasMoreMessage(label, hasMoreMessage);
        std::this_thread::sleep_for(std::chrono::seconds(2));
      }
      std::cout << "Done." << std::endl;
      /*for (uint32_t i = 0; i < 100; i++) {
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
      }*/
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << "ERROR: Server is not available." << std::endl;
    }
    //=============================================
    ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
    }
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::naeem::hottentot::runtime::Logger::Shutdown(); 
  } catch (...) {
    ::naeem::hottentot::runtime::Logger::GetOut() << "Error." << std::endl;
    return 1;
  }
  return 0;
}