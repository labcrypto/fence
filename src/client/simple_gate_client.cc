#include <naeem++/conf/config_manager.h>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>
#include <naeem/hottentot/runtime/proxy/proxy.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include <gate/message_status.h>
#include <gate/message.h>
#include <gate/proxy/gate_service.h>
#include <gate/proxy/gate_service_proxy_builder.h>

#include <naeem/gate/client/simple_gate_client.h>


namespace naeem {
namespace gate {
namespace client {
  void 
  SimpleGateClient::Init(int argc, char **argv) {
    ::naeem::hottentot::runtime::Logger::Init();  
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy runtime is initialized." << std::endl;
    }
  }
  void 
  SimpleGateClient::Shutdown() {
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::naeem::hottentot::runtime::Logger::Shutdown(); 
  }
  uint64_t 
  SimpleGateClient::SubmitMessage(std::string label, unsigned char *data, uint32_t length) {
    ::ir::ntnaeem::gate::proxy::GateService *proxy = 
      ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Create(host_, port_);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is created." << std::endl;
    }
    try {
      if (dynamic_cast< ::naeem::hottentot::runtime::proxy::Proxy*>(proxy)->IsServerAlive()) {
        ::ir::ntnaeem::gate::Message message;
        message.SetId(0);
        message.SetLabel(label);
        message.SetRelLabel("");
        message.SetRelId(0);
        message.SetContent(::naeem::hottentot::runtime::types::ByteArray(data, length));
        ::naeem::hottentot::runtime::types::UInt64 id;
        proxy->EnqueueMessage(message, id);
        uint64_t assignedId = id.GetValue();
        ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
        if (::naeem::hottentot::runtime::Configuration::Verbose()) {
          ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
        }
        return assignedId;
      }
    } catch (std::exception &e) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "ERROR: " << e.what() << std::endl;
      ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
      if (::naeem::hottentot::runtime::Configuration::Verbose()) {
        ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
      }
      throw e;
    }
    
  }
}
}
}