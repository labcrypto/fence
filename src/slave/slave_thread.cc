#include <thread>
#include <chrono>
#include <iostream>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include "../common/runtime.h"
#include "../common/gate/message.h"
#include "../common/transport/transport_message.h"
#include "../common/transport/transport_service.h"
#include "../common/transport/proxy/transport_service_proxy_builder.h"

#include "slave_thread.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace slave {
  void
  SlaveThread::Start() {
    std::thread t(SlaveThread::ThreadBody);
    t.detach();
  }
  void
  SlaveThread::ThreadBody() {
    while (true) {
      {
        std::this_thread::sleep_for(std::chrono::seconds(60));
        if (::naeem::hottentot::runtime::Configuration::Verbose()) {
          ::naeem::hottentot::runtime::Logger::GetOut() << "Sending to master gate ..." << std::endl;
        }
        // Aquiring main lock by creating guard object
        std::lock_guard<std::mutex> guard(Runtime::mainLock_);
        // TODO: Disable LAN ethernet
        // TODO: Enable WAN ethernet
        // Create a proxy to Master Gate
        ::ir::ntnaeem::gate::transport::TransportService *transportProxy = 
          ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Create("master-gate", 8766);
        // Send queued messages to Master Gate
        std::vector< ::ir::ntnaeem::gate::Message*> messages;
        Runtime::mainQueue_->GetMessages(messages);
        if (::naeem::hottentot::runtime::Configuration::Verbose()) {
          ::naeem::hottentot::runtime::Logger::GetOut() << "Number of messages to send: " << messages.size() << std::endl;
        }
        for (uint32_t i = 0; i < messages.size(); i++) {
          ::ir::ntnaeem::gate::transport::TransportMessage transportMessage;
          transportMessage.SetId(0);
          transportMessage.SetSlaveId(messages[i]->GetId());
          transportMessage.SetRelId(0);
          transportMessage.SetLabel(messages[i]->GetLabel());
          transportMessage.SetRelLabel("");
          transportMessage.SetContent(messages[i]->GetContent());
          ::naeem::hottentot::runtime::types::UInt32 masterId;
          try {
            if (::naeem::hottentot::runtime::Configuration::Verbose()) {
              ::naeem::hottentot::runtime::Logger::GetOut() << "Sending mesage ..." << std::endl;
            }
            transportProxy->Send(transportMessage, masterId);
            Runtime::sentQueue_->Put(messages[i]->GetLabel().ToStdString(), messages[i]);
            if (::naeem::hottentot::runtime::Configuration::Verbose()) {
              ::naeem::hottentot::runtime::Logger::GetOut() << "Message sent and added to sent queue." << std::endl;
            }
          } catch (...) {
            ::naeem::hottentot::runtime::Logger::GetError() << "Send error." << std::endl;
          }
        }
        // TODO: Receive queued messages from Master Gate
        // Disconnect from Master Gate
        ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Destroy(transportProxy);
        // TODO: Disable WAN ethernet
        // TODO: Enable LAN ethernet
        // Releasing main lock by leaving the scope
        if (::naeem::hottentot::runtime::Configuration::Verbose()) {
          ::naeem::hottentot::runtime::Logger::GetOut() << "Send is complete." << std::endl;
        }
      }
    }
  }
}
}
}
}