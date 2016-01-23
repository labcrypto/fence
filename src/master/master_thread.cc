#include <thread>
#include <chrono>
#include <iostream>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include "../common/gate/message.h"
#include "../common/transport/transport_message.h"
#include "../common/transport/transport_service.h"
// #include "../common/transport/proxy/transport_service_proxy.h"
// #include "../common/transport/proxy/transport_service_proxy_builder.h"

#include "master_thread.h"
#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace master {
  void
  MasterThread::Start() {
    std::thread t(MasterThread::ThreadBody);
    t.detach();
  }
  void
  MasterThread::ThreadBody() {
    // ::naeem::hottentot::runtime::types::UInt32 slaveId = ::naeem::hottentot::runtime::Configuration::AsUInt32("sid", "slave-id");
    // std::string masterHost = ::naeem::hottentot::runtime::Configuration::AsString("m", "master");
    // while (true) {
    //   {
    //     std::this_thread::sleep_for(std::chrono::seconds(60));
    //     if (::naeem::hottentot::runtime::Configuration::Verbose()) {
    //       ::naeem::hottentot::runtime::Logger::GetOut() << "VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV" << std::endl;
    //     }
    //     if (::naeem::hottentot::runtime::Configuration::Verbose()) {
    //       ::naeem::hottentot::runtime::Logger::GetOut() << "Connecting to master gate ..." << std::endl;
    //     }
    //     // Aquiring main lock by creating guard object
    //     std::lock_guard<std::mutex> guard(Runtime::mainLock_);
    //     // TODO: Disable LAN ethernet
    //     // TODO: Enable WAN ethernet
    //     // Create a proxy to Master Gate
    //     if (::naeem::hottentot::runtime::Configuration::Verbose()) {
    //       ::naeem::hottentot::runtime::Logger::GetOut() << "Making proxy object ..." << std::endl;
    //     }
    //     ::ir::ntnaeem::gate::transport::TransportService *transportProxy = 
    //       ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Create(masterHost, 8766);
    //     // TODO: If server is not alive, postbone the operation and release the lock.
    //     if (::naeem::hottentot::runtime::Configuration::Verbose()) {
    //       ::naeem::hottentot::runtime::Logger::GetOut() << "Checking if server is available ..." << std::endl;
    //     }
    //     bool isServerAlive = dynamic_cast<::ir::ntnaeem::gate::transport::proxy::TransportServiceProxy*>(transportProxy)->IsServerAlive();
    //     if (isServerAlive) {
    //       if (::naeem::hottentot::runtime::Configuration::Verbose()) {
    //         ::naeem::hottentot::runtime::Logger::GetOut() << "Server is up and running ..." << std::endl;
    //       }
    //       // Send queued messages to Master Gate
    //       std::vector< ::ir::ntnaeem::gate::Message*> messages;
    //       Runtime::outboxQueue_->GetMessages(messages);
    //       if (::naeem::hottentot::runtime::Configuration::Verbose()) {
    //         ::naeem::hottentot::runtime::Logger::GetOut() << "Number of messages to send: " << messages.size() << std::endl;
    //       }
    //       for (uint32_t i = 0; i < messages.size(); i++) {
    //         ::ir::ntnaeem::gate::transport::TransportMessage *transportMessage =
    //           new ::ir::ntnaeem::gate::transport::TransportMessage;
    //         transportMessage->SetId(0);
    //         transportMessage->SetSlaveId(1000);
    //         transportMessage->SetSlaveMessageId(messages[i]->GetId());
    //         transportMessage->SetRelId(0);
    //         transportMessage->SetLabel(messages[i]->GetLabel());
    //         transportMessage->SetRelLabel("");
    //         transportMessage->SetContent(messages[i]->GetContent());
    //         ::naeem::hottentot::runtime::types::UInt32 masterId;
    //         try {
    //           if (::naeem::hottentot::runtime::Configuration::Verbose()) {
    //             ::naeem::hottentot::runtime::Logger::GetOut() << "Sending message ..." << std::endl;
    //           }
    //           transportProxy->Send(*transportMessage, masterId);
    //           transportMessage->SetId(masterId);
    //           Runtime::sentQueue_->Put(transportMessage->GetLabel().ToStdString(), transportMessage);
    //           if (::naeem::hottentot::runtime::Configuration::Verbose()) {
    //             ::naeem::hottentot::runtime::Logger::GetOut() << "Message sent and added to sent queue." << std::endl;
    //           }
    //         } catch (...) {
    //           ::naeem::hottentot::runtime::Logger::GetError() << "Send error." << std::endl;
    //         }
    //       }
    //       // Receive queued messages from Master Gate
    //       ::ir::ntnaeem::gate::transport::TransportMessage recTransportMessage;
    //       ::naeem::hottentot::runtime::types::Boolean result;
    //       transportProxy->AnyMessagesLeft(slaveId, result);
    //       while (result.GetValue()) {
    //         transportProxy->Receive(slaveId, recTransportMessage);
    //         // TODO: Store messages in inbox queue
    //         transportProxy->AnyMessagesLeft(slaveId, result);
    //       }
    //       // Disconnect from Master Gate
    //       ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Destroy(transportProxy);
    //       // TODO: Disable WAN ethernet
    //       // TODO: Enable LAN ethernet
    //       // Releasing main lock by leaving the scope
    //       if (::naeem::hottentot::runtime::Configuration::Verbose()) {
    //         ::naeem::hottentot::runtime::Logger::GetOut() << "Send is complete." << std::endl;
    //       }
    //     } else {
    //       if (::naeem::hottentot::runtime::Configuration::Verbose()) {
    //         ::naeem::hottentot::runtime::Logger::GetOut() << "Master is not available now. We postbone the send to next try." << std::endl;
    //       }
    //       ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Destroy(transportProxy);
    //     }
    //   }
    // }
  }
}
}
}
}