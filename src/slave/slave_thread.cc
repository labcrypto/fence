#include <thread>
#include <chrono>
#include <iostream>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include "../common/gate/message.h"
#include "../common/transport/transport_message.h"
#include "../common/transport/transport_service.h"
#include "../common/transport/proxy/transport_service_proxy.h"
#include "../common/transport/proxy/transport_service_proxy_builder.h"

#include "slave_thread.h"
#include "runtime.h"


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
    ::naeem::hottentot::runtime::types::UInt32 slaveId = ::naeem::hottentot::runtime::Configuration::AsUInt32("sid", "slave-id");
    std::string masterHost = ::naeem::hottentot::runtime::Configuration::AsString("m", "master");
    while (true) {
      {
        std::this_thread::sleep_for(std::chrono::seconds(60));
        // Aquiring main lock by creating guard object
        std::lock_guard<std::mutex> guard(Runtime::mainLock_);
        if (::naeem::hottentot::runtime::Configuration::Verbose()) {
          ::naeem::hottentot::runtime::Logger::GetOut() << "VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV" << std::endl;
        }
        // TODO: Disable LAN ethernet
        // TODO: Enable WAN ethernet
        // Create a proxy to Master Gate
        if (::naeem::hottentot::runtime::Configuration::Verbose()) {
          ::naeem::hottentot::runtime::Logger::GetOut() << "Connecting to master gate ..." << std::endl;
        }
        if (::naeem::hottentot::runtime::Configuration::Verbose()) {
          ::naeem::hottentot::runtime::Logger::GetOut() << "Making proxy object ..." << std::endl;
        }
        ::ir::ntnaeem::gate::transport::TransportService *transportProxy = 
          ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Create(masterHost, 8766);
        // TODO: If server is not alive, postbone the operation and release the lock.
        if (::naeem::hottentot::runtime::Configuration::Verbose()) {
          ::naeem::hottentot::runtime::Logger::GetOut() << "Checking if server is available ..." << std::endl;
        }
        bool isServerAlive = dynamic_cast<::ir::ntnaeem::gate::transport::proxy::TransportServiceProxy*>(transportProxy)->IsServerAlive();
        if (isServerAlive) {
          if (::naeem::hottentot::runtime::Configuration::Verbose()) {
            ::naeem::hottentot::runtime::Logger::GetOut() << "Server is up and running ..." << std::endl;
          }
          // Make a list of transport messages
          {
            std::vector< ::ir::ntnaeem::gate::Message*> messages =
              Runtime::outboxQueue_->PopAll();
            if (::naeem::hottentot::runtime::Configuration::Verbose()) {
              ::naeem::hottentot::runtime::Logger::GetOut() << "Number of messages to send: " << messages.size() << std::endl;
            }
            ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::TransportMessage> transportMessages;
            std::map<uint64_t, ::ir::ntnaeem::gate::transport::TransportMessage*> map;
            for (uint32_t i = 0; i < messages.size(); i++) {
              ::ir::ntnaeem::gate::transport::TransportMessage *transportMessage =
                new ::ir::ntnaeem::gate::transport::TransportMessage;
              transportMessage->SetMasterMId(0);
              transportMessage->SetSlaveId(slaveId);
              transportMessage->SetSlaveMId(messages[i]->GetId());
              transportMessage->SetRelMId(0);
              transportMessage->SetRelLabel("");
              transportMessage->SetLabel(messages[i]->GetLabel());
              transportMessage->SetContent(messages[i]->GetContent());
              ::naeem::hottentot::runtime::types::UInt64 masterId;
              transportMessages.Add(transportMessage);
              map.insert(std::pair<uint64_t, ::ir::ntnaeem::gate::transport::TransportMessage*>(transportMessage->GetSlaveMId().GetValue(), transportMessage));
            }
            // Send queued messages to Master Gate
            ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::AcceptReport> acceptReports;
            try {
              if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                ::naeem::hottentot::runtime::Logger::GetOut() << "Sending messages ..." << std::endl;
              }
              transportProxy->AcceptSlaveMassages(transportMessages, acceptReports);
              if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                ::naeem::hottentot::runtime::Logger::GetOut() << "Message sent and added to sent queue." << std::endl;
              }
            } catch (...) {
              ::naeem::hottentot::runtime::Logger::GetError() << "Send error." << std::endl;
            }
            // Analyse accept reports
            for (uint32_t i = 0; i < acceptReports.Size(); i++) {
              ::ir::ntnaeem::gate::transport::AcceptReport *acceptReport = acceptReports.Get(i);
              if (acceptReport->GetStatusCode().GetValue() == 0) {
                map[acceptReport->GetSlaveMId().GetValue()]->SetMasterMId(acceptReport->GetMasterMId());
                Runtime::sentQueue_->Put(map[acceptReport->GetSlaveMId().GetValue()]);
                if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                  ::naeem::hottentot::runtime::Logger::GetOut() << "Message is sent successfully." << std::endl;
                }
              } else {
                Runtime::failedQueue_->Put(map[acceptReport->GetSlaveMId().GetValue()]);
                if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                  ::naeem::hottentot::runtime::Logger::GetOut() << "Message send is failed." << std::endl;
                }
              }
            }
          }
          // Receive queued messages from Master Gate
          {
            if (::naeem::hottentot::runtime::Configuration::Verbose()) {
              ::naeem::hottentot::runtime::Logger::GetOut() << "Retrieving messages from master ..." << std::endl;
            }
            ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::TransportMessage> transportMessages;
            transportProxy->RetrieveSlaveMessages(slaveId, transportMessages);
            ::naeem::hottentot::runtime::types::List< ::naeem::hottentot::runtime::types::UInt64> acks;
            for (uint32_t i = 0; i < transportMessages.Size(); i++) {
              ::ir::ntnaeem::gate::transport::TransportMessage *transportMessage = transportMessages.Get(i);
              ::ir::ntnaeem::gate::Message *message = new ::ir::ntnaeem::gate::Message;
              message->SetId(transportMessage->GetSlaveMId());
              message->SetRelId(transportMessage->GetRelMId());
              message->SetLabel(transportMessage->GetLabel());
              message->SetRelLabel(transportMessage->GetRelLabel());
              message->SetContent(transportMessage->GetContent());
              Runtime::inboxQueue_->Put(message->GetLabel().ToStdString(), message);

              acks.Add(new ::naeem::hottentot::runtime::types::UInt64(transportMessage->GetMasterMId().GetValue()));
            }
            if (::naeem::hottentot::runtime::Configuration::Verbose()) {
              ::naeem::hottentot::runtime::Logger::GetOut() << "Sending acks ..." << std::endl;
            }
            transportProxy->Ack(acks);
            if (::naeem::hottentot::runtime::Configuration::Verbose()) {
              ::naeem::hottentot::runtime::Logger::GetOut() << "Messages are retrieved." << std::endl;
            }
          }
          // Disconnect from Master Gate
          ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Destroy(transportProxy);
          // TODO: Disable WAN ethernet
          // TODO: Enable LAN ethernet
          // Releasing main lock by leaving the scope
          if (::naeem::hottentot::runtime::Configuration::Verbose()) {
            ::naeem::hottentot::runtime::Logger::GetOut() << "Send is complete." << std::endl;
          }
        } else {
          if (::naeem::hottentot::runtime::Configuration::Verbose()) {
            ::naeem::hottentot::runtime::Logger::GetOut() << "Master is not available now. We postbone the send to next try." << std::endl;
          }
          ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Destroy(transportProxy);
        }
      }
    }
  }
}
}
}
}