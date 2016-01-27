#include <thread>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>

#include "../common/gate/message.h"
#include "../common/transport/transport_message.h"

#include "gate_service_impl.h"
#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace slave {
  void
  PutInOutboxQueue(::ir::ntnaeem::gate::Message *message) {
    // TODO: Serialize and persist the message for FT purposes
    std::lock_guard<std::mutex> guard(Runtime::mainLock_);
    std::lock_guard<std::mutex> guard2(Runtime::outboxQueueLock_);
    Runtime::outboxQueue_->Put(message);
    std::cout << "Message is enqueued with id: " << message->GetId().GetValue() << std::endl;
  }
  void
  GateServiceImpl::OnInit() {
    ::naeem::hottentot::runtime::Logger::GetOut() << "Gate Service is initialized." << std::endl;
  }
  void
  GateServiceImpl::OnShutdown() {
    // TODO: Called when service is shutting down.
  }
  void
  GateServiceImpl::EnqueueMessage(::ir::ntnaeem::gate::Message &message, 
                                  ::naeem::hottentot::runtime::types::UInt64 &out) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::EnqueueMessage() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::counterLock_);
      message.SetId(Runtime::messageCounter_);
      out.SetValue(Runtime::messageCounter_);
      Runtime::messageCounter_++;
    }
    // TODO: Select a thread from thread-pool
    ::ir::ntnaeem::gate::Message *newMessage = 
      new ::ir::ntnaeem::gate::Message;
    newMessage->SetId(message.GetId());
    newMessage->SetLabel(message.GetLabel());
    newMessage->SetRelLabel(message.GetRelLabel());
    newMessage->SetRelId(message.GetRelId());
    newMessage->SetContent(message.GetContent());
    std::thread t(PutInOutboxQueue, newMessage);
    t.detach();
  }
  void
  GateServiceImpl::GetMessageStatus(::naeem::hottentot::runtime::types::UInt64 &id, 
                                    ::ir::ntnaeem::gate::MessageStatus &out) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::GetMessageStatus() is called." << std::endl;
    }
    // TODO
  }
  void
  GateServiceImpl::HasMoreMessage(::naeem::hottentot::runtime::types::Utf8String &label, 
                                  ::naeem::hottentot::runtime::types::Boolean &out) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::HasMoreMessage() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      std::lock_guard<std::mutex> guard2(Runtime::inboxQueueLock_);
      out.SetValue(Runtime::inboxQueue_->HasMore(label.ToStdString()));  
    }
  }
  void
  GateServiceImpl::NextMessage(::naeem::hottentot::runtime::types::Utf8String &label, 
                               ::naeem::hottentot::runtime::types::Boolean &messageRetrieved,
                               ::ir::ntnaeem::gate::Message &out) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::NextMessage() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      std::lock_guard<std::mutex> guard2(Runtime::inboxQueueLock_);
      ::ir::ntnaeem::gate::Message *message = Runtime::inboxQueue_->Next(label.ToStdString());
      if (message == NULL ) {
        messageRetrieved.SetValue(false);
      } else {
        messageRetrieved.SetValue(true);
        out.SetId(message->GetId());
        out.SetRelId(message->GetRelId());
        out.SetRelLabel(message->GetRelLabel());
        out.SetLabel(message->GetLabel());
        out.SetContent(message->GetContent());
      }
    }
  }
} // END OF NAMESPACE master
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir