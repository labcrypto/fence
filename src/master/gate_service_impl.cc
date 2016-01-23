#include <thread>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>

#include "gate_service_impl.h"
#include "runtime.h"

#include "../common/gate/message.h"
#include "../common/transport/transport_message.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace master {
  void
  PutInMainQueue(::ir::ntnaeem::gate::Message &message) {
    // TODO: Serialize and persist the message for FT purposes
    // std::lock_guard<std::mutex> guard(Runtime::mainLock_);
    // Runtime::outboxQueue_->Put(message.GetLabel().ToStdString(), &message);
  }
  void
  GateServiceImpl::OnInit() {
    // Runtime::messageCounter_ = 1000;
    // Runtime::inboxQueue_ = new Queue< ::ir::ntnaeem::gate::Message>;
    // Runtime::outboxQueue_ = new Queue< ::ir::ntnaeem::gate::Message>;
    // Runtime::sentQueue_ = new Queue< ::ir::ntnaeem::gate::transport::TransportMessage>;
    ::naeem::hottentot::runtime::Logger::GetOut() << "Gate Service is initialized." << std::endl;
  }
  void
  GateServiceImpl::OnShutdown() {
    // TODO: Called when service is shutting down.
  }
  void
  GateServiceImpl::EnqueueMessage(::ir::ntnaeem::gate::Message &message, ::naeem::hottentot::runtime::types::UInt32 &out) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::EnqueueMessage() is called." << std::endl;
    }
    // {
    //   std::lock_guard<std::mutex> guard(Runtime::counterLock_);
    //   message.SetId(Runtime::messageCounter_);
    //   out.SetValue(Runtime::messageCounter_);
    //   Runtime::messageCounter_++;
    // }
    // // TODO: Select a thread from thread-pool
    // std::thread t(PutInMainQueue, std::ref(message));
    // t.detach();
  }
  void
  GateServiceImpl::GetMessageStatus(::naeem::hottentot::runtime::types::UInt32 &id, ::ir::ntnaeem::gate::Status &out) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::GetMessageStatus() is called." << std::endl;
    }
    // std::lock_guard<std::mutex> guard(Runtime::mainLock_);
    // TODO
  }
  void
  GateServiceImpl::GetMessages(::naeem::hottentot::runtime::types::Utf8String &label, ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::Message> &out) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::GetMessages() is called." << std::endl;
    }
    // std::lock_guard<std::mutex> guard(Runtime::mainLock_);
    // TODO
  }
} // END OF NAMESPACE master
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir