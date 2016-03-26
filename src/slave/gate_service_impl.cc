#include <thread>
#include <chrono>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>

#include <naeem/os.h>

#include <naeem++/conf/config_manager.h>

#include <gate/message.h>

#include <transport/transport_message.h>

#include "gate_service_impl.h"
#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace slave {
  void
  GateServiceImpl::OnInit() {
    // TODO: Load runtime data structures
    workDir_ = ::naeem::conf::ConfigManager::GetValueAsString("slave", "work_dir");
    NAEEM_data temp;
    NAEEM_length tempLength;
    if (NAEEM_os__file_exists((NAEEM_path)workDir_.c_str(), (NAEEM_string)"counter")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"counter",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(Runtime::messageCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::messageCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << "Last Message Counter value is " << Runtime::messageCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Message Counter is set to " << Runtime::messageCounter_ << std::endl;
    }
    ::naeem::hottentot::runtime::Logger::GetOut() << "Gate Service is initialized." << std::endl;
  }
  void
  GateServiceImpl::OnShutdown() {
    // TODO: Persist runtime data structures if not persisted
    {
      std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
      Runtime::termSignal_ = true;
    }
    ::naeem::hottentot::runtime::Logger::GetOut() << "Waiting for slave thread to exit ..." << std::endl;
    while (true) {
      std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
      if (Runtime::slaveThreadTerminated_) {
        ::naeem::hottentot::runtime::Logger::GetOut() << "Slave thread exited." << std::endl;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  void
  GateServiceImpl::Enqueue(
    ::ir::ntnaeem::gate::Message &message, 
    ::naeem::hottentot::runtime::types::UInt64 &out, 
    ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::Enqueue() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::counterLock_);
      message.SetId(Runtime::messageCounter_);
      out.SetValue(Runtime::messageCounter_);
      Runtime::messageCounter_++;
      NAEEM_os__write_to_file (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"counter", 
        (NAEEM_data)&(Runtime::messageCounter_), 
        (NAEEM_length)sizeof(Runtime::messageCounter_)
      );
    }
    ::ir::ntnaeem::gate::Message *newMessage = 
      new ::ir::ntnaeem::gate::Message;
    newMessage->SetId(message.GetId());
    newMessage->SetLabel(message.GetLabel());
    newMessage->SetRelLabel(message.GetRelLabel());
    newMessage->SetRelId(message.GetRelId());
    newMessage->SetContent(message.GetContent());
    std::lock_guard<std::mutex> guard2(Runtime::mainLock_);
    std::lock_guard<std::mutex> guard3(Runtime::outboxQueueLock_);
    try {
      Runtime::outboxQueue_->Put(newMessage);
    } catch (...) {

    }
    ::naeem::hottentot::runtime::Logger::GetOut() << Runtime::GetCurrentStat();
  }
  void
  GateServiceImpl::GetStatus(
    ::naeem::hottentot::runtime::types::UInt64 &id, 
    ::naeem::hottentot::runtime::types::Enum< ::ir::ntnaeem::gate::MessageStatus> &out, 
    ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::GetStatus() is called." << std::endl;
    }
    // TODO
  }
  void
  GateServiceImpl::Discard(
    ::naeem::hottentot::runtime::types::UInt64 &id, 
    ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::Discard() is called." << std::endl;
    }
    // TODO
  }
  void
  GateServiceImpl::HasMore(
    ::naeem::hottentot::runtime::types::Utf8String &label, 
    ::naeem::hottentot::runtime::types::Boolean &out, 
    ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::HasMore() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      std::lock_guard<std::mutex> guard2(Runtime::inboxQueueLock_);
      out.SetValue(Runtime::inboxQueue_->HasMore(label.ToStdString()));  
    }
  }
  void
  GateServiceImpl::PopNext(
    ::naeem::hottentot::runtime::types::Utf8String &label, 
    ::ir::ntnaeem::gate::Message &out, 
    ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::PopNext() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      std::lock_guard<std::mutex> guard2(Runtime::inboxQueueLock_);
      ::ir::ntnaeem::gate::Message *message = Runtime::inboxQueue_->Next(label.ToStdString());
      if (message == NULL ) {
        out.SetId(0);
        out.SetRelId(0);
      } else {
        out.SetId(message->GetId());
        out.SetRelId(message->GetRelId());
        out.SetRelLabel(message->GetRelLabel());
        out.SetLabel(message->GetLabel());
        out.SetContent(message->GetContent());
      }
    }
  }
  void
  GateServiceImpl::Ack(
    ::naeem::hottentot::runtime::types::UInt64 &id, 
    ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::Ack() is called." << std::endl;
    }
    // TODO
  }
} // END OF NAMESPACE master
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir