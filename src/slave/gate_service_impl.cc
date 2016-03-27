#include <thread>
#include <chrono>
#include <sstream>

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
    workDir_ = ::naeem::conf::ConfigManager::GetValueAsString("slave", "work_dir");
    // Make directories
    if (!NAEEM_os__dir_exists((NAEEM_path)workDir_.c_str())) {
      NAEEM_os__mkdir((NAEEM_path)workDir_.c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/outbox").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/outbox").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/sent").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/sent").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/status").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/status").c_str());
    }
    // Reading message counter file
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
    // Reading states
    NAEEM_string_ptr filenames;
    NAEEM_length filenamesLength;
    NAEEM_os__enum_file_names(
      (NAEEM_path)(workDir_ + "/status").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint16_t status = 0;
      NAEEM_os__read_file3 (
        (NAEEM_path)(workDir_ + "/status/" + filenames[i]).c_str(),
        (NAEEM_data)&status,
        0
      );
      Runtime::states_.insert(
        std::pair<uint64_t, ::ir::ntnaeem::gate::MessageStatus>(
          atoll(filenames[i]), (::ir::ntnaeem::gate::MessageStatus)status));
    }
    NAEEM_os__free_file_names(filenames, filenamesLength);
    // Reading outbox messages
    NAEEM_os__enum_file_names(
      (NAEEM_path)(workDir_ + "/outbox").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      NAEEM_data data;
      NAEEM_length dataLength;
      NAEEM_os__read_file2 (
        (NAEEM_path)(workDir_ + "/outbox/" + filenames[i]).c_str(),
        &data,
        &dataLength
      );
      ::ir::ntnaeem::gate::Message *newMessage = 
        new ::ir::ntnaeem::gate::Message;
      newMessage->Deserialize(data, dataLength);
      free(data);
      if (Runtime::states_.find(newMessage->GetId().GetValue()) != Runtime::states_.end()) {
        if (Runtime::states_[newMessage->GetId().GetValue()] == 
              ::ir::ntnaeem::gate::kMessageStatus___EnqueuedForTransmission) {
          Runtime::outboxQueue_->Put(newMessage);
        } else {
          // TODO: Message status is not EnqueuedForTransmission !
        }
      } else {
        // TODO: Id does not exist in states map.
      }
    }
    NAEEM_os__free_file_names(filenames, filenamesLength);
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
      // Persisting message
      std::stringstream ss;
      ss << newMessage->GetId().GetValue();
      NAEEM_length dataLength = 0;
      NAEEM_data data = newMessage->Serialize(&dataLength);
      NAEEM_os__write_to_file (
        (NAEEM_path)(workDir_ + "/outbox").c_str(), 
        (NAEEM_string)ss.str().c_str(),
        data,
        dataLength
      );
      uint16_t status = (uint16_t)kMessageStatus___EnqueuedForTransmission;
      NAEEM_os__write_to_file (
        (NAEEM_path)(workDir_ + "/status").c_str(), 
        (NAEEM_string)ss.str().c_str(),
        (NAEEM_data)(&status),
        sizeof(status)
      );
      Runtime::outboxQueue_->Put(newMessage);
      delete [] data;
    } catch (std::exception &e) {
      ::naeem::hottentot::runtime::Logger::GetError() << e.what() << std::endl;
      throw std::runtime_error(e.what());
    } catch (...) {
      ::naeem::hottentot::runtime::Logger::GetError() << "Error in enqueuing message." << std::endl;
      throw std::runtime_error("Enqueue error.");
    }
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
    if (Runtime::states_.find(id.GetValue()) == Runtime::states_.end()) {
      std::stringstream filePath;
      filePath << id.GetValue();
      if (NAEEM_os__file_exists(
            (NAEEM_path)(workDir_ + "/status").c_str(), 
            (NAEEM_string)filePath.str().c_str()
          )
        ) {
        uint16_t status = 0;
        NAEEM_os__read_file3 (
          (NAEEM_path)(workDir_ + "/status/" + filePath.str()).c_str(),
          (NAEEM_data)&status,
          0
        );
        Runtime::states_.insert(
          std::pair<uint64_t, ::ir::ntnaeem::gate::MessageStatus>(
            id.GetValue(), (::ir::ntnaeem::gate::MessageStatus)status));
      } else {
        throw std::runtime_error("Message id is not found.");
      }
    }
    out.SetValue(Runtime::states_[id.GetValue()]);
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