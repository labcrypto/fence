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
    /*
     * Make directories
     */
    if (!NAEEM_os__dir_exists((NAEEM_path)workDir_.c_str())) {
      NAEEM_os__mkdir((NAEEM_path)workDir_.c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/e").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/e").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/t").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/t").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/s").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/s").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/f").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/f").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/r").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/r").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/pa").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/pa").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/pna").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/pna").c_str());
    }
    /*
     * Reading message id counter file
     */
    NAEEM_data temp;
    NAEEM_length tempLength;
    if (NAEEM_os__file_exists((NAEEM_path)workDir_.c_str(), (NAEEM_string)"mco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"mco",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(Runtime::messageIdCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::messageIdCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << "Last Message Id Counter value is " << Runtime::messageIdCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Message Id Counter is set to " << Runtime::messageIdCounter_ << std::endl;
    }
    /*
     * Reading inbox message counter file
     */
    if (NAEEM_os__file_exists((NAEEM_path)workDir_.c_str(), (NAEEM_string)"imco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"imco",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(Runtime::inboxMessageCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::inboxMessageCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << "Last Inbox Message Counter value is " << Runtime::inboxMessageCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Inbox Message Counter is set to " << Runtime::inboxMessageCounter_ << std::endl;
    }
    /*
     * Reading outbox message counter file
     */
    if (NAEEM_os__file_exists((NAEEM_path)workDir_.c_str(), (NAEEM_string)"omco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"omco",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(Runtime::outboxMessageCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::outboxMessageCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << "Last Outbox Message Counter value is " << Runtime::outboxMessageCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Outbox Message Counter is set to " << Runtime::outboxMessageCounter_ << std::endl;
    }
    /*
     * Reading transmitted counter
     */
    if (NAEEM_os__file_exists((NAEEM_path)workDir_.c_str(), (NAEEM_string)"tco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"tco",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(Runtime::transmittedCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::transmittedCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << "Last Transmitted Counter value is " << Runtime::transmittedCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Transmitted Counter is set to " << Runtime::transmittedCounter_ << std::endl;
    }
    /*
     * Reading transmission failure counter
     */
    if (NAEEM_os__file_exists((NAEEM_path)workDir_.c_str(), (NAEEM_string)"fco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"fco",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(Runtime::transmissionFailureCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::transmissionFailureCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << "Last Transmission Failure Counter value is " << Runtime::transmissionFailureCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Transmission Failure Counter is set to " << Runtime::transmissionFailureCounter_ << std::endl;
    }
    /*
     * Reading states
     */
    NAEEM_string_ptr filenames;
    NAEEM_length filenamesLength;
    NAEEM_os__enum_file_names(
      (NAEEM_path)(workDir_ + "/s").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint16_t status = 0;
      NAEEM_os__read_file3 (
        (NAEEM_path)(workDir_ + "/s/" + filenames[i]).c_str(),
        (NAEEM_data)&status,
        0
      );
      Runtime::states_.insert(
        std::pair<uint64_t, ::ir::ntnaeem::gate::MessageStatus>(
          atoll(filenames[i]), (::ir::ntnaeem::gate::MessageStatus)status));
    }
    NAEEM_os__free_file_names(filenames, filenamesLength);
    /*
     * Reading enqueued messages
     */
    NAEEM_os__enum_file_names(
      (NAEEM_path)(workDir_ + "/e").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      if (Runtime::states_.find(messageId) != Runtime::states_.end()) {
        if (Runtime::states_[messageId] == ::ir::ntnaeem::gate::kMessageStatus___EnqueuedForTransmission) {
          Runtime::outbox_.push_back(messageId);
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
      std::lock_guard<std::mutex> guard(Runtime::messageIdCounterLock_);
      message.SetId(Runtime::messageIdCounter_);
      out.SetValue(Runtime::messageIdCounter_);
      Runtime::messageIdCounter_++;
      NAEEM_os__write_to_file (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"mco", 
        (NAEEM_data)&(Runtime::messageIdCounter_), 
        (NAEEM_length)sizeof(Runtime::messageIdCounter_)
      );
    }
    std::lock_guard<std::mutex> guard2(Runtime::mainLock_);
    std::lock_guard<std::mutex> guard3(Runtime::outboxLock_);
    try {
      /*
       * Persisting message
       */
      std::stringstream ss;
      ss << message.GetId().GetValue();
      NAEEM_length dataLength = 0;
      NAEEM_data data = message.Serialize(&dataLength);
      NAEEM_os__write_to_file (
        (NAEEM_path)(workDir_ + "/e").c_str(), 
        (NAEEM_string)ss.str().c_str(),
        data,
        dataLength
      );
      uint16_t status = (uint16_t)kMessageStatus___EnqueuedForTransmission;
      NAEEM_os__write_to_file (
        (NAEEM_path)(workDir_ + "/s").c_str(), 
        (NAEEM_string)ss.str().c_str(),
        (NAEEM_data)(&status),
        sizeof(status)
      );
      Runtime::outboxMessageCounter_++;
      NAEEM_os__write_to_file (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"omco", 
        (NAEEM_data)&(Runtime::outboxMessageCounter_), 
        (NAEEM_length)sizeof(Runtime::outboxMessageCounter_)
      );
      Runtime::outbox_.push_back(message.GetId().GetValue());
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
    std::lock_guard<std::mutex> guard2(Runtime::mainLock_);
    if (Runtime::states_.find(id.GetValue()) == Runtime::states_.end()) {
      std::stringstream filePath;
      filePath << id.GetValue();
      if (NAEEM_os__file_exists(
            (NAEEM_path)(workDir_ + "/s").c_str(), 
            (NAEEM_string)filePath.str().c_str()
          )
        ) {
        uint16_t status = 0;
        NAEEM_os__read_file3 (
          (NAEEM_path)(workDir_ + "/s/" + filePath.str()).c_str(),
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
    std::lock_guard<std::mutex> guard2(Runtime::mainLock_);
    if (Runtime::states_.find(id.GetValue()) == Runtime::states_.end()) {
      std::stringstream filePath;
      filePath << id.GetValue();
      if (NAEEM_os__file_exists(
            (NAEEM_path)(workDir_ + "/s").c_str(), 
            (NAEEM_string)filePath.str().c_str()
          )
        ) {
        uint16_t status = 0;
        NAEEM_os__read_file3 (
          (NAEEM_path)(workDir_ + "/s/" + filePath.str()).c_str(),
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
    if (Runtime::states_[id.GetValue()] == kMessageStatus___Transmitted) {
      throw std::runtime_error("Message is transmitted. Discarding is not possible.");
    } else {
      // TODO: Discard the message
    }
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
      std::lock_guard<std::mutex> guard2(Runtime::inboxLock_);
      // out.SetValue(Runtime::inboxQueue_->HasMore(label.ToStdString()));  
      out.SetValue(false);
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
      std::lock_guard<std::mutex> guard2(Runtime::inboxLock_);
      /*::ir::ntnaeem::gate::Message *message = Runtime::inboxQueue_->Next(label.ToStdString());
      if (message == NULL ) {
        out.SetId(0);
        out.SetRelId(0);
      } else {
        out.SetId(message->GetId());
        out.SetRelId(message->GetRelId());
        out.SetRelLabel(message->GetRelLabel());
        out.SetLabel(message->GetLabel());
        out.SetContent(message->GetContent());
      }*/
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