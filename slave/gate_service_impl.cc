#include <thread>
#include <chrono>
#include <sstream>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>

#include <naeem/os.h>

#include <naeem++/conf/config_manager.h>
#include <naeem++/date/helper.h>

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
    ackTimeout_ = ::naeem::conf::ConfigManager::GetValueAsUInt32("slave", "ack_timeout");
    if (Runtime::coreInitialized_) {
      return;
    }
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
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/pnat").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/pnat").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/pat").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/pat").c_str());
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
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "Last Message Id Counter value is " << Runtime::messageIdCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "Message Id Counter is set to " << Runtime::messageIdCounter_ << std::endl;
    }
    /*
     * Reading ready for pop total counter file
     */
    if (NAEEM_os__file_exists((NAEEM_path)workDir_.c_str(), (NAEEM_string)"rfptco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"rfptco",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(Runtime::readyForPopTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::readyForPopTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
            "Last Ready For Pop Total Counter value is " << Runtime::readyForPopTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "Ready For Pop Total Counter is set to " << Runtime::readyForPopTotalCounter_ << std::endl;
    }
    /*
     * Reading enqueued total counter file
     */
    if (NAEEM_os__file_exists((NAEEM_path)workDir_.c_str(), (NAEEM_string)"etco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"etco",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(Runtime::enqueuedTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::enqueuedTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "Last Enqueued Total Counter value is " << Runtime::enqueuedTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "Enqueued Total Counter is set to " << Runtime::enqueuedTotalCounter_ << std::endl;
    }
    /*
     * Reading transmitted total counter
     */
    if (NAEEM_os__file_exists((NAEEM_path)workDir_.c_str(), (NAEEM_string)"ttco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"ttco",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(Runtime::transmittedTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::transmittedTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "Last Transmitted Total Counter value is " << Runtime::transmittedTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "Transmitted Total Counter is set to " << Runtime::transmittedTotalCounter_ << std::endl;
    }
    /*
     * Reading transmission failure total counter
     */
    if (NAEEM_os__file_exists((NAEEM_path)workDir_.c_str(), (NAEEM_string)"ftco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"ftco",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(Runtime::transmissionFailureTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::transmissionFailureTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "Last Transmission Failure Total Counter value is " << Runtime::transmissionFailureTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "Transmission Failure Total Counter is set to " << Runtime::transmissionFailureTotalCounter_ << std::endl;
    }
    /*
     * Reading ready for pop total counter file
     */
    if (NAEEM_os__file_exists((NAEEM_path)workDir_.c_str(), (NAEEM_string)"rfptco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"rfptco",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(Runtime::readyForPopTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::readyForPopTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "Last Ready For Pop Total Counter value is " << Runtime::readyForPopTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "Ready For Pop Total Counter is set to " << Runtime::readyForPopTotalCounter_ << std::endl;
    }
    /*
     * Reading popped and acked total counter file
     */
    if (NAEEM_os__file_exists((NAEEM_path)workDir_.c_str(), (NAEEM_string)"patco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"patco",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(Runtime::poppedAndAckedTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::poppedAndAckedTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "Last Popped And Acked Total Counter value is " << Runtime::poppedAndAckedTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "Popped And Acked Total Counter is set to " << Runtime::poppedAndAckedTotalCounter_ << std::endl;
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
    /*
     * Reading ready for pop messages
     */
    NAEEM_os__enum_file_names(
      (NAEEM_path)(workDir_ + "/rfp").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      if (Runtime::states_.find(messageId) != Runtime::states_.end()) {
        if (Runtime::states_[messageId] == 
              (uint16_t)::ir::ntnaeem::gate::kMessageStatus___ReadyForPop) {
          NAEEM_os__read_file_with_path (
            (NAEEM_path)(workDir_ + "/rfp").c_str(), 
            (NAEEM_string)filenames[i],
            &temp, 
            &tempLength
          );
          ::ir::ntnaeem::gate::Message message;
          message.Deserialize(temp, tempLength);
          free(temp);
          if (Runtime::readyForPop_.find(message.GetLabel().ToStdString()) == 
                Runtime::readyForPop_.end()) {
            Runtime::readyForPop_.insert(std::pair<std::string, std::deque<uint64_t>*>
              (message.GetLabel().ToStdString(), new std::deque<uint64_t>()));
          }
          Runtime::readyForPop_[message.GetLabel().ToStdString()]
            ->push_back(message.GetId().GetValue());
        } else {
          // TODO: Message status is not ReadyForPop !
        }
      } else {
        // TODO: Id does not exist in states map.
      }
    }
    NAEEM_os__free_file_names(filenames, filenamesLength);
    /*
     * Reading popped but not acked messages
     */
    NAEEM_os__enum_file_names(
      (NAEEM_path)(workDir_ + "/pna").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      if (Runtime::states_.find(messageId) != Runtime::states_.end()) {
        if (Runtime::states_[messageId] == 
              (uint16_t)::ir::ntnaeem::gate::kMessageStatus___PoppedButNotAcked) {
          NAEEM_os__read_file_with_path (
            (NAEEM_path)(workDir_ + "/pna").c_str(), 
            (NAEEM_string)filenames[i],
            &temp, 
            &tempLength
          );
          uint64_t popTime = 0;
          NAEEM_os__read_file3 (
            (NAEEM_path)(workDir_ + "/pnat/" + filenames[i]).c_str(),
            (NAEEM_data)(&popTime),
            0
          );
          ::ir::ntnaeem::gate::Message message;
          message.Deserialize(temp, tempLength);
          free(temp);
          if (Runtime::poppedButNotAcked_.find(message.GetLabel().ToStdString()) == 
                Runtime::poppedButNotAcked_.end()) {
            Runtime::poppedButNotAcked_.insert(std::pair<std::string, std::map<uint64_t, uint64_t>*>
              (message.GetLabel().ToStdString(), new std::map<uint64_t, uint64_t>()));
          }
          (*(Runtime::poppedButNotAcked_[message.GetLabel().ToStdString()]))[messageId] = popTime;
        } else {
          // TODO: Message status is not PoppedButNotAcked !
        }
      } else {
        // TODO: Id does not exist in states map.
      }
    }
    NAEEM_os__free_file_names(filenames, filenamesLength);
    Runtime::coreInitialized_ = true;
    ::naeem::hottentot::runtime::Logger::GetOut() << 
      "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
        "Gate Service is initialized." << std::endl;
  }
  void
  GateServiceImpl::OnShutdown() {
    {
      std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
      Runtime::termSignal_ = true;
    }
    ::naeem::hottentot::runtime::Logger::GetOut() << 
      "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
        "Waiting for slave thread to exit ..." << std::endl;
    while (true) {
      std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
      if (Runtime::slaveThreadTerminated_) {
        ::naeem::hottentot::runtime::Logger::GetOut() << 
          "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
            "Slave thread exited." << std::endl;
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
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "GateServiceImpl::Enqueue() is called." << std::endl;
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
    std::lock_guard<std::mutex> guard3(Runtime::enqueueLock_);
    try {
      /*
       * Persisting message
       */
      NAEEM_length dataLength = 0;
      NAEEM_data data = message.Serialize(&dataLength);
      std::stringstream ss;
      ss << message.GetId().GetValue();
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
      Runtime::enqueuedTotalCounter_++;
      NAEEM_os__write_to_file (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"etco", 
        (NAEEM_data)&(Runtime::enqueuedTotalCounter_), 
        (NAEEM_length)sizeof(Runtime::enqueuedTotalCounter_)
      );
      Runtime::outbox_.push_back(message.GetId().GetValue());
      Runtime::states_[message.GetId().GetValue()] = status;
      delete [] data;
    } catch (std::exception &e) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          e.what() << std::endl;
      throw std::runtime_error("[" + ::naeem::date::helper::GetCurrentTime() + "]: " + e.what());
    } catch (...) {
      ::naeem::hottentot::runtime::Logger::GetError() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "Error in enqueuing message." << std::endl;
      throw std::runtime_error("[" + ::naeem::date::helper::GetCurrentTime() + "]: Enqueue error.");
    }
  }
  void
  GateServiceImpl::GetStatus(
    ::naeem::hottentot::runtime::types::UInt64 &id, 
    ::naeem::hottentot::runtime::types::UInt16 &out, 
    ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "GateServiceImpl::GetStatus() is called." << std::endl;
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
        throw std::runtime_error("[" + ::naeem::date::helper::GetCurrentTime() + "]: Message id is not found.");
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
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "GateServiceImpl::Discard() is called." << std::endl;
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
        throw std::runtime_error("[" + ::naeem::date::helper::GetCurrentTime() + "]: Message id is not found.");
      }
    }
    if (Runtime::states_[id.GetValue()] == kMessageStatus___Transmitted) {
      throw std::runtime_error("[" + ::naeem::date::helper::GetCurrentTime() +"]: Message is transmitted. Discarding is not possible.");
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
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "GateServiceImpl::HasMore() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      std::lock_guard<std::mutex> guard2(Runtime::readyForPopLock_);
      bool messageIsChosen = false;
      if (Runtime::poppedButNotAcked_.find(label.ToStdString()) != Runtime::poppedButNotAcked_.end()) {
        if (Runtime::poppedButNotAcked_[label.ToStdString()]->size() > 0) {
          for (std::map<uint64_t, uint64_t>::iterator it = Runtime::poppedButNotAcked_[label.ToStdString()]->begin();
               it != Runtime::poppedButNotAcked_[label.ToStdString()]->end();
               it++) {
            uint64_t currentTime = time(NULL);
            if ((currentTime - it->second) > 10) {
              messageIsChosen = true;
              break;
            }
          }
        }
      }
      if (messageIsChosen) {
        out.SetValue(true);
        return;
      }
      if (Runtime::readyForPop_.find(label.ToStdString()) == Runtime::readyForPop_.end()) {
        out.SetValue(false);
      } else {
        out.SetValue(Runtime::readyForPop_[label.ToStdString()]->size() > 0);
      }
    }
  }
  void
  GateServiceImpl::PopNext(
    ::naeem::hottentot::runtime::types::Utf8String &label, 
    ::ir::ntnaeem::gate::Message &out, 
    ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "GateServiceImpl::PopNext() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      std::lock_guard<std::mutex> guard2(Runtime::readyForPopLock_);
      bool messageIsChosen = false;
      uint64_t messageId = 0;
      if (Runtime::poppedButNotAcked_.find(label.ToStdString()) != Runtime::poppedButNotAcked_.end()) {
        if (Runtime::poppedButNotAcked_[label.ToStdString()]->size() > 0) {
          for (std::map<uint64_t, uint64_t>::iterator it = Runtime::poppedButNotAcked_[label.ToStdString()]->begin();
               it != Runtime::poppedButNotAcked_[label.ToStdString()]->end();
               it++) {
            uint64_t currentTime = time(NULL);
            if ((currentTime - it->second) > ackTimeout_) {
              messageId = it->first;
              messageIsChosen = true;
              break;
            }
          }
        }
      }
      if (!messageIsChosen) {
        if (Runtime::readyForPop_.find(label.ToStdString()) == Runtime::readyForPop_.end()) {
          out.SetId(0);
          out.SetRelId(0);
          return;
        }
        if (Runtime::readyForPop_[label.ToStdString()]->size() == 0) {
          out.SetId(0);
          out.SetRelId(0);
          return;
        }
        messageId = Runtime::readyForPop_[label.ToStdString()]->front();
        Runtime::readyForPop_[label.ToStdString()]->pop_front();
      }
      if (messageId == 0) {
        throw std::runtime_error("[" + ::naeem::date::helper::GetCurrentTime() + "]: Internal server error.");
      }
      std::stringstream ss;
      ss << messageId;
      NAEEM_data data;
      NAEEM_length dataLength;
      if (messageIsChosen) {
        NAEEM_os__read_file_with_path (
          (NAEEM_path)(workDir_ + "/pna").c_str(),
          (NAEEM_string)ss.str().c_str(),
          &data,
          &dataLength
        );
      } else {
        NAEEM_os__read_file_with_path (
          (NAEEM_path)(workDir_ + "/r").c_str(),
          (NAEEM_string)ss.str().c_str(),
          &data,
          &dataLength
        );
      }
      ::ir::ntnaeem::gate::Message message;
      message.Deserialize(data, dataLength);
      free(data);
      out.SetId(message.GetId());
      out.SetRelId(message.GetRelId());
      out.SetLabel(message.GetLabel());
      out.SetContent(message.GetContent());
      if (!messageIsChosen) {
        uint16_t status = 
          (uint16_t)::ir::ntnaeem::gate::kMessageStatus___PoppedButNotAcked;
        NAEEM_os__write_to_file (
          (NAEEM_path)(workDir_ + "/s").c_str(), 
          (NAEEM_string)ss.str().c_str(),
          (NAEEM_data)(&status),
          sizeof(status)
        );
        Runtime::states_[messageId] = status;
        NAEEM_os__move_file (
          (NAEEM_path)(workDir_ + "/r").c_str(),
          (NAEEM_string)ss.str().c_str(),
          (NAEEM_path)(workDir_ + "/pna").c_str(),
          (NAEEM_string)ss.str().c_str()
        );
      }
      uint64_t currentTime = time(NULL);
      NAEEM_os__write_to_file (
        (NAEEM_path)(workDir_ + "/pnat").c_str(),
        (NAEEM_string)ss.str().c_str(),
        (NAEEM_data)&currentTime,
        sizeof(currentTime)
      );
      if (Runtime::poppedButNotAcked_.find(label.ToStdString()) == Runtime::poppedButNotAcked_.end()) {
        Runtime::poppedButNotAcked_.insert(
          std::pair<std::string, std::map<uint64_t, uint64_t>*>(
            label.ToStdString(), new std::map<uint64_t, uint64_t>()));
      }
      (*(Runtime::poppedButNotAcked_[label.ToStdString()]))[messageId] = currentTime;
    }
  }
  void
  GateServiceImpl::Ack(
    ::naeem::hottentot::runtime::types::UInt64 &id, 
    ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentTime() << "]: " << 
          "GateServiceImpl::Ack() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      std::lock_guard<std::mutex> guard2(Runtime::readyForPopLock_);
      uint64_t messageId = id.GetValue();
      std::stringstream ss;
      ss << messageId;
      if (NAEEM_os__file_exists (
            (NAEEM_path)(workDir_ + "/pna").c_str(), 
            (NAEEM_string)ss.str().c_str()
          )
      ) {
        NAEEM_data data;
        NAEEM_length dataLength;
        NAEEM_os__read_file_with_path (
          (NAEEM_path)(workDir_ + "/pna").c_str(),
          (NAEEM_string)ss.str().c_str(),
          &data,
          &dataLength
        );
        ::ir::ntnaeem::gate::Message message;
        message.Deserialize(data, dataLength);
        free(data);
        if (Runtime::poppedButNotAcked_.find(message.GetLabel().ToStdString()) 
              != Runtime::poppedButNotAcked_.end()) {
          Runtime::poppedButNotAcked_[message.GetLabel().ToStdString()]->erase(messageId);
        }
        uint16_t status = 
          (uint16_t)::ir::ntnaeem::gate::kMessageStatus___PoppedAndAcked;
        NAEEM_os__write_to_file (
          (NAEEM_path)(workDir_ + "/s").c_str(), 
          (NAEEM_string)ss.str().c_str(),
          (NAEEM_data)(&status),
          sizeof(status)
        );
        Runtime::states_[messageId] = status;
        NAEEM_os__move_file (
          (NAEEM_path)(workDir_ + "/pna").c_str(),
          (NAEEM_string)ss.str().c_str(),
          (NAEEM_path)(workDir_ + "/pa").c_str(),
          (NAEEM_string)ss.str().c_str()
        );
        uint64_t currentTime = time(NULL);
        NAEEM_os__write_to_file (
          (NAEEM_path)(workDir_ + "/pat").c_str(),
          (NAEEM_string)ss.str().c_str(),
          (NAEEM_data)&currentTime,
          sizeof(currentTime)
        );
        Runtime::poppedAndAckedTotalCounter_++;
        NAEEM_os__write_to_file (
          (NAEEM_path)workDir_.c_str(), 
          (NAEEM_string)"patco", 
          (NAEEM_data)&(Runtime::poppedAndAckedTotalCounter_), 
          (NAEEM_length)sizeof(Runtime::poppedAndAckedTotalCounter_)
        );
      } else {
        throw std::runtime_error("[" + ::naeem::date::helper::GetCurrentTime() + "]: Message is not found.");
      }
    }
  }
} // END OF NAMESPACE master
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir