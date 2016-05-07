#include <thread>
#include <chrono>
#include <sstream>

#include <org/labcrypto/abettor/fs.h>

#include <org/labcrypto/abettor++/conf/config_manager.h>
#include <org/labcrypto/abettor++/date/helper.h>

#include <org/labcrypto/hottentot/runtime/configuration.h>
#include <org/labcrypto/hottentot/runtime/logger.h>
#include <org/labcrypto/hottentot/runtime/utils.h>

#include <fence/message.h>
#include <transport/transport_message.h>

#include "fence_service_impl.h"
#include "runtime.h"


namespace org {
namespace labcrypto {
namespace fence {
namespace slave {
  void
  FenceServiceImpl::OnInit() {
    workDir_ = ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("slave", "work_dir");
    ackTimeout_ = ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("slave", "ack_timeout");
    if (Runtime::coreInitialized_) {
      return;
    }
    /*
     * Make directories
     */
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/e").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/e").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/t").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/t").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/f").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/f").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/r").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/r").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pa").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pa").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pna").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pna").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pnat").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pnat").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pat").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pat").c_str());
    }
    /*
     * Reading message id counter file
     */
    ORG_LABCRYPTO_ABETTOR_data temp;
    ORG_LABCRYPTO_ABETTOR_length tempLength;
    if (ORG_LABCRYPTO_ABETTOR__fs__file_exists((ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), (ORG_LABCRYPTO_ABETTOR_string)"mco")) {
      ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
        (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"mco",
        &temp, 
        &tempLength
      );
      ORG_LABCRYPTO_ABETTOR_data ptr = (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::messageIdCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::messageIdCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Last Message Id Counter value is " << Runtime::messageIdCounter_ << std::endl;
      free(temp);
    } else {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Message Id Counter is set to " << Runtime::messageIdCounter_ << std::endl;
    }
    /*
     * Reading ready for pop total counter file
     */
    if (ORG_LABCRYPTO_ABETTOR__fs__file_exists((ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), (ORG_LABCRYPTO_ABETTOR_string)"rfptco")) {
      ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
        (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"rfptco",
        &temp, 
        &tempLength
      );
      ORG_LABCRYPTO_ABETTOR_data ptr = (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::readyForPopTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::readyForPopTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
            "Last Ready For Pop Total Counter value is " << Runtime::readyForPopTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Ready For Pop Total Counter is set to " << Runtime::readyForPopTotalCounter_ << std::endl;
    }
    /*
     * Reading enqueued total counter file
     */
    if (ORG_LABCRYPTO_ABETTOR__fs__file_exists((ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), (ORG_LABCRYPTO_ABETTOR_string)"etco")) {
      ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
        (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"etco",
        &temp, 
        &tempLength
      );
      ORG_LABCRYPTO_ABETTOR_data ptr = (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::enqueuedTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::enqueuedTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Last Enqueued Total Counter value is " << Runtime::enqueuedTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Enqueued Total Counter is set to " << Runtime::enqueuedTotalCounter_ << std::endl;
    }
    /*
     * Reading transmitted total counter
     */
    if (ORG_LABCRYPTO_ABETTOR__fs__file_exists((ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), (ORG_LABCRYPTO_ABETTOR_string)"ttco")) {
      ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
        (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"ttco",
        &temp, 
        &tempLength
      );
      ORG_LABCRYPTO_ABETTOR_data ptr = (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::transmittedTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::transmittedTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Last Transmitted Total Counter value is " << Runtime::transmittedTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Transmitted Total Counter is set to " << Runtime::transmittedTotalCounter_ << std::endl;
    }
    /*
     * Reading transmission failure total counter
     */
    if (ORG_LABCRYPTO_ABETTOR__fs__file_exists((ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), (ORG_LABCRYPTO_ABETTOR_string)"ftco")) {
      ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
        (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"ftco",
        &temp, 
        &tempLength
      );
      ORG_LABCRYPTO_ABETTOR_data ptr = (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::transmissionFailureTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::transmissionFailureTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Last Transmission Failure Total Counter value is " << Runtime::transmissionFailureTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Transmission Failure Total Counter is set to " << Runtime::transmissionFailureTotalCounter_ << std::endl;
    }
    /*
     * Reading ready for pop total counter file
     */
    if (ORG_LABCRYPTO_ABETTOR__fs__file_exists((ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), (ORG_LABCRYPTO_ABETTOR_string)"rfptco")) {
      ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
        (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"rfptco",
        &temp, 
        &tempLength
      );
      ORG_LABCRYPTO_ABETTOR_data ptr = (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::readyForPopTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::readyForPopTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Last Ready For Pop Total Counter value is " << Runtime::readyForPopTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Ready For Pop Total Counter is set to " << Runtime::readyForPopTotalCounter_ << std::endl;
    }
    /*
     * Reading popped and acked total counter file
     */
    if (ORG_LABCRYPTO_ABETTOR__fs__file_exists((ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), (ORG_LABCRYPTO_ABETTOR_string)"patco")) {
      ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
        (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"patco",
        &temp, 
        &tempLength
      );
      ORG_LABCRYPTO_ABETTOR_data ptr = (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::poppedAndAckedTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::poppedAndAckedTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Last Popped And Acked Total Counter value is " << Runtime::poppedAndAckedTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Popped And Acked Total Counter is set to " << Runtime::poppedAndAckedTotalCounter_ << std::endl;
    }
    /*
     * Reading states
     */
    ORG_LABCRYPTO_ABETTOR_string_ptr filenames;
    ORG_LABCRYPTO_ABETTOR_length filenamesLength;
    ORG_LABCRYPTO_ABETTOR__fs__enum_file_names (
      (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint16_t status = 0;
      ORG_LABCRYPTO_ABETTOR__fs__read_file_into_buffer (
        (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s/" + filenames[i]).c_str(),
        (ORG_LABCRYPTO_ABETTOR_data)&status,
        0
      );
      Runtime::states_.insert(
        std::pair<uint64_t, ::org::labcrypto::fence::MessageStatus>(
          atoll(filenames[i]), (::org::labcrypto::fence::MessageStatus)status));
    }
    ORG_LABCRYPTO_ABETTOR__fs__free_file_names(filenames, filenamesLength);
    /*
     * Reading enqueued messages
     */
    ORG_LABCRYPTO_ABETTOR__fs__enum_file_names (
      (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/e").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      if (Runtime::states_.find(messageId) != Runtime::states_.end()) {
        if (Runtime::states_[messageId] == ::org::labcrypto::fence::kMessageStatus___EnqueuedForTransmission) {
          Runtime::outbox_.push_back(messageId);
        } else {
          // TODO: Message status is not EnqueuedForTransmission !
        }
      } else {
        // TODO: Id does not exist in states map.
      }
    }
    ORG_LABCRYPTO_ABETTOR__fs__free_file_names(filenames, filenamesLength);
    /*
     * Reading ready for pop messages
     */
    ORG_LABCRYPTO_ABETTOR__fs__enum_file_names (
      (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rfp").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      if (Runtime::states_.find(messageId) != Runtime::states_.end()) {
        if (Runtime::states_[messageId] == 
              (uint16_t)::org::labcrypto::fence::kMessageStatus___ReadyForPop) {
          ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
            (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rfp").c_str(), 
            (ORG_LABCRYPTO_ABETTOR_string)filenames[i],
            &temp, 
            &tempLength
          );
          ::org::labcrypto::fence::Message message;
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
    ORG_LABCRYPTO_ABETTOR__fs__free_file_names(filenames, filenamesLength);
    /*
     * Reading popped but not acked messages
     */
    ORG_LABCRYPTO_ABETTOR__fs__enum_file_names (
      (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pna").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      if (Runtime::states_.find(messageId) != Runtime::states_.end()) {
        if (Runtime::states_[messageId] == 
              (uint16_t)::org::labcrypto::fence::kMessageStatus___PoppedButNotAcked) {
          ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
            (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pna").c_str(), 
            (ORG_LABCRYPTO_ABETTOR_string)filenames[i],
            &temp, 
            &tempLength
          );
          uint64_t popTime = 0;
          ORG_LABCRYPTO_ABETTOR__fs__read_file_into_buffer (
            (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pnat/" + filenames[i]).c_str(),
            (ORG_LABCRYPTO_ABETTOR_data)(&popTime),
            0
          );
          ::org::labcrypto::fence::Message message;
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
    ORG_LABCRYPTO_ABETTOR__fs__free_file_names(filenames, filenamesLength);
    Runtime::coreInitialized_ = true;
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
        "Fence Service is initialized." << std::endl;
  }
  void
  FenceServiceImpl::OnShutdown() {
    {
      std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
      Runtime::termSignal_ = true;
    }
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
        "Waiting for slave thread to exit ..." << std::endl;
    while (true) {
      std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
      if (Runtime::slaveThreadTerminated_) {
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
          "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
            "Slave thread exited." << std::endl;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  void
  FenceServiceImpl::Enqueue(
    ::org::labcrypto::fence::Message &message, 
    ::org::labcrypto::hottentot::UInt64 &out, 
    ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "FenceServiceImpl::Enqueue() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::messageIdCounterLock_);
      message.SetId(Runtime::messageIdCounter_);
      out.SetValue(Runtime::messageIdCounter_);
      Runtime::messageIdCounter_++;
      ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
        (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"mco", 
        (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::messageIdCounter_), 
        (ORG_LABCRYPTO_ABETTOR_length)sizeof(Runtime::messageIdCounter_)
      );
    }
    std::lock_guard<std::mutex> guard2(Runtime::mainLock_);
    std::lock_guard<std::mutex> guard3(Runtime::enqueueLock_);
    try {
      /*
       * Persisting message
       */
      ORG_LABCRYPTO_ABETTOR_length dataLength = 0;
      ORG_LABCRYPTO_ABETTOR_data data = message.Serialize(&dataLength);
      std::stringstream ss;
      ss << message.GetId().GetValue();
      ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
        (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/e").c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
        data,
        dataLength
      );
      uint16_t status = (uint16_t)kMessageStatus___EnqueuedForTransmission;
      ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
        (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s").c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
        (ORG_LABCRYPTO_ABETTOR_data)(&status),
        sizeof(status)
      );
      Runtime::enqueuedTotalCounter_++;
      ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
        (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"etco", 
        (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::enqueuedTotalCounter_), 
        (ORG_LABCRYPTO_ABETTOR_length)sizeof(Runtime::enqueuedTotalCounter_)
      );
      Runtime::outbox_.push_back(message.GetId().GetValue());
      Runtime::states_[message.GetId().GetValue()] = status;
      delete [] data;
    } catch (std::exception &e) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          e.what() << std::endl;
      throw std::runtime_error("[" + ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() + "]: " + e.what());
    } catch (...) {
      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Error in enqueuing message." << std::endl;
      throw std::runtime_error("[" + ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() + "]: Enqueue error.");
    }
  }
  void
  FenceServiceImpl::GetStatus(
    ::org::labcrypto::hottentot::UInt64 &id, 
    ::org::labcrypto::hottentot::UInt16 &out, 
    ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "FenceServiceImpl::GetStatus() is called." << std::endl;
    }
    std::lock_guard<std::mutex> guard2(Runtime::mainLock_);
    if (Runtime::states_.find(id.GetValue()) == Runtime::states_.end()) {
      std::stringstream filePath;
      filePath << id.GetValue();
      if (ORG_LABCRYPTO_ABETTOR__fs__file_exists(
            (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s").c_str(), 
            (ORG_LABCRYPTO_ABETTOR_string)filePath.str().c_str()
          )
        ) {
        uint16_t status = 0;
        ORG_LABCRYPTO_ABETTOR__fs__read_file_into_buffer (
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s/" + filePath.str()).c_str(),
          (ORG_LABCRYPTO_ABETTOR_data)&status,
          0
        );
        Runtime::states_.insert(
          std::pair<uint64_t, ::org::labcrypto::fence::MessageStatus>(
            id.GetValue(), (::org::labcrypto::fence::MessageStatus)status));
      } else {
        throw std::runtime_error("[" + ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() + "]: Message id is not found.");
      }
    }
    out.SetValue(Runtime::states_[id.GetValue()]);
  }
  void
  FenceServiceImpl::Discard(
    ::org::labcrypto::hottentot::UInt64 &id, 
    ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "FenceServiceImpl::Discard() is called." << std::endl;
    }
    std::lock_guard<std::mutex> guard2(Runtime::mainLock_);
    if (Runtime::states_.find(id.GetValue()) == Runtime::states_.end()) {
      std::stringstream filePath;
      filePath << id.GetValue();
      if (ORG_LABCRYPTO_ABETTOR__fs__file_exists (
            (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s").c_str(), 
            (ORG_LABCRYPTO_ABETTOR_string)filePath.str().c_str()
          )
        ) {
        uint16_t status = 0;
        ORG_LABCRYPTO_ABETTOR__fs__read_file_into_buffer (
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s/" + filePath.str()).c_str(),
          (ORG_LABCRYPTO_ABETTOR_data)&status,
          0
        );
        Runtime::states_.insert(
          std::pair<uint64_t, ::org::labcrypto::fence::MessageStatus>(
            id.GetValue(), (::org::labcrypto::fence::MessageStatus)status));
      } else {
        throw std::runtime_error("[" + ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() + "]: Message id is not found.");
      }
    }
    if (Runtime::states_[id.GetValue()] == kMessageStatus___Transmitted) {
      throw std::runtime_error("[" + ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() +"]: Message is transmitted. Discarding is not possible.");
    } else {
      // TODO: Discard the message
    }
  }
  void
  FenceServiceImpl::HasMore(
    ::org::labcrypto::hottentot::Utf8String &label, 
    ::org::labcrypto::hottentot::Boolean &out, 
    ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "FenceServiceImpl::HasMore() is called." << std::endl;
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
  FenceServiceImpl::PopNext(
    ::org::labcrypto::hottentot::Utf8String &label, 
    ::org::labcrypto::fence::Message &out, 
    ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "FenceServiceImpl::PopNext() is called." << std::endl;
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
        throw std::runtime_error("[" + ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() + "]: Internal server error.");
      }
      std::stringstream ss;
      ss << messageId;
      ORG_LABCRYPTO_ABETTOR_data data;
      ORG_LABCRYPTO_ABETTOR_length dataLength;
      if (messageIsChosen) {
        ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pna").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          &data,
          &dataLength
        );
      } else {
        ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/r").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          &data,
          &dataLength
        );
      }
      ::org::labcrypto::fence::Message message;
      message.Deserialize(data, dataLength);
      free(data);
      out.SetId(message.GetId());
      out.SetRelId(message.GetRelId());
      out.SetLabel(message.GetLabel());
      out.SetContent(message.GetContent());
      if (!messageIsChosen) {
        uint16_t status = 
          (uint16_t)::org::labcrypto::fence::kMessageStatus___PoppedButNotAcked;
        ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s").c_str(), 
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          (ORG_LABCRYPTO_ABETTOR_data)(&status),
          sizeof(status)
        );
        Runtime::states_[messageId] = status;
        ORG_LABCRYPTO_ABETTOR__fs__move_file (
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/r").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pna").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
        );
      }
      uint64_t currentTime = time(NULL);
      ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
        (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pnat").c_str(),
        (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
        (ORG_LABCRYPTO_ABETTOR_data)&currentTime,
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
  FenceServiceImpl::Ack(
    ::org::labcrypto::hottentot::UInt64 &id, 
    ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "FenceServiceImpl::Ack() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      std::lock_guard<std::mutex> guard2(Runtime::readyForPopLock_);
      uint64_t messageId = id.GetValue();
      std::stringstream ss;
      ss << messageId;
      if (ORG_LABCRYPTO_ABETTOR__fs__file_exists (
            (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pna").c_str(), 
            (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
          )
      ) {
        ORG_LABCRYPTO_ABETTOR_data data;
        ORG_LABCRYPTO_ABETTOR_length dataLength;
        ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pna").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          &data,
          &dataLength
        );
        ::org::labcrypto::fence::Message message;
        message.Deserialize(data, dataLength);
        free(data);
        if (Runtime::poppedButNotAcked_.find(message.GetLabel().ToStdString()) 
              != Runtime::poppedButNotAcked_.end()) {
          Runtime::poppedButNotAcked_[message.GetLabel().ToStdString()]->erase(messageId);
        }
        uint16_t status = 
          (uint16_t)::org::labcrypto::fence::kMessageStatus___PoppedAndAcked;
        ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s").c_str(), 
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          (ORG_LABCRYPTO_ABETTOR_data)(&status),
          sizeof(status)
        );
        Runtime::states_[messageId] = status;
        ORG_LABCRYPTO_ABETTOR__fs__move_file (
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pna").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pa").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
        );
        uint64_t currentTime = time(NULL);
        ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pat").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          (ORG_LABCRYPTO_ABETTOR_data)&currentTime,
          sizeof(currentTime)
        );
        Runtime::poppedAndAckedTotalCounter_++;
        ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
          (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
          (ORG_LABCRYPTO_ABETTOR_string)"patco", 
          (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::poppedAndAckedTotalCounter_), 
          (ORG_LABCRYPTO_ABETTOR_length)sizeof(Runtime::poppedAndAckedTotalCounter_)
        );
      } else {
        throw std::runtime_error("[" + ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() + 
            "]: Message is not found.");
      }
    }
  }
} // END OF NAMESPACE slave
} // END OF NAMESPACE fence
} // END OF NAMESPACE labcrypto
} // END OF NAMESPACE org