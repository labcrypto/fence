#include <sstream>
#include <thread>
#include <chrono>

#include <org/labcrypto/hottentot/runtime/configuration.h>
#include <org/labcrypto/hottentot/runtime/logger.h>
#include <org/labcrypto/hottentot/runtime/utils.h>

#include <org/labcrypto/abettor/fs.h>

#include <org/labcrypto/abettor++/conf/config_manager.h>
#include <org/labcrypto/abettor++/date/helper.h>

#include <transport/enums.h>
#include <transport/transport_message.h>
#include <transport/enqueue_report.h>

#include "transport_service_impl.h"
#include "runtime.h"


namespace org {
namespace labcrypto {
namespace fence {
namespace master {
  void
  TransportServiceImpl::OnInit() {
    workDir_ = ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("master", "work_dir");
    ackTimeout_ = ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("master", "ack_timeout");
    /*
     * Make directories
     */
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/ss").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/ss").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/a").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/a").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/aa").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/aa").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rfp").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rfp").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rfr").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rfr").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/ra").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/ra").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rna").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rna").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rnat").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rnat").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rat").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rat").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rf").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rf").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pna").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pna").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pnat").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pnat").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pa").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pa").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pat").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pat").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/e").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/e").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/ea").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/ea").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/ef").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/ef").c_str());
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
     * Reading arrived total counter file
     */
    if (ORG_LABCRYPTO_ABETTOR__fs__file_exists((ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), (ORG_LABCRYPTO_ABETTOR_string)"atco")) {
      ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
        (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"atco",
        &temp, 
        &tempLength
      );
      ORG_LABCRYPTO_ABETTOR_data ptr = (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::arrivedTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::arrivedTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Last Arrived Total Counter value is " << Runtime::arrivedTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Arrived Total Counter is set to " << Runtime::arrivedTotalCounter_ << std::endl;
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
     * Reading enqueue failed total counter file
     */
    if (ORG_LABCRYPTO_ABETTOR__fs__file_exists((ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), (ORG_LABCRYPTO_ABETTOR_string)"eftco")) {
      ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
        (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"eftco",
        &temp, 
        &tempLength
      );
      ORG_LABCRYPTO_ABETTOR_data ptr = (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::enqueueFailedTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::enqueueFailedTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Last Enqueue Failed Total Counter value is " << Runtime::enqueueFailedTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Enqueue Failed Total Counter is set to " << Runtime::enqueueFailedTotalCounter_ << std::endl;
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
     * Reading ready for retrieval total counter file
     */
    if (ORG_LABCRYPTO_ABETTOR__fs__file_exists((ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), (ORG_LABCRYPTO_ABETTOR_string)"rfrtco")) {
      ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
        (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"rfrtco",
        &temp, 
        &tempLength
      );
      ORG_LABCRYPTO_ABETTOR_data ptr = (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::readyForRetrievalTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::readyForRetrievalTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Last Ready For Retrieval Total Counter value is " << Runtime::readyForRetrievalTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Ready For Retrieval Total Counter is set to " << Runtime::readyForRetrievalTotalCounter_ << std::endl;
    }
    /*
     * Reading enqueue failed total counter file
     */
    if (ORG_LABCRYPTO_ABETTOR__fs__file_exists((ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), (ORG_LABCRYPTO_ABETTOR_string)"eftco")) {
      ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
        (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"eftco",
        &temp, 
        &tempLength
      );
      ORG_LABCRYPTO_ABETTOR_data ptr = (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::enqueueFailedTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::enqueueFailedTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Enqueue Failed Total Counter value is " << Runtime::enqueueFailedTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() <<  
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Enqueue Failed Total Counter is set to " << Runtime::enqueueFailedTotalCounter_ << std::endl;
    }
    /*
     * Reading retrieved and acked total counter file
     */
    if (ORG_LABCRYPTO_ABETTOR__fs__file_exists((ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), (ORG_LABCRYPTO_ABETTOR_string)"ratco")) {
      ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
        (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"ratco",
        &temp, 
        &tempLength
      );
      ORG_LABCRYPTO_ABETTOR_data ptr = (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::retrievedAndAckedTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::retrievedAndAckedTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Last Retrieved And Acked Total Counter value is " << Runtime::retrievedAndAckedTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Retrieved And Acked Total Counter is set to " << Runtime::retrievedAndAckedTotalCounter_ << std::endl;
    }
    /*
     * Reading states
     */
    ORG_LABCRYPTO_ABETTOR_string_ptr filenames;
    ORG_LABCRYPTO_ABETTOR_length filenamesLength = 0;
    ORG_LABCRYPTO_ABETTOR__fs__enum_file_names(
      (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint16_t status = 0;
      ORG_LABCRYPTO_ABETTOR__fs__read_file_into_buffer (
        (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s/" + filenames[i]).c_str(),
        (ORG_LABCRYPTO_ABETTOR_data)(&status),
        0
      );
      Runtime::states_.insert(
        std::pair<uint64_t, uint16_t>(atoll(filenames[i]), status));
    }
    ORG_LABCRYPTO_ABETTOR__fs__free_file_names(filenames, filenamesLength);
    /*
     * Reading arrived messages
     */
    ORG_LABCRYPTO_ABETTOR__fs__enum_file_names (
      (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/a").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      if (Runtime::states_.find(messageId) != Runtime::states_.end()) {
        if (Runtime::states_[messageId] == 
              (uint16_t)::org::labcrypto::fence::transport::kTransportMessageStatus___Arrived) {
          Runtime::arrived_.push_back(messageId);
        } else {
          // TODO: Message status is not Arrived !
        }
      } else {
        // TODO: Id does not exist in states map.
      }
    }
    ORG_LABCRYPTO_ABETTOR__fs__free_file_names(filenames, filenamesLength);
    /*
     * Reading ready for pop messages
     */
    ORG_LABCRYPTO_ABETTOR__fs__enum_file_names(
      (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rfp").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      if (Runtime::states_.find(messageId) != Runtime::states_.end()) {
        if (Runtime::states_[messageId] == 
              (uint16_t)::org::labcrypto::fence::transport::kTransportMessageStatus___ReadyForPop) {
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
    ORG_LABCRYPTO_ABETTOR__fs__enum_file_names(
      (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/pna").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      if (Runtime::states_.find(messageId) != Runtime::states_.end()) {
        if (Runtime::states_[messageId] == 
              (uint16_t)::org::labcrypto::fence::transport::kTransportMessageStatus___PoppedButNotAcked) {
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
        if (Runtime::states_[messageId] == 
              (uint16_t)::org::labcrypto::fence::transport::kTransportMessageStatus___EnqueuedForTransmission) {
          Runtime::enqueued_.push_back(messageId);
        } else {
          // TODO: Message status is not EnqueuedForTransmission !
        }
      } else {
        // TODO: Id does not exist in states map.
      }
    }
    ORG_LABCRYPTO_ABETTOR__fs__free_file_names(filenames, filenamesLength);
    /*
     * Reading ready for retrieval messages
     */
    ORG_LABCRYPTO_ABETTOR__fs__enum_file_names (
      (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rfr").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      if (Runtime::states_.find(messageId) != Runtime::states_.end()) {
        if (Runtime::states_[messageId] == 
              (uint16_t)::org::labcrypto::fence::transport::kTransportMessageStatus___ReadyForRetrieval) {
          ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
            (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rfr").c_str(), 
            (ORG_LABCRYPTO_ABETTOR_string)filenames[i],
            &temp, 
            &tempLength
          );
          ::org::labcrypto::fence::transport::TransportMessage transportMessage;
          transportMessage.Deserialize(temp, tempLength);
          free(temp);
          if (Runtime::readyForRetrieval_.find(transportMessage.GetSlaveId().GetValue()) 
                == Runtime::readyForRetrieval_.end()) {
            Runtime::readyForRetrieval_.insert(std::pair<uint32_t, std::vector<uint64_t>*>
              (transportMessage.GetSlaveId().GetValue(), new std::vector<uint64_t>()));
          }
          Runtime::readyForRetrieval_[transportMessage.GetSlaveId().GetValue()]
            ->push_back(transportMessage.GetMasterMId().GetValue());
        } else {
          // TODO: Message status is not ReadyForRetrieval !
        }
      } else {
        // TODO: Id does not exist in states map.
      }
    }
    ORG_LABCRYPTO_ABETTOR__fs__free_file_names(filenames, filenamesLength);
    /*
     * Reading retrieved but not acked messages
     */
    ORG_LABCRYPTO_ABETTOR__fs__enum_file_names(
      (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rna").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      if (Runtime::states_.find(messageId) != Runtime::states_.end()) {
        if (Runtime::states_[messageId] == 
              (uint16_t)::org::labcrypto::fence::transport::kTransportMessageStatus___RetrievedButNotAcked) {
          ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
            (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rna").c_str(), 
            (ORG_LABCRYPTO_ABETTOR_string)filenames[i],
            &temp, 
            &tempLength
          );
          ::org::labcrypto::fence::transport::TransportMessage transportMessage;
          transportMessage.Deserialize(temp, tempLength);
          free(temp);
          uint64_t popTime = 0;
          ORG_LABCRYPTO_ABETTOR__fs__read_file_into_buffer (
            (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rnat/" + filenames[i]).c_str(),
            (ORG_LABCRYPTO_ABETTOR_data)(&popTime),
            0
          );
          uint32_t slaveId = transportMessage.GetSlaveId().GetValue();
          /* ORG_LABCRYPTO_ABETTOR__fs__read_file_into_buffer (
            (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/ss/" + filenames[i] + ".slaveid").c_str(),
            (ORG_LABCRYPTO_ABETTOR_data)(&slaveId),
            0
          ); */
          if (Runtime::retrievedButNotAcked_.find(slaveId) == 
                Runtime::retrievedButNotAcked_.end()) {
            Runtime::retrievedButNotAcked_.insert(
              std::pair<uint32_t, std::map<uint64_t, uint64_t>*>
                (slaveId, new std::map<uint64_t, uint64_t>()));
          }
          (*(Runtime::retrievedButNotAcked_[slaveId]))[transportMessage.GetMasterMId().GetValue()] = popTime;
        } else {
          // TODO: Message status is not PoppedButNotAcked !
        }
      } else {
        // TODO: Id does not exist in states map.
      }
    }
    ORG_LABCRYPTO_ABETTOR__fs__free_file_names(filenames, filenamesLength);
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
        "Transport Service is initialized." << std::endl;
  }
  void
  TransportServiceImpl::OnShutdown() {
    {
      std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
      Runtime::termSignal_ = true;
    }
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
        "Waiting for master thread to exit ..." << std::endl;
    while (true) {
      std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
      if (Runtime::masterThreadTerminated_) {
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
          "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
            "Master thread exited." << std::endl;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  void
  TransportServiceImpl::Transmit(
    ::org::labcrypto::hottentot::List< ::org::labcrypto::fence::transport::TransportMessage> &messages, 
    ::org::labcrypto::hottentot::List< ::org::labcrypto::fence::transport::EnqueueReport> &out, 
    ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "TransportServiceImpl::AcceptSlaveMassages() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      std::lock_guard<std::mutex> guard2(Runtime::arrivedLock_);
      for (uint32_t i = 0; i < messages.Size(); i++) {
        ::org::labcrypto::fence::transport::EnqueueReport *enqueueReport = 
          new ::org::labcrypto::fence::transport::EnqueueReport;
        enqueueReport->SetSlaveMId(messages.Get(i)->GetSlaveMId());
        try {
          {
            std::lock_guard<std::mutex> guard(Runtime::messageIdCounterLock_);
            messages.Get(i)->SetMasterMId(Runtime::messageIdCounter_);
            enqueueReport->SetMasterMId(messages.Get(i)->GetMasterMId());
            Runtime::messageIdCounter_++;
            ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
              (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
              (ORG_LABCRYPTO_ABETTOR_string)"mco", 
              (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::messageIdCounter_), 
              (ORG_LABCRYPTO_ABETTOR_length)sizeof(Runtime::messageIdCounter_)
            );
          }
          /*
           * Message serialization
           */
          ORG_LABCRYPTO_ABETTOR_length dataLength = 0;
          ORG_LABCRYPTO_ABETTOR_data data = messages.Get(i)->Serialize(&dataLength);
          try {
            std::stringstream ss;
            ss << messages.Get(i)->GetMasterMId().GetValue();
            /*
             * Persisting message
             */
            ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
              (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/a").c_str(), 
              (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
              data,
              dataLength
            );
            /*
             * Updating status
             */
            uint16_t status = (uint16_t)::org::labcrypto::fence::transport::kTransportMessageStatus___Arrived;
            ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
              (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s").c_str(), 
              (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
              (ORG_LABCRYPTO_ABETTOR_data)(&status),
              sizeof(status)
            );
            Runtime::arrived_.push_back(messages.Get(i)->GetMasterMId().GetValue());
            /*
             * Updating arrived total counter
             */
            Runtime::arrivedTotalCounter_++;
            ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
              (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
              (ORG_LABCRYPTO_ABETTOR_string)"atco", 
              (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::arrivedTotalCounter_), 
              (ORG_LABCRYPTO_ABETTOR_length)sizeof(Runtime::arrivedTotalCounter_)
            );
            delete [] data;
          } catch (std::exception &e) {
            delete [] data;
            throw std::runtime_error("[" + ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() + "]: " + e.what());
          } catch (...) {
            delete [] data;
            throw std::runtime_error("[" + ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() + "]: Unknown exception.");
          }
          enqueueReport->SetFailed(false);
          enqueueReport->SetErrorMessage("");
        } catch (std::exception &e) {
          enqueueReport->SetFailed(true);
          enqueueReport->SetErrorMessage(e.what());
        } catch (...) {
          enqueueReport->SetFailed(true);
          enqueueReport->SetErrorMessage("Arrival failed.");
        }
        out.Add(enqueueReport);
      }
    }
  }
  void
  TransportServiceImpl::Retrieve(
    ::org::labcrypto::hottentot::UInt32 &slaveId, 
    ::org::labcrypto::hottentot::List< ::org::labcrypto::fence::transport::TransportMessage> &out, 
    ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "TransportServiceImpl::RetrieveSlaveMessages() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      std::lock_guard<std::mutex> guard2(Runtime::readyForRetrievalLock_);
      if (Runtime::retrievedButNotAcked_.find(slaveId.GetValue()) != Runtime::retrievedButNotAcked_.end()) {
        if (Runtime::retrievedButNotAcked_[slaveId.GetValue()]->size() > 0) {
          for (std::map<uint64_t, uint64_t>::iterator it = Runtime::retrievedButNotAcked_[slaveId.GetValue()]->begin();
               it != Runtime::retrievedButNotAcked_[slaveId.GetValue()]->end();
               it++) {
            uint64_t currentTime = time(NULL);
            if ((currentTime - it->second) > ackTimeout_) {
              std::stringstream ss;
              ss << it->first;
              ORG_LABCRYPTO_ABETTOR_data data;
              ORG_LABCRYPTO_ABETTOR_length dataLength;
              ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
                (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rna").c_str(),
                (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                &data,
                &dataLength
              );
              ::org::labcrypto::fence::transport::TransportMessage *transportMessage =
                new ::org::labcrypto::fence::transport::TransportMessage;
              transportMessage->Deserialize(data, dataLength);
              out.Add(transportMessage);
              free(data);
              uint64_t currentTime = time(NULL);
              ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rnat").c_str(),
                (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                (ORG_LABCRYPTO_ABETTOR_data)&currentTime,
                sizeof(currentTime)
              );
              if (Runtime::retrievedButNotAcked_.find(slaveId.GetValue()) == Runtime::retrievedButNotAcked_.end()) {
                Runtime::retrievedButNotAcked_.insert(
                  std::pair<uint32_t, std::map<uint64_t, uint64_t>*>(
                    slaveId.GetValue(), new std::map<uint64_t, uint64_t>()));
              }
              (*(Runtime::retrievedButNotAcked_[slaveId.GetValue()]))[it->first] = currentTime;
            }
          }
        }
      }
      if (Runtime::readyForRetrieval_.find(slaveId.GetValue()) == Runtime::readyForRetrieval_.end()) {
        return;
      }
      if (Runtime::readyForRetrieval_[slaveId.GetValue()]->size() == 0) {
        return;
      }
      std::vector<uint64_t> readyForRetrievalIds = std::move(*(Runtime::readyForRetrieval_[slaveId.GetValue()]));
      for (uint64_t i = 0; i < readyForRetrievalIds.size(); i++) {
        std::stringstream ss;
        ss << readyForRetrievalIds[i];
        ORG_LABCRYPTO_ABETTOR_data data;
        ORG_LABCRYPTO_ABETTOR_length dataLength;
        ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rfr").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          &data,
          &dataLength
        );
        ::org::labcrypto::fence::transport::TransportMessage *transportMessage =
          new ::org::labcrypto::fence::transport::TransportMessage;
        transportMessage->Deserialize(data, dataLength);
        free(data);
        out.Add(transportMessage);
        uint16_t status = 
          (uint16_t)::org::labcrypto::fence::transport::kTransportMessageStatus___RetrievedButNotAcked;
        ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s").c_str(), 
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          (ORG_LABCRYPTO_ABETTOR_data)(&status),
          sizeof(status)
        );
        Runtime::states_[readyForRetrievalIds[i]] = status;
        ORG_LABCRYPTO_ABETTOR__fs__move_file (
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rfr").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rna").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
        );
        uint64_t currentTime = time(NULL);
        ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rnat").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          (ORG_LABCRYPTO_ABETTOR_data)&currentTime,
          sizeof(currentTime)
        );
        if (Runtime::retrievedButNotAcked_.find(slaveId.GetValue()) == Runtime::retrievedButNotAcked_.end()) {
          Runtime::retrievedButNotAcked_.insert(
            std::pair<uint32_t, std::map<uint64_t, uint64_t>*>(
              slaveId.GetValue(), new std::map<uint64_t, uint64_t>()));
        }
        (*(Runtime::retrievedButNotAcked_[slaveId.GetValue()]))[readyForRetrievalIds[i]] = currentTime;
      }      
    }
  }
  void
  TransportServiceImpl::Ack(
    ::org::labcrypto::hottentot::List< ::org::labcrypto::hottentot::UInt64> &masterMIds, 
    ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "TransportServiceImpl::Ack() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      std::lock_guard<std::mutex> guard2(Runtime::readyForRetrievalLock_);
      for (uint32_t i = 0; i < masterMIds.Size(); i++) {
        uint64_t messageId = masterMIds.Get(i)->GetValue();
        std::stringstream ss;
        ss << messageId;
        if (ORG_LABCRYPTO_ABETTOR__fs__file_exists (
              (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rna").c_str(), 
              (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
            )
        ) {
          ORG_LABCRYPTO_ABETTOR_data data;
          ORG_LABCRYPTO_ABETTOR_length dataLength;
          ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
            (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rna").c_str(),
            (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
            &data,
            &dataLength
          );
          ::org::labcrypto::fence::transport::TransportMessage transportMessage;
          transportMessage.Deserialize(data, dataLength);
          free(data);
          if (Runtime::retrievedButNotAcked_.find(transportMessage.GetSlaveId().GetValue()) 
                != Runtime::retrievedButNotAcked_.end()) {
            Runtime::retrievedButNotAcked_[transportMessage.GetSlaveId().GetValue()]->erase(messageId);
          }
          uint16_t status = 
            (uint16_t)::org::labcrypto::fence::transport::kTransportMessageStatus___RetrievedAndAcked;
          ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
            (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s").c_str(), 
            (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
            (ORG_LABCRYPTO_ABETTOR_data)(&status),
            sizeof(status)
          );
          Runtime::states_[messageId] = status;
          ORG_LABCRYPTO_ABETTOR__fs__move_file (
            (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rna").c_str(),
            (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
            (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/ra").c_str(),
            (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
          );
          uint64_t currentTime = time(NULL);
          ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
            (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rat").c_str(),
            (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
            (ORG_LABCRYPTO_ABETTOR_data)&currentTime,
            sizeof(currentTime)
          );
          Runtime::retrievedAndAckedTotalCounter_++;
          ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
            (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
            (ORG_LABCRYPTO_ABETTOR_string)"ratco", 
            (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::retrievedAndAckedTotalCounter_), 
            (ORG_LABCRYPTO_ABETTOR_length)sizeof(Runtime::retrievedAndAckedTotalCounter_)
          );
        }
      }
    }
  }
  void
  TransportServiceImpl::GetStatus(
    ::org::labcrypto::hottentot::UInt64 &masterMId, 
    ::org::labcrypto::hottentot::UInt16 &out, 
    ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "TransportServiceImpl::GetStatus() is called." << std::endl;
    }
    std::lock_guard<std::mutex> guard2(Runtime::mainLock_);
    if (Runtime::states_.find(masterMId.GetValue()) == Runtime::states_.end()) {
      std::stringstream filePath;
      filePath << masterMId.GetValue();
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
        Runtime::states_.insert(std::pair<uint64_t, uint16_t>(masterMId.GetValue(), status));
      } else {
        throw std::runtime_error("[" + ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() + "]: Message id is not found.");
      }
    }
    out.SetValue(Runtime::states_[masterMId.GetValue()]);
  }
} // END OF NAMESPACE master
} // END OF NAMESPACE fence
} // END OF NAMESPACE labcrypto
} // END OF NAMESPACE org