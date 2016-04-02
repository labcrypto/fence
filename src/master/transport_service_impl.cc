#include <sstream>
#include <thread>
#include <chrono>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>

#include <naeem/os.h>

#include <naeem++/conf/config_manager.h>

#include <transport/enums.h>
#include <transport/transport_message.h>
#include <transport/enqueue_report.h>

#include "transport_service_impl.h"
#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace master {
  void
  TransportServiceImpl::OnInit() {
    workDir_ = ::naeem::conf::ConfigManager::GetValueAsString("master", "work_dir");
    /*
     * Make directories
     */
    if (!NAEEM_os__dir_exists((NAEEM_path)workDir_.c_str())) {
      NAEEM_os__mkdir((NAEEM_path)workDir_.c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/s").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/s").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/ss").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/ss").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/a").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/a").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/aa").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/aa").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/rfp").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/rfp").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/rfr").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/rfr").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/ra").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/ra").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/rna").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/rna").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/rf").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/rf").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/pna").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/pna").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/pnat").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/pnat").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/pa").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/pa").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/pat").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/pat").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/e").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/e").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/ea").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/ea").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/ef").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/ef").c_str());
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
     * Reading arrived total counter file
     */
    if (NAEEM_os__file_exists((NAEEM_path)workDir_.c_str(), (NAEEM_string)"atco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"atco",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(Runtime::arrivedTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::arrivedTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << "Last Arrived Total Counter value is " << Runtime::arrivedTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Arrived Total Counter is set to " << Runtime::arrivedTotalCounter_ << std::endl;
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
      ::naeem::hottentot::runtime::Logger::GetOut() << "Last Ready For Pop Total Counter value is " << Runtime::readyForPopTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Ready For Pop Total Counter is set to " << Runtime::readyForPopTotalCounter_ << std::endl;
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
      ::naeem::hottentot::runtime::Logger::GetOut() << "Last Popped And Acked Total Counter value is " << Runtime::poppedAndAckedTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Popped And Acked Total Counter is set to " << Runtime::poppedAndAckedTotalCounter_ << std::endl;
    }
    /*
     * Reading enqueue failed total counter file
     */
    if (NAEEM_os__file_exists((NAEEM_path)workDir_.c_str(), (NAEEM_string)"eftco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"eftco",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(Runtime::enqueueFailedTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::enqueueFailedTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << "Last Enqueue Failed Total Counter value is " << Runtime::enqueueFailedTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Enqueue Failed Total Counter is set to " << Runtime::enqueueFailedTotalCounter_ << std::endl;
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
      ::naeem::hottentot::runtime::Logger::GetOut() << "Last Enqueued Total Counter value is " << Runtime::enqueuedTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Enqueued Total Counter is set to " << Runtime::enqueuedTotalCounter_ << std::endl;
    }
    /*
     * Reading ready for retrieval total counter file
     */
    if (NAEEM_os__file_exists((NAEEM_path)workDir_.c_str(), (NAEEM_string)"rfrtco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"rfrtco",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(Runtime::readyForRetrievalTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::readyForRetrievalTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << "Last Ready For Retrieval Total Counter value is " << Runtime::readyForRetrievalTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Ready For Retrieval Total Counter is set to " << Runtime::readyForRetrievalTotalCounter_ << std::endl;
    }
    /*
     * Reading enqueue failed total counter file
     */
    if (NAEEM_os__file_exists((NAEEM_path)workDir_.c_str(), (NAEEM_string)"eftco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir_.c_str(), 
        (NAEEM_string)"eftco",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(Runtime::enqueueFailedTotalCounter_);
      for (uint32_t i = 0; i < sizeof(Runtime::enqueueFailedTotalCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << "Enqueue Failed Total Counter value is " << Runtime::enqueueFailedTotalCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Enqueue Failed Total Counter is set to " << Runtime::enqueueFailedTotalCounter_ << std::endl;
    }
    /*
     * Reading states
     */
    NAEEM_string_ptr filenames;
    NAEEM_length filenamesLength = 0;
    NAEEM_os__enum_file_names(
      (NAEEM_path)(workDir_ + "/s").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint16_t status = 0;
      NAEEM_os__read_file3 (
        (NAEEM_path)(workDir_ + "/s/" + filenames[i]).c_str(),
        (NAEEM_data)(&status),
        0
      );
      Runtime::states_.insert(
        std::pair<uint64_t, uint16_t>(atoll(filenames[i]), status));
    }
    NAEEM_os__free_file_names(filenames, filenamesLength);
    /*
     * Reading arrived messages
     */
    NAEEM_os__enum_file_names (
      (NAEEM_path)(workDir_ + "/a").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      if (Runtime::states_.find(messageId) != Runtime::states_.end()) {
        if (Runtime::states_[messageId] == 
              (uint16_t)::ir::ntnaeem::gate::transport::kTransportMessageStatus___Arrived) {
          Runtime::arrived_.push_back(messageId);
        } else {
          // TODO: Message status is not Arrived !
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
              (uint16_t)::ir::ntnaeem::gate::transport::kTransportMessageStatus___ReadyForPop) {
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
              (uint16_t)::ir::ntnaeem::gate::transport::kTransportMessageStatus___PoppedButNotAcked) {
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
    /*
     * Reading enqueued messages
     */
    NAEEM_os__enum_file_names (
      (NAEEM_path)(workDir_ + "/e").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      if (Runtime::states_.find(messageId) != Runtime::states_.end()) {
        if (Runtime::states_[messageId] == 
              (uint16_t)::ir::ntnaeem::gate::transport::kTransportMessageStatus___EnqueuedForTransmission) {
          Runtime::enqueued_.push_back(messageId);
        } else {
          // TODO: Message status is not EnqueuedForTransmission !
        }
      } else {
        // TODO: Id does not exist in states map.
      }
    }
    NAEEM_os__free_file_names(filenames, filenamesLength);
    /*
     * Reading ready for retrieval messages
     */
    NAEEM_os__enum_file_names (
      (NAEEM_path)(workDir_ + "/rfr").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      if (Runtime::states_.find(messageId) != Runtime::states_.end()) {
        if (Runtime::states_[messageId] == 
              (uint16_t)::ir::ntnaeem::gate::transport::kTransportMessageStatus___ReadyForRetrieval) {
          NAEEM_os__read_file_with_path (
            (NAEEM_path)(workDir_ + "/rfr").c_str(), 
            (NAEEM_string)filenames[i],
            &temp, 
            &tempLength
          );
          ::ir::ntnaeem::gate::transport::TransportMessage transportMessage;
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
    NAEEM_os__free_file_names(filenames, filenamesLength);
    ::naeem::hottentot::runtime::Logger::GetOut() << "Transport Service is initialized." << std::endl;
  }
  void
  TransportServiceImpl::OnShutdown() {
    {
      std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
      Runtime::termSignal_ = true;
    }
    ::naeem::hottentot::runtime::Logger::GetOut() << "Waiting for master thread to exit ..." << std::endl;
    while (true) {
      std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
      if (Runtime::masterThreadTerminated_) {
        ::naeem::hottentot::runtime::Logger::GetOut() << "Master thread exited." << std::endl;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  void
  TransportServiceImpl::Transmit(
      ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::TransportMessage> &messages, 
      ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::EnqueueReport> &out, 
      ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "TransportServiceImpl::AcceptSlaveMassages() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      std::lock_guard<std::mutex> guard2(Runtime::arrivedLock_);
      for (uint32_t i = 0; i < messages.Size(); i++) {
        ::ir::ntnaeem::gate::transport::EnqueueReport *enqueueReport = 
          new ::ir::ntnaeem::gate::transport::EnqueueReport;
        enqueueReport->SetSlaveMId(messages.Get(i)->GetSlaveMId());
        try {
          {
            std::lock_guard<std::mutex> guard(Runtime::messageIdCounterLock_);
            messages.Get(i)->SetMasterMId(Runtime::messageIdCounter_);
            enqueueReport->SetMasterMId(messages.Get(i)->GetMasterMId());
            Runtime::messageIdCounter_++;
            NAEEM_os__write_to_file (
              (NAEEM_path)workDir_.c_str(), 
              (NAEEM_string)"mco", 
              (NAEEM_data)&(Runtime::messageIdCounter_), 
              (NAEEM_length)sizeof(Runtime::messageIdCounter_)
            );
          }
          /*
           * Message serialization
           */
          NAEEM_length dataLength = 0;
          NAEEM_data data = messages.Get(i)->Serialize(&dataLength);
          try {
            std::stringstream ss;
            ss << messages.Get(i)->GetMasterMId().GetValue();
            /*
             * Persisting message
             */
            NAEEM_os__write_to_file (
              (NAEEM_path)(workDir_ + "/a").c_str(), 
              (NAEEM_string)ss.str().c_str(),
              data,
              dataLength
            );
            /*
             * Updating status
             */
            uint16_t status = (uint16_t)::ir::ntnaeem::gate::transport::kTransportMessageStatus___Arrived;
            NAEEM_os__write_to_file (
              (NAEEM_path)(workDir_ + "/s").c_str(), 
              (NAEEM_string)ss.str().c_str(),
              (NAEEM_data)(&status),
              sizeof(status)
            );
            Runtime::arrived_.push_back(messages.Get(i)->GetMasterMId().GetValue());
            /*
             * Updating arrived total counter
             */
            Runtime::arrivedTotalCounter_++;
            NAEEM_os__write_to_file (
              (NAEEM_path)workDir_.c_str(), 
              (NAEEM_string)"atco", 
              (NAEEM_data)&(Runtime::arrivedTotalCounter_), 
              (NAEEM_length)sizeof(Runtime::arrivedTotalCounter_)
            );
            delete [] data;
          } catch (std::exception &e) {
            delete [] data;
            throw std::runtime_error(e.what());
          } catch (...) {
            delete [] data;
            throw std::runtime_error("Unknown exception.");
          }
          enqueueReport->SetFailed(false);
          enqueueReport->SetErrorMessage("");
          /* if ((i % 2) == 0) {
            throw std::runtime_error("Simulated exception.");
          } */
        } catch (std::exception &e) {
          enqueueReport->SetFailed(true);
          enqueueReport->SetErrorMessage(e.what());
        } catch (...) {
          enqueueReport->SetFailed(true);
          enqueueReport->SetErrorMessage("Arrival failed.");
        }
        out.Add(enqueueReport);
        // ::naeem::hottentot::runtime::Logger::GetOut() << Runtime::GetCurrentStat();
      }
    }
  }
  void
  TransportServiceImpl::Retrieve(
      ::naeem::hottentot::runtime::types::UInt32 &slaveId, 
      ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::TransportMessage> &out, 
      ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "TransportServiceImpl::RetrieveSlaveMessages() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      std::lock_guard<std::mutex> guard2(Runtime::transportOutboxQueueLock_);
      /* std::vector<::ir::ntnaeem::gate::transport::TransportMessage*> messages = 
        Runtime::transportOutboxQueue_->PopAll(slaveId.GetValue());
      for (uint32_t i = 0; i < messages.size(); i++) {
        out.Add(messages[i]);
        Runtime::transportSentQueue_->Put(messages[i]);
      } */
    }
  }
  void
  TransportServiceImpl::Ack(
      ::naeem::hottentot::runtime::types::List< ::naeem::hottentot::runtime::types::UInt64> &masterMIds, 
      ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "TransportServiceImpl::Ack() is called." << std::endl;
    }
    // TODO
  }
  void
  TransportServiceImpl::GetStatus(
      ::naeem::hottentot::runtime::types::UInt64 &masterMId, 
      ::naeem::hottentot::runtime::types::UInt16 &out, 
      ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "TransportServiceImpl::GetStatus() is called." << std::endl;
    }
    // TODO
  }
} // END OF NAMESPACE master
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir