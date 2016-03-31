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
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/a").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/a").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/qfp").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/qfp").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir_ + "/qfr").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir_ + "/qfr").c_str());
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
        std::pair<uint64_t, uint16_t>(atoll(filenames[i]), status));
    }
    NAEEM_os__free_file_names(filenames, filenamesLength);
    /*
     * Reading arrived messages
     */
    NAEEM_os__enum_file_names(
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
      std::lock_guard<std::mutex> guard2(Runtime::transportInboxQueueLock_);
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