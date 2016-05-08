#include <sstream>
#include <thread>

#include <org/labcrypto/hottentot/runtime/configuration.h>
#include <org/labcrypto/hottentot/runtime/logger.h>
#include <org/labcrypto/hottentot/runtime/utils.h>

#include <org/labcrypto/abettor/fs.h>

#include <org/labcrypto/abettor++/conf/config_manager.h>
#include <org/labcrypto/abettor++/date/helper.h>

#include <fence/message.h>

#include <transport/enums.h>
#include <transport/transport_message.h>

#include "fence_service_impl.h"
#include "runtime.h"


namespace org {
namespace labcrypto {
namespace fence {
namespace master {
  void
  FenceServiceImpl::OnInit() {
    workDir_ = ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("master", "work_dir");
    ackTimeout_ = ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("master", "ack_timeout");
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
        "Fence Service is initialized." << std::endl;
  }
  void
  FenceServiceImpl::OnShutdown() {
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
          "FenceServiceImpl::EnqueueMessage() is called." << std::endl;
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
      delete [] data;
      uint16_t status = (uint16_t)::org::labcrypto::fence::transport::kTransportMessageStatus___EnqueuedForTransmission;
      ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
        (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s").c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
        (ORG_LABCRYPTO_ABETTOR_data)(&status),
        sizeof(status)
      );
      Runtime::states_[message.GetId().GetValue()] = status;
      Runtime::enqueuedTotalCounter_++;
      ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
        (ORG_LABCRYPTO_ABETTOR_path)workDir_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"etco", 
        (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::enqueuedTotalCounter_), 
        (ORG_LABCRYPTO_ABETTOR_length)sizeof(Runtime::enqueuedTotalCounter_)
      );
      Runtime::enqueued_.push_back(message.GetId().GetValue());
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
          "FenceServiceImpl::GetMessageStatus() is called." << std::endl;
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
        Runtime::states_.insert(std::pair<uint64_t, uint16_t>(id.GetValue(), status));
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
    // TODO
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
          "FenceServiceImpl::HasMoreMessage() is called." << std::endl;
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
          "FenceServiceImpl::NextMessage() is called." << std::endl;
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
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rfp").c_str(),
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
          (uint16_t)::org::labcrypto::fence::transport::kTransportMessageStatus___PoppedButNotAcked;
        ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/s").c_str(), 
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          (ORG_LABCRYPTO_ABETTOR_data)(&status),
          sizeof(status)
        );
        Runtime::states_[messageId] = status;
        ORG_LABCRYPTO_ABETTOR__fs__move_file (
          (ORG_LABCRYPTO_ABETTOR_path)(workDir_ + "/rfp").c_str(),
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
          (uint16_t)::org::labcrypto::fence::transport::kTransportMessageStatus___PoppedAndAcked;
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
        throw std::runtime_error("[" + ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() + "]: Message is not found.");
      }
    }
  }
} // END OF NAMESPACE master
} // END OF NAMESPACE fence
} // END OF NAMESPACE labcrypto
} // END OF NAMESPACE org