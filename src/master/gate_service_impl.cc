#include <sstream>
#include <thread>

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
namespace master {
  // void
  // PutInOutboxQueue(::ir::ntnaeem::gate::Message *message) {
  //   // TODO: Serialize and persist the message for FT purposes
  //   std::lock_guard<std::mutex> guard(Runtime::mainLock_);
  //   std::lock_guard<std::mutex> guard2(Runtime::outboxQueueLock_);
  //   Runtime::outboxQueue_->Put(message);
  //   std::cout << "Message is enqueued with id: " << message->GetId().GetValue() << std::endl;
  //   // pthread_exit(NULL);
  // }
  void
  GateServiceImpl::OnInit() {
    workDir_ = ::naeem::conf::ConfigManager::GetValueAsString("master", "work_dir");
    ::naeem::hottentot::runtime::Logger::GetOut() << "Gate Service is initialized." << std::endl;
  }
  void
  GateServiceImpl::OnShutdown() {
    // TODO: Called when service is shutting down.
  }
  void
  GateServiceImpl::Enqueue(
      ::ir::ntnaeem::gate::Message &message, 
      ::naeem::hottentot::runtime::types::UInt64 &out, 
      ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::EnqueueMessage() is called." << std::endl;
    }
    // {
    //   std::lock_guard<std::mutex> guard(Runtime::messageIdCounterLock_);
    //   message.SetId(Runtime::messageIdCounter_);
    //   out.SetValue(Runtime::messageIdCounter_);
    //   Runtime::messageIdCounter_++;
    // }
    // // TODO: Select a thread from thread-pool
    // ::ir::ntnaeem::gate::Message *newMessage = 
    //   new ::ir::ntnaeem::gate::Message;
    // newMessage->SetId(message.GetId());
    // newMessage->SetLabel(message.GetLabel());
    // newMessage->SetRelLabel(message.GetRelLabel());
    // newMessage->SetRelId(message.GetRelId());
    // newMessage->SetContent(message.GetContent());
    // // ::naeem::hottentot::runtime::Utils::PrintArray("CONTENT", message.GetContent().GetValue(), message.GetContent().GetLength());
    // // std::thread t(PutInOutboxQueue, newMessage);
    // // t.detach();
    // PutInOutboxQueue(newMessage);
    // // Runtime::PrintStatus();
  }
  void
  GateServiceImpl::GetStatus(
      ::naeem::hottentot::runtime::types::UInt64 &id, 
      ::naeem::hottentot::runtime::types::UInt16 &out, 
      ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::GetMessageStatus() is called." << std::endl;
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
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::HasMoreMessage() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      std::lock_guard<std::mutex> guard2(Runtime::readyForPopLock_);
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
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::NextMessage() is called." << std::endl;
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
            if ((currentTime - it->second) > 10) {
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
        throw std::runtime_error("Internal server error.");
      }
      std::cout << ">>>>>>>>>> " << messageId << std::endl;
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
          (NAEEM_path)(workDir_ + "/rfp").c_str(),
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
      out.SetRelLabel(message.GetRelLabel());
      out.SetLabel(message.GetLabel());
      out.SetContent(message.GetContent());
      if (!messageIsChosen) {
        uint16_t status = 
          (uint16_t)::ir::ntnaeem::gate::transport::kTransportMessageStatus___PoppedButNotAcked;
        NAEEM_os__write_to_file (
          (NAEEM_path)(workDir_ + "/s").c_str(), 
          (NAEEM_string)ss.str().c_str(),
          (NAEEM_data)(&status),
          sizeof(status)
        );
        NAEEM_os__move_file (
          (NAEEM_path)(workDir_ + "/rfp").c_str(),
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
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateServiceImpl::Ack() is called." << std::endl;
    }
    // TODO
  }
} // END OF NAMESPACE master
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir