#include <sstream>

#include <naeem/os.h>

#include <naeem++/conf/config_manager.h>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>
#include <naeem/hottentot/runtime/proxy/proxy.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include <gate/message.h>

#include <naeem/gate/client/simple_gate_client.h>
#include <naeem/gate/client/runtime.h>
#include <naeem/gate/client/submitter_thread.h>


namespace naeem {
namespace gate {
namespace client {
  void 
  SimpleGateClient::Init (
    int argc, 
    char **argv
  ) {
    ::naeem::hottentot::runtime::Logger::Init();
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy runtime is initialized." << std::endl;
    }
    ::naeem::gate::client::Runtime::Init(argc, argv);
    submitterThread_ = new SubmitterThread(enqueueLabel_, popLabel_);
    submitterThread_->Start();
  }
  void 
  SimpleGateClient::Shutdown() {
    ::naeem::gate::client::Runtime::Shutdown();
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::naeem::hottentot::runtime::Logger::Shutdown();
    delete submitterThread_;
  }
  uint64_t 
  SimpleGateClient::SubmitMessage (
    unsigned char *data, 
    uint32_t length
  ) {
    if (!::naeem::conf::ConfigManager::HasValue("gate-client", "work_dir")) {
      throw std::runtime_error("(2) ERROR: Value 'gate-client.work_dir' is not found in configurations.");
    }
    std::string workDir = ::naeem::conf::ConfigManager::GetValueAsString("gate-client", "work_dir");
    uint64_t messageId;
    {
      std::lock_guard<std::mutex> guard(Runtime::messageIdCounterLock_);
      messageId = Runtime::messageIdCounter_;
      Runtime::messageIdCounter_++;
      NAEEM_os__write_to_file (
        (NAEEM_path)workDir.c_str(), 
        (NAEEM_string)"mco", 
        (NAEEM_data)&(Runtime::messageIdCounter_), 
        (NAEEM_length)sizeof(Runtime::messageIdCounter_)
      );
    }
    std::stringstream ss;
    ss << messageId;
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      ::ir::ntnaeem::gate::Message message;
      try {
        message.SetId(0);
        message.SetLabel(enqueueLabel_);
        message.SetRelId(0);
        message.SetContent(::naeem::hottentot::runtime::types::ByteArray(data, length));
        NAEEM_length dataLength;
        NAEEM_data data = message.Serialize(&dataLength);
        NAEEM_os__write_to_file (
          (NAEEM_path)(workDir + "/e").c_str(), 
          (NAEEM_string)ss.str().c_str(), 
          data, 
          dataLength
        );
        delete [] data;
        Runtime::enqueued_.push_back(messageId);
      } catch (std::exception &e) {
        ::naeem::hottentot::runtime::Logger::GetError() << 
          "ERROR: " << e.what() << std::endl;
      } catch (...) {
        ::naeem::hottentot::runtime::Logger::GetError() << 
          "ERROR: Unknown error." << std::endl;
      }
    }
    return messageId;
  }
  std::vector<Message*>
  SimpleGateClient::GetMessages () {
    if (!::naeem::conf::ConfigManager::HasValue("gate-client", "work_dir")) {
      throw std::runtime_error("(3) ERROR: Value 'gate-client.work_dir' is not found in configurations.");
    }
    std::string workDir = ::naeem::conf::ConfigManager::GetValueAsString("gate-client", "work_dir");
    uint32_t ackTimeout_ = ::naeem::conf::ConfigManager::GetValueAsUInt32("gate-client", "ack_timeout");
    std::vector<Message*> messages;
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      if (Runtime::poppedButNotAcked_.find(popLabel_) != Runtime::poppedButNotAcked_.end()) {
        if (Runtime::poppedButNotAcked_[popLabel_]->size() > 0) {
          for (std::map<uint64_t, uint64_t>::iterator it = Runtime::poppedButNotAcked_[popLabel_]->begin();
               it != Runtime::poppedButNotAcked_[popLabel_]->end();
               it++) {
            uint64_t currentTime = time(NULL);
            if ((currentTime - it->second) > ackTimeout_) {
              std::stringstream ss;
              ss << it->first;
              NAEEM_data data;
              NAEEM_length dataLength;
              NAEEM_os__read_file_with_path (
                (NAEEM_path)(workDir + "/pna").c_str(),
                (NAEEM_string)ss.str().c_str(),
                &data,
                &dataLength
              );
              ::ir::ntnaeem::gate::Message message;
              message.Deserialize(data, dataLength);
              free(data);
              /* ------------------------------------------ */
              ::naeem::gate::client::Message *clientMessage = 
                new ::naeem::gate::client::Message;
              clientMessage->SetId (it->first);
              if (message.GetRelId().GetValue() != 0) {
                std::stringstream crss;
                crss << message.GetRelId().GetValue();
                uint64_t clientRelId;
                NAEEM_os__read_file3 (
                  (NAEEM_path)(workDir + "/s/" + crss.str() + ".cid").c_str(),
                  (NAEEM_data)&clientRelId,
                  0
                );
                clientMessage->SetRelId(clientRelId);
              } else {
                clientMessage->SetRelId(0);
              }
              clientMessage->SetLabel(popLabel_);
              clientMessage->SetContent (
                ByteArray (
                  message.GetContent().GetValue(), 
                  message.GetContent().GetLength()
                )
              );
              messages.push_back(clientMessage);
              /* ------------------------------------------ */
              uint64_t currentTime = time(NULL);
              NAEEM_os__write_to_file (
                (NAEEM_path)(workDir + "/pnat").c_str(),
                (NAEEM_string)ss.str().c_str(),
                (NAEEM_data)&currentTime,
                sizeof(currentTime)
              );
              if (Runtime::poppedButNotAcked_.find(popLabel_) == Runtime::poppedButNotAcked_.end()) {
                Runtime::poppedButNotAcked_.insert(
                  std::pair<std::string, std::map<uint64_t, uint64_t>*>(
                    popLabel_, new std::map<uint64_t, uint64_t>()));
              }
              (*(Runtime::poppedButNotAcked_[popLabel_]))[it->first] = currentTime;
            }
          }
        }
      }
      if (Runtime::received_.find(popLabel_) == Runtime::received_.end()) {
        return messages;
      }
      if (Runtime::received_[popLabel_]->size() == 0) {
        return messages;
      }
      std::deque<uint64_t> receivedIds = std::move(*(Runtime::received_[popLabel_]));
      for (uint64_t i = 0; i < receivedIds.size(); i++) {
        std::stringstream ss;
        ss << receivedIds[i];
        NAEEM_data data;
        NAEEM_length dataLength;
        NAEEM_os__read_file_with_path (
          (NAEEM_path)(workDir + "/r").c_str(),
          (NAEEM_string)ss.str().c_str(),
          &data,
          &dataLength
        );
        ::ir::ntnaeem::gate::Message message;
        message.Deserialize(data, dataLength);
        free(data);
        /* ------------------------------------------ */
        ::naeem::gate::client::Message *clientMessage = 
          new ::naeem::gate::client::Message;
        clientMessage->SetId (receivedIds[i]);
        if (message.GetRelId().GetValue() != 0) {
          std::stringstream crss;
          crss << message.GetRelId().GetValue();
          uint64_t clientRelId;
          NAEEM_os__read_file3 (
            (NAEEM_path)(workDir + "/s/" + crss.str() + ".cid").c_str(),
            (NAEEM_data)&clientRelId,
            0
          );
          clientMessage->SetRelId (clientRelId);
        } else {
          clientMessage->SetRelId (0);
        }
        clientMessage->SetLabel(popLabel_);
        clientMessage->SetContent (
          ByteArray (
            message.GetContent().GetValue(), 
            message.GetContent().GetLength()
          )
        );
        messages.push_back(clientMessage);
        /* ------------------------------------------ */
        NAEEM_os__copy_file (
          (NAEEM_path)(workDir + "/r").c_str(),
          (NAEEM_string)ss.str().c_str(),
          (NAEEM_path)(workDir + "/a").c_str(),
          (NAEEM_string)ss.str().c_str()
        );
        NAEEM_os__move_file (
          (NAEEM_path)(workDir + "/r").c_str(),
          (NAEEM_string)ss.str().c_str(),
          (NAEEM_path)(workDir + "/pna").c_str(),
          (NAEEM_string)ss.str().c_str()
        );
        uint64_t currentTime = time(NULL);
        NAEEM_os__write_to_file (
          (NAEEM_path)(workDir + "/pnat").c_str(),
          (NAEEM_string)ss.str().c_str(),
          (NAEEM_data)&currentTime,
          sizeof(currentTime)
        );
        if (Runtime::poppedButNotAcked_.find(popLabel_) == Runtime::poppedButNotAcked_.end()) {
          Runtime::poppedButNotAcked_.insert(
            std::pair<std::string, std::map<uint64_t, uint64_t>*>(
              popLabel_, new std::map<uint64_t, uint64_t>()));
        }
        (*(Runtime::poppedButNotAcked_[popLabel_]))[receivedIds[i]] = currentTime;
      }
    }
    return messages;
  }
  void
  SimpleGateClient::Ack (
    std::vector<uint64_t> ids
  ) {
    std::string workDir = ::naeem::conf::ConfigManager::GetValueAsString("gate-client", "work_dir");
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      for (uint32_t i = 0; i < ids.size(); i++) {
        uint64_t messageId = ids[i];
        std::stringstream ss;
        ss << messageId;
        if (NAEEM_os__file_exists (
              (NAEEM_path)(workDir + "/pna").c_str(), 
              (NAEEM_string)ss.str().c_str()
            )
        ) {
          NAEEM_data data;
          NAEEM_length dataLength;
          NAEEM_os__read_file_with_path (
            (NAEEM_path)(workDir + "/pna").c_str(),
            (NAEEM_string)ss.str().c_str(),
            &data,
            &dataLength
          );
          ::ir::ntnaeem::gate::Message message;
          message.Deserialize(data, dataLength);
          free(data);
          if (Runtime::poppedButNotAcked_.find(popLabel_) 
                != Runtime::poppedButNotAcked_.end()) {
            Runtime::poppedButNotAcked_[popLabel_]->erase(messageId);
          }
          NAEEM_os__move_file (
            (NAEEM_path)(workDir + "/pna").c_str(),
            (NAEEM_string)ss.str().c_str(),
            (NAEEM_path)(workDir + "/pa").c_str(),
            (NAEEM_string)ss.str().c_str()
          );
          uint64_t currentTime = time(NULL);
          NAEEM_os__write_to_file (
            (NAEEM_path)(workDir + "/pat").c_str(),
            (NAEEM_string)ss.str().c_str(),
            (NAEEM_data)&currentTime,
            sizeof(currentTime)
          );
        }
      }
    }
  }
}
}
}