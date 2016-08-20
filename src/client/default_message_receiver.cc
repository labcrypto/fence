#include <sstream>
#include <chrono>
#include <thread>

#include <org/labcrypto/abettor/fs.h>

#include <org/labcrypto/abettor++/conf/config_manager.h>
#include <org/labcrypto/abettor++/date/helper.h>

#include <org/labcrypto/hottentot/runtime/configuration.h>
#include <org/labcrypto/hottentot/runtime/logger.h>
#include <org/labcrypto/hottentot/runtime/utils.h>
#include <org/labcrypto/hottentot/runtime/proxy/proxy.h>
#include <org/labcrypto/hottentot/runtime/proxy/proxy_runtime.h>

#include <fence/message.h>

#include <org/labcrypto/fence/client/default_message_receiver.h>
#include <org/labcrypto/fence/client/runtime.h>


namespace org {
namespace labcrypto {
namespace fence {
namespace client {
  void 
  DefaultMessageReceiver::Init (
    int argc, 
    char **argv
  ) {
    ::org::labcrypto::hottentot::runtime::Logger::Init();
    ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " <<
          "Proxy runtime is initialized." << std::endl;
    }
    Runtime::RegisterWorkDirPath(workDirPath_);
    runtime_ = Runtime::GetRuntime(workDirPath_);
    runtime_->Init(workDirPath_, argc, argv);
    runtime_->Init(workDirPath_, argc, argv);
    receiverThread_ = new ReceiverThread(fenceHost_, fencePort_, popLabel_, workDirPath_, runtime_);
    receiverThread_->Start();
  }
  void 
  DefaultMessageReceiver::Shutdown() {
    receiverThread_->Shutdown();
    delete receiverThread_;
    runtime_->Shutdown();
  }
  std::vector<Message*>
  DefaultMessageReceiver::GetMessages () {
    std::vector<Message*> messages;
    {
      std::lock_guard<std::mutex> guard(runtime_->mainLock_);
      if (runtime_->poppedButNotAcked_.find(popLabel_) != runtime_->poppedButNotAcked_.end()) {
        if (runtime_->poppedButNotAcked_[popLabel_]->size() > 0) {
          for (std::map<uint64_t, uint64_t>::iterator it = runtime_->poppedButNotAcked_[popLabel_]->begin();
               it != runtime_->poppedButNotAcked_[popLabel_]->end();
               it++) {
            uint64_t currentTime = time(NULL);
            if ((currentTime - it->second) > ackTimeout_) {
              std::stringstream ss;
              ss << it->first;
              ORG_LABCRYPTO_ABETTOR_data data;
              ORG_LABCRYPTO_ABETTOR_length dataLength;
              ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
                (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/pna").c_str(),
                (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                &data,
                &dataLength
              );
              ::org::labcrypto::fence::Message message;
              message.Deserialize(data, dataLength);
              free(data);
              /* ------------------------------------------ */
              ::org::labcrypto::fence::client::Message *clientMessage = 
                new ::org::labcrypto::fence::client::Message;
              clientMessage->SetId (it->first);
              if (message.GetRelId().GetValue() != 0) {
                std::stringstream crss;
                crss << message.GetRelId().GetValue();
                uint64_t clientRelId;
                if (ORG_LABCRYPTO_ABETTOR__fs__file_exists (
                      (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/s").c_str(),
                      (ORG_LABCRYPTO_ABETTOR_string)(crss.str() + ".cid").c_str()
                    )
                ) {
                  ORG_LABCRYPTO_ABETTOR__fs__read_file_into_buffer (
                    (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/s/" + crss.str() + ".cid").c_str(),
                    (ORG_LABCRYPTO_ABETTOR_data)&clientRelId,
                    0
                  );
                  clientMessage->SetRelId (clientRelId);
                } else {
                  clientMessage->SetRelId (0);
                }
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
              ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/pnat").c_str(),
                (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                (ORG_LABCRYPTO_ABETTOR_data)&currentTime,
                sizeof(currentTime)
              );
              if (runtime_->poppedButNotAcked_.find(popLabel_) == runtime_->poppedButNotAcked_.end()) {
                runtime_->poppedButNotAcked_.insert(
                  std::pair<std::string, std::map<uint64_t, uint64_t>*>(
                    popLabel_, new std::map<uint64_t, uint64_t>()));
              }
              (*(runtime_->poppedButNotAcked_[popLabel_]))[it->first] = currentTime;
            }
          }
        }
      }
      if (runtime_->received_.find(popLabel_) == runtime_->received_.end()) {
        return messages;
      }
      if (runtime_->received_[popLabel_]->size() == 0) {
        return messages;
      }
      std::deque<uint64_t> receivedIds = std::move(*(runtime_->received_[popLabel_]));
      for (uint64_t i = 0; i < receivedIds.size(); i++) {
        std::stringstream ss;
        ss << receivedIds[i];
        ORG_LABCRYPTO_ABETTOR_data data;
        ORG_LABCRYPTO_ABETTOR_length dataLength;
        ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
          (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/r").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          &data,
          &dataLength
        );
        ::org::labcrypto::fence::Message message;
        message.Deserialize(data, dataLength);
        free(data);
        /* ------------------------------------------ */
        ::org::labcrypto::fence::client::Message *clientMessage = 
          new ::org::labcrypto::fence::client::Message;
        clientMessage->SetId (receivedIds[i]);
        if (message.GetRelId().GetValue() != 0) {
          std::stringstream crss;
          crss << message.GetRelId().GetValue();
          uint64_t clientRelId;
          if (ORG_LABCRYPTO_ABETTOR__fs__file_exists (
                (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/s").c_str(),
                (ORG_LABCRYPTO_ABETTOR_string)(crss.str() + ".cid").c_str()
              )
          ) {
            ORG_LABCRYPTO_ABETTOR__fs__read_file_into_buffer (
              (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/s/" + crss.str() + ".cid").c_str(),
              (ORG_LABCRYPTO_ABETTOR_data)&clientRelId,
              0
            );
            clientMessage->SetRelId (clientRelId);
          } else {
            clientMessage->SetRelId (0);
          }
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
        ORG_LABCRYPTO_ABETTOR__fs__copy_file (
          (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/r").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/a").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
        );
        ORG_LABCRYPTO_ABETTOR__fs__move_file (
          (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/r").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/pna").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
        );
        uint64_t currentTime = time(NULL);
        ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
          (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/pnat").c_str(),
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
          (ORG_LABCRYPTO_ABETTOR_data)&currentTime,
          sizeof(currentTime)
        );
        if (runtime_->poppedButNotAcked_.find(popLabel_) == runtime_->poppedButNotAcked_.end()) {
          runtime_->poppedButNotAcked_.insert(
            std::pair<std::string, std::map<uint64_t, uint64_t>*>(
              popLabel_, new std::map<uint64_t, uint64_t>()));
        }
        (*(runtime_->poppedButNotAcked_[popLabel_]))[receivedIds[i]] = currentTime;
      }
    }
    return messages;
  }
  void
  DefaultMessageReceiver::Ack (
    std::vector<uint64_t> ids
  ) {
    {
      std::lock_guard<std::mutex> guard(runtime_->mainLock_);
      for (uint32_t i = 0; i < ids.size(); i++) {
        uint64_t messageId = ids[i];
        std::stringstream ss;
        ss << messageId;
        if (ORG_LABCRYPTO_ABETTOR__fs__file_exists (
              (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/pna").c_str(), 
              (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
            )
        ) {
          ORG_LABCRYPTO_ABETTOR_data data;
          ORG_LABCRYPTO_ABETTOR_length dataLength;
          ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
            (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/pna").c_str(),
            (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
            &data,
            &dataLength
          );
          ::org::labcrypto::fence::Message message;
          message.Deserialize(data, dataLength);
          free(data);
          if (runtime_->poppedButNotAcked_.find(popLabel_) 
                != runtime_->poppedButNotAcked_.end()) {
            runtime_->poppedButNotAcked_[popLabel_]->erase(messageId);
          }
          ORG_LABCRYPTO_ABETTOR__fs__move_file (
            (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/pna").c_str(),
            (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
            (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/pa").c_str(),
            (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
          );
          uint64_t currentTime = time(NULL);
          ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
            (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/pnat").c_str(),
            (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
            (ORG_LABCRYPTO_ABETTOR_data)&currentTime,
            sizeof(currentTime)
          );
        }
      }
    }
  }
} // END NAMESAPCE client
} // END NAMESPACE fence
} // END NAMESPACE labcrypto
} // END NAMESPACE org