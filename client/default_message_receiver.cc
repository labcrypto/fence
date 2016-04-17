#include <sstream>
#include <chrono>
#include <thread>

#include <naeem/os.h>

#include <naeem++/conf/config_manager.h>
#include <naeem++/date/helper.h>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>
#include <naeem/hottentot/runtime/proxy/proxy.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include <gate/message.h>

#include <naeem/gate/client/default_message_receiver.h>
#include <naeem/gate/client/runtime.h>


namespace naeem {
namespace gate {
namespace client {
  void 
  DefaultMessageReceiver::Init (
    int argc, 
    char **argv
  ) {
    ::naeem::hottentot::runtime::Logger::Init();
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " <<
          "Proxy runtime is initialized." << std::endl;
    }
    Runtime::RegisterWorkDirPath(workDirPath_);
    runtime_ = Runtime::GetRuntime(workDirPath_);
    runtime_->Init(workDirPath_, argc, argv);
    runtime_->Init(workDirPath_, argc, argv);
    receiverThread_ = new ReceiverThread(gateHost_, gatePort_, popLabel_, workDirPath_, runtime_);
    receiverThread_->Start();
  }
  void 
  DefaultMessageReceiver::Shutdown() {
    receiverThread_->Shutdown();
    delete receiverThread_;
    runtime_->Shutdown();
    // delete runtime_;
    /* ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::naeem::hottentot::runtime::Logger::Shutdown(); */
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
              NAEEM_data data;
              NAEEM_length dataLength;
              NAEEM_os__read_file_with_path (
                (NAEEM_path)(workDirPath_ + "/pna").c_str(),
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
                if (NAEEM_os__file_exists (
                      (NAEEM_path)(workDirPath_ + "/s").c_str(),
                      (NAEEM_string)(crss.str() + ".cid").c_str()
                    )
                ) {
                  NAEEM_os__read_file3 (
                    (NAEEM_path)(workDirPath_ + "/s/" + crss.str() + ".cid").c_str(),
                    (NAEEM_data)&clientRelId,
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
              NAEEM_os__write_to_file (
                (NAEEM_path)(workDirPath_ + "/pnat").c_str(),
                (NAEEM_string)ss.str().c_str(),
                (NAEEM_data)&currentTime,
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
        NAEEM_data data;
        NAEEM_length dataLength;
        NAEEM_os__read_file_with_path (
          (NAEEM_path)(workDirPath_ + "/r").c_str(),
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
          if (NAEEM_os__file_exists (
                (NAEEM_path)(workDirPath_ + "/s").c_str(),
                (NAEEM_string)(crss.str() + ".cid").c_str()
              )
          ) {
            NAEEM_os__read_file3 (
              (NAEEM_path)(workDirPath_ + "/s/" + crss.str() + ".cid").c_str(),
              (NAEEM_data)&clientRelId,
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
        NAEEM_os__copy_file (
          (NAEEM_path)(workDirPath_ + "/r").c_str(),
          (NAEEM_string)ss.str().c_str(),
          (NAEEM_path)(workDirPath_ + "/a").c_str(),
          (NAEEM_string)ss.str().c_str()
        );
        NAEEM_os__move_file (
          (NAEEM_path)(workDirPath_ + "/r").c_str(),
          (NAEEM_string)ss.str().c_str(),
          (NAEEM_path)(workDirPath_ + "/pna").c_str(),
          (NAEEM_string)ss.str().c_str()
        );
        uint64_t currentTime = time(NULL);
        NAEEM_os__write_to_file (
          (NAEEM_path)(workDirPath_ + "/pnat").c_str(),
          (NAEEM_string)ss.str().c_str(),
          (NAEEM_data)&currentTime,
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
        if (NAEEM_os__file_exists (
              (NAEEM_path)(workDirPath_ + "/pna").c_str(), 
              (NAEEM_string)ss.str().c_str()
            )
        ) {
          NAEEM_data data;
          NAEEM_length dataLength;
          NAEEM_os__read_file_with_path (
            (NAEEM_path)(workDirPath_ + "/pna").c_str(),
            (NAEEM_string)ss.str().c_str(),
            &data,
            &dataLength
          );
          ::ir::ntnaeem::gate::Message message;
          message.Deserialize(data, dataLength);
          free(data);
          if (runtime_->poppedButNotAcked_.find(popLabel_) 
                != runtime_->poppedButNotAcked_.end()) {
            runtime_->poppedButNotAcked_[popLabel_]->erase(messageId);
          }
          NAEEM_os__move_file (
            (NAEEM_path)(workDirPath_ + "/pna").c_str(),
            (NAEEM_string)ss.str().c_str(),
            (NAEEM_path)(workDirPath_ + "/pa").c_str(),
            (NAEEM_string)ss.str().c_str()
          );
          uint64_t currentTime = time(NULL);
          NAEEM_os__write_to_file (
            (NAEEM_path)(workDirPath_ + "/pnat").c_str(),
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