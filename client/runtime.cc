#include <thread>
#include <chrono>
#include <iostream>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>

#include <naeem/os.h>

#include <naeem++/conf/config_manager.h>
#include <naeem++/date/helper.h>

#include <gate/message.h>

#include <naeem/gate/client/runtime.h>


namespace naeem {
namespace gate {
namespace client {

  // bool initialized_ = false;
  // bool termSignal_ = false;
  // bool submitterThreadTerminated_ = false;
  // bool receiverThreadTerminated_ = false;

  // std::mutex termSignalLock_;
  // std::mutex messageIdCounterLock_;
  // std::mutex mainLock_;

  // uint64_t messageIdCounter_;
  // std::deque<uint64_t> enqueued_;
  // std::map<std::string, std::deque<uint64_t>*> received_;
  // std::map<std::string, std::map<uint64_t, uint64_t>*> poppedButNotAcked_;

  static std::map<std::string, Runtime*> Runtime::runtimes_;

  void
  Runtime::Init(std::string workDirPath, int agrc, char **argv) {
    if (initialized_) {
      return;
    }
    messageIdCounter_ = 10;
    /*
     * Make directories
     */
    if (!NAEEM_os__dir_exists((NAEEM_path)workDirPath.c_str())) {
      NAEEM_os__mkdir((NAEEM_path)workDirPath.c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDirPath + "/e").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDirPath + "/e").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDirPath + "/a").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDirPath + "/a").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDirPath + "/s").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDirPath + "/s").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDirPath + "/r").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDirPath + "/r").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDirPath + "/ra").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDirPath + "/ra").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDirPath + "/pna").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDirPath + "/pna").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDirPath + "/pnat").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDirPath + "/pnat").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDirPath + "/pa").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDirPath + "/pa").c_str());
    }
    /*
     * Reading message id counter file
     */
    NAEEM_data temp;
    NAEEM_length tempLength;
    if (NAEEM_os__file_exists((NAEEM_path)workDirPath.c_str(), (NAEEM_string)"mco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDirPath.c_str(), 
        (NAEEM_string)"mco",
        &temp, 
        &tempLength
      );
      NAEEM_data ptr = (NAEEM_data)&(messageIdCounter_);
      for (uint32_t i = 0; i < sizeof(messageIdCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " <<
          "Last Message Id Counter value is " << messageIdCounter_ << std::endl;
      free(temp);
    } else {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " <<
          "Message Id Counter is set to " << messageIdCounter_ << std::endl;
    }
    /*
     * Reading waiting messages
     */
    NAEEM_string_ptr filenames;
    NAEEM_length filenamesLength;
    NAEEM_os__enum_file_names (
      (NAEEM_path)(workDirPath + "/e").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      enqueued_.push_back(messageId);
    }
    NAEEM_os__free_file_names(filenames, filenamesLength);
    /*
     * Reading received messages
     */
    NAEEM_os__enum_file_names (
      (NAEEM_path)(workDirPath + "/r").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      NAEEM_data data;
      NAEEM_length dataLength;
      NAEEM_os__read_file_with_path (
        (NAEEM_path)(workDirPath + "/r").c_str(), 
        (NAEEM_string)filenames[i],
        &data, 
        &dataLength
      );
      ::ir::ntnaeem::gate::Message message;
      message.Deserialize(data, dataLength);
      free(data);
      if (received_.find(message.GetLabel().ToStdString()) == 
            received_.end()) {
        received_.insert(std::pair<std::string, std::deque<uint64_t>*>
          (message.GetLabel().ToStdString(), new std::deque<uint64_t>()));
      }
      received_[message.GetLabel().ToStdString()]->push_back(messageId);
    }
    NAEEM_os__free_file_names(filenames, filenamesLength);
    /*
     * Reading popped but not acked messages
     */
    NAEEM_os__enum_file_names (
      (NAEEM_path)(workDirPath + "/pna").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      NAEEM_data data;
      NAEEM_length dataLength;
      NAEEM_os__read_file_with_path (
        (NAEEM_path)(workDirPath + "/pna").c_str(), 
        (NAEEM_string)filenames[i],
        &data, 
        &dataLength
      );
      uint64_t popTime = 0;
      NAEEM_os__read_file3 (
        (NAEEM_path)(workDirPath + "/pnat/" + filenames[i]).c_str(),
        (NAEEM_data)(&popTime),
        0
      );
      ::ir::ntnaeem::gate::Message message;
      message.Deserialize(data, dataLength);
      free(data);
      if (poppedButNotAcked_.find(message.GetLabel().ToStdString()) == 
            poppedButNotAcked_.end()) {
        poppedButNotAcked_.insert(std::pair<std::string, std::map<uint64_t, uint64_t>*>
          (message.GetLabel().ToStdString(), new std::map<uint64_t, uint64_t>()));
      }
      (*(poppedButNotAcked_[message.GetLabel().ToStdString()]))[messageId] = popTime;
    }
    NAEEM_os__free_file_names(filenames, filenamesLength);
    initialized_ = true;
  }
  void
  Runtime::Shutdown() {
    if (!initialized_) {
      return;
    }
    /*{
      std::lock_guard<std::mutex> guard(termSignalLock_);
      termSignal_ = true;
    }
    ::naeem::hottentot::runtime::Logger::GetOut() << 
      "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " <<
        "Waiting for submitter thread to exit ..." << std::endl;
    while (true) {
      std::lock_guard<std::mutex> guard(termSignalLock_);
      if (submitterThreadTerminated_) {
        ::naeem::hottentot::runtime::Logger::GetOut() << 
          "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " <<
            "Submitter thread exited." << std::endl;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }*/
    for (std::map<std::string, std::deque<uint64_t>*>::iterator it = received_.begin();
         it != received_.end();
        ) {
      delete it->second;
      received_.erase(it++);
    }
    for (std::map<std::string, std::map<uint64_t, uint64_t>*>::iterator it = poppedButNotAcked_.begin();
         it != poppedButNotAcked_.end();
        ) {
      delete it->second;
      poppedButNotAcked_.erase(it++);
    }
    initialized_ = false;
  }
}
}
}

