#include <thread>
#include <chrono>
#include <iostream>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>

#include <naeem/os.h>

#include <naeem++/conf/config_manager.h>

#include <gate/message.h>

#include <naeem/gate/client/runtime.h>


namespace naeem {
namespace gate {
namespace client {

  bool Runtime::initialized_ = false;
  bool Runtime::termSignal_;
  bool Runtime::submitterThreadTerminated_;

  std::mutex Runtime::termSignalLock_;
  std::mutex Runtime::messageIdCounterLock_;
  std::mutex Runtime::mainLock_;

  uint64_t Runtime::messageIdCounter_;
  std::deque<uint64_t> Runtime::enqueued_;
  std::map<std::string, std::deque<uint64_t>*> Runtime::received_;
  std::map<std::string, std::map<uint64_t, uint64_t>*> Runtime::poppedButNotAcked_;

  void
  Runtime::Init(int agrc, char **argv) {
    if (!::naeem::conf::ConfigManager::HasValue("gate-client", "host")) {
      std::cout << "ERROR: Value 'gate-client.host' is not found in configurations." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("gate-client", "port")) {
      std::cout << "ERROR: Value 'gate-client.port' is not found in configurations." << std::endl;
      exit(1);
    }
    if (!::naeem::conf::ConfigManager::HasValue("gate-client", "work_dir")) {
      std::cout << "(1) ERROR: Value 'gate-client.work_dir' is not found in configurations." << std::endl;
      exit(1);
    }
    Runtime::messageIdCounter_ = 10;
    std::string workDir = ::naeem::conf::ConfigManager::GetValueAsString("gate-client", "work_dir");
    if (initialized_) {
      return;
    }
    /*
     * Make directories
     */
    if (!NAEEM_os__dir_exists((NAEEM_path)workDir.c_str())) {
      NAEEM_os__mkdir((NAEEM_path)workDir.c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir + "/e").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir + "/e").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir + "/a").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir + "/a").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir + "/s").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir + "/s").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir + "/r").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir + "/r").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir + "/ra").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir + "/ra").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir + "/pna").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir + "/pna").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir + "/pnat").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir + "/pnat").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir + "/pa").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir + "/pa").c_str());
    }
    /*
     * Reading message id counter file
     */
    NAEEM_data temp;
    NAEEM_length tempLength;
    if (NAEEM_os__file_exists((NAEEM_path)workDir.c_str(), (NAEEM_string)"mco")) {
      NAEEM_os__read_file_with_path (
        (NAEEM_path)workDir.c_str(), 
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
     * Reading waiting messages
     */
    NAEEM_string_ptr filenames;
    NAEEM_length filenamesLength;
    NAEEM_os__enum_file_names (
      (NAEEM_path)(workDir + "/e").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      Runtime::enqueued_.push_back(messageId);
    }
    NAEEM_os__free_file_names(filenames, filenamesLength);
    /*
     * Reading received messages
     */
    NAEEM_os__enum_file_names (
      (NAEEM_path)(workDir + "/r").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      NAEEM_data data;
      NAEEM_length dataLength;
      NAEEM_os__read_file_with_path (
        (NAEEM_path)(workDir + "/r").c_str(), 
        (NAEEM_string)filenames[i],
        &data, 
        &dataLength
      );
      ::ir::ntnaeem::gate::Message message;
      message.Deserialize(data, dataLength);
      free(data);
      if (Runtime::received_.find(message.GetLabel().ToStdString()) == 
            Runtime::received_.end()) {
        Runtime::received_.insert(std::pair<std::string, std::deque<uint64_t>*>
          (message.GetLabel().ToStdString(), new std::deque<uint64_t>()));
      }
      Runtime::received_[message.GetLabel().ToStdString()]->push_back(messageId);
    }
    NAEEM_os__free_file_names(filenames, filenamesLength);
    /*
     * Reading popped but not acked messages
     */
    NAEEM_os__enum_file_names (
      (NAEEM_path)(workDir + "/pna").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      NAEEM_data data;
      NAEEM_length dataLength;
      NAEEM_os__read_file_with_path (
        (NAEEM_path)(workDir + "/pna").c_str(), 
        (NAEEM_string)filenames[i],
        &data, 
        &dataLength
      );
      uint64_t popTime = 0;
      NAEEM_os__read_file3 (
        (NAEEM_path)(workDir + "/pnat/" + filenames[i]).c_str(),
        (NAEEM_data)(&popTime),
        0
      );
      ::ir::ntnaeem::gate::Message message;
      message.Deserialize(data, dataLength);
      free(data);
      if (Runtime::poppedButNotAcked_.find(message.GetLabel().ToStdString()) == 
            Runtime::poppedButNotAcked_.end()) {
        Runtime::poppedButNotAcked_.insert(std::pair<std::string, std::map<uint64_t, uint64_t>*>
          (message.GetLabel().ToStdString(), new std::map<uint64_t, uint64_t>()));
      }
      (*(Runtime::poppedButNotAcked_[message.GetLabel().ToStdString()]))[messageId] = popTime;
    }
    NAEEM_os__free_file_names(filenames, filenamesLength);
    initialized_ = true;
  }
  void
  Runtime::Shutdown() {
    if (!initialized_) {
      return;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
      Runtime::termSignal_ = true;
    }
    ::naeem::hottentot::runtime::Logger::GetOut() << "Waiting for submitter thread to exit ..." << std::endl;
    while (true) {
      std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
      if (Runtime::submitterThreadTerminated_) {
        ::naeem::hottentot::runtime::Logger::GetOut() << "Submitter thread exited." << std::endl;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    for (std::map<std::string, std::deque<uint64_t>*>::iterator it = Runtime::received_.begin();
         it != Runtime::received_.end();
        ) {
      delete it->second;
      Runtime::received_.erase(it++);
    }
    for (std::map<std::string, std::map<uint64_t, uint64_t>*>::iterator it = Runtime::poppedButNotAcked_.begin();
         it != Runtime::poppedButNotAcked_.end();
        ) {
      delete it->second;
      Runtime::poppedButNotAcked_.erase(it++);
    }
  }
}
}
}

