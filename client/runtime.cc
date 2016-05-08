#include <thread>
#include <chrono>
#include <iostream>

#include <org/labcrypto/hottentot/runtime/configuration.h>
#include <org/labcrypto/hottentot/runtime/logger.h>
#include <org/labcrypto/hottentot/runtime/utils.h>

#include <org/labcrypto/abettor/fs.h>

#include <org/labcrypto/abettor++/conf/config_manager.h>
#include <org/labcrypto/abettor++/date/helper.h>

#include <fence/message.h>

#include <org/labcrypto/fence/client/runtime.h>


namespace org {
namespace labcrypto {
namespace fence {
namespace client {

  std::map<std::string, Runtime*> Runtime::runtimes_;

  void
  Runtime::Init(std::string workDirPath, int agrc, char **argv) {
    if (initialized_) {
      return;
    }
    messageIdCounter_ = 10;
    /*
     * Make directories
     */
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)workDirPath.c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)workDirPath.c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/e").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/e").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/a").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/a").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/s").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/s").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/r").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/r").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/ra").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/ra").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/pna").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/pna").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/pnat").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/pnat").c_str());
    }
    if (!ORG_LABCRYPTO_ABETTOR__fs__dir_exists((ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/pa").c_str())) {
      ORG_LABCRYPTO_ABETTOR__fs__mkdir((ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/pa").c_str());
    }
    /*
     * Reading message id counter file
     */
    ORG_LABCRYPTO_ABETTOR_data temp;
    ORG_LABCRYPTO_ABETTOR_length tempLength;
    if (ORG_LABCRYPTO_ABETTOR__fs__file_exists((ORG_LABCRYPTO_ABETTOR_path)workDirPath.c_str(), (ORG_LABCRYPTO_ABETTOR_string)"mco")) {
      ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
        (ORG_LABCRYPTO_ABETTOR_path)workDirPath.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"mco",
        &temp, 
        &tempLength
      );
      ORG_LABCRYPTO_ABETTOR_data ptr = (ORG_LABCRYPTO_ABETTOR_data)&(messageIdCounter_);
      for (uint32_t i = 0; i < sizeof(messageIdCounter_); i++) {
        ptr[i] = temp[i];
      }
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " <<
          "Last Message Id Counter value is " << messageIdCounter_ << std::endl;
      free(temp);
    } else {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " <<
          "Message Id Counter is set to " << messageIdCounter_ << std::endl;
    }
    /*
     * Reading waiting messages
     */
    ORG_LABCRYPTO_ABETTOR_string_ptr filenames;
    ORG_LABCRYPTO_ABETTOR_length filenamesLength;
    ORG_LABCRYPTO_ABETTOR__fs__enum_file_names (
      (ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/e").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      enqueued_.push_back(messageId);
    }
    ORG_LABCRYPTO_ABETTOR__fs__free_file_names(filenames, filenamesLength);
    /*
     * Reading received messages
     */
    ORG_LABCRYPTO_ABETTOR__fs__enum_file_names (
      (ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/r").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      ORG_LABCRYPTO_ABETTOR_data data;
      ORG_LABCRYPTO_ABETTOR_length dataLength;
      ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
        (ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/r").c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)filenames[i],
        &data, 
        &dataLength
      );
      ::org::labcrypto::fence::Message message;
      message.Deserialize(data, dataLength);
      free(data);
      if (received_.find(message.GetLabel().ToStdString()) == 
            received_.end()) {
        received_.insert(std::pair<std::string, std::deque<uint64_t>*>
          (message.GetLabel().ToStdString(), new std::deque<uint64_t>()));
      }
      received_[message.GetLabel().ToStdString()]->push_back(messageId);
    }
    ORG_LABCRYPTO_ABETTOR__fs__free_file_names(filenames, filenamesLength);
    /*
     * Reading popped but not acked messages
     */
    ORG_LABCRYPTO_ABETTOR__fs__enum_file_names (
      (ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/pna").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      ORG_LABCRYPTO_ABETTOR_data data;
      ORG_LABCRYPTO_ABETTOR_length dataLength;
      ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
        (ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/pna").c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)filenames[i],
        &data, 
        &dataLength
      );
      uint64_t popTime = 0;
      ORG_LABCRYPTO_ABETTOR__fs__read_file_into_buffer (
        (ORG_LABCRYPTO_ABETTOR_path)(workDirPath + "/pnat/" + filenames[i]).c_str(),
        (ORG_LABCRYPTO_ABETTOR_data)(&popTime),
        0
      );
      ::org::labcrypto::fence::Message message;
      message.Deserialize(data, dataLength);
      free(data);
      if (poppedButNotAcked_.find(message.GetLabel().ToStdString()) == 
            poppedButNotAcked_.end()) {
        poppedButNotAcked_.insert(std::pair<std::string, std::map<uint64_t, uint64_t>*>
          (message.GetLabel().ToStdString(), new std::map<uint64_t, uint64_t>()));
      }
      (*(poppedButNotAcked_[message.GetLabel().ToStdString()]))[messageId] = popTime;
    }
    ORG_LABCRYPTO_ABETTOR__fs__free_file_names(filenames, filenamesLength);
    initialized_ = true;
  }
  void
  Runtime::Shutdown() {
    if (!initialized_) {
      return;
    }
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
} // END NAMESAPCE client
} // END NAMESPACE fence
} // END NAMESPACE labcrypto
} // END NAMESPACE org