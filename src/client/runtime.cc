#include <thread>
#include <chrono>
#include <iostream>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>

#include <naeem/os.h>

#include <naeem++/conf/config_manager.h>

#include <naeem/gate/client/runtime.h>
#include <naeem/gate/client/submitter_thread.h>


namespace naeem {
namespace gate {
namespace client {

  bool Runtime::termSignal_;
  bool Runtime::submitterThreadTerminated_;

  std::mutex Runtime::termSignalLock_;
  std::mutex Runtime::messageIdCounterLock_;
  std::mutex Runtime::mainLock_;

  uint64_t Runtime::messageIdCounter_;
  std::vector<uint64_t> Runtime::waiting_;

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
      std::cout << "ERROR: Value 'gate-client.work_dir' is not found in configurations." << std::endl;
      exit(1);
    }
    Runtime::messageIdCounter_ = 10;
    std::string workDir = ::naeem::conf::ConfigManager::GetValueAsString("gate-client", "work_dir");
    /*
     * Make directories
     */
    if (!NAEEM_os__dir_exists((NAEEM_path)workDir.c_str())) {
      NAEEM_os__mkdir((NAEEM_path)workDir.c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir + "/w").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir + "/w").c_str());
    }
    if (!NAEEM_os__dir_exists((NAEEM_path)(workDir + "/s").c_str())) {
      NAEEM_os__mkdir((NAEEM_path)(workDir + "/s").c_str());
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
      (NAEEM_path)(workDir + "/w").c_str(),
      &filenames,
      &filenamesLength
    );
    for (uint32_t i = 0; i < filenamesLength; i++) {
      uint64_t messageId = atoll(filenames[i]);
      Runtime::waiting_.push_back(messageId);
    }
    /*
     * Starting submitter thread
     */
    SubmitterThread::Start();
  }
  void
  Runtime::Shutdown() {
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
  }
}
}
}

