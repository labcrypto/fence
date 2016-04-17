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

#include <naeem/gate/client/default_message_submitter.h>
#include <naeem/gate/client/runtime.h>


namespace naeem {
namespace gate {
namespace client {
  void 
  DefaultMessageSubmitter::Init (
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
    submitterThread_ = new SubmitterThread(gateHost_, gatePort_, enqueueLabel_, workDirPath_,runtime_);
    submitterThread_->Start();
  }
  void 
  DefaultMessageSubmitter::Shutdown() {
    submitterThread_->Shutdown();
    delete submitterThread_;
    runtime_->Shutdown();
    // delete runtime_;
    /* ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::naeem::hottentot::runtime::Logger::Shutdown(); */
  }
  uint64_t 
  DefaultMessageSubmitter::SubmitMessage (
    unsigned char *data, 
    uint32_t length
  ) {
    uint64_t messageId;
    {
      std::lock_guard<std::mutex> guard(runtime_->messageIdCounterLock_);
      messageId = runtime_->messageIdCounter_;
      runtime_->messageIdCounter_++;
      NAEEM_os__write_to_file (
        (NAEEM_path)workDirPath_.c_str(), 
        (NAEEM_string)"mco", 
        (NAEEM_data)&(runtime_->messageIdCounter_), 
        (NAEEM_length)sizeof(runtime_->messageIdCounter_)
      );
    }
    std::stringstream ss;
    ss << messageId;
    {
      std::lock_guard<std::mutex> guard(runtime_->mainLock_);
      ::ir::ntnaeem::gate::Message message;
      try {
        message.SetId(0);
        message.SetLabel(enqueueLabel_);
        message.SetRelId(0);
        message.SetContent(::naeem::hottentot::runtime::types::ByteArray(data, length));
        NAEEM_length dataLength;
        NAEEM_data data = message.Serialize(&dataLength);
        NAEEM_os__write_to_file (
          (NAEEM_path)(workDirPath_ + "/e").c_str(), 
          (NAEEM_string)ss.str().c_str(), 
          data, 
          dataLength
        );
        delete [] data;
        runtime_->enqueued_.push_back(messageId);
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
  uint64_t 
  DefaultMessageSubmitter::ReplyMessage (
    uint64_t sourceMessageId,
    unsigned char *data, 
    uint32_t length
  ) {
    uint64_t messageId;
    {
      std::lock_guard<std::mutex> guard(runtime_->messageIdCounterLock_);
      messageId = runtime_->messageIdCounter_;
      runtime_->messageIdCounter_++;
      NAEEM_os__write_to_file (
        (NAEEM_path)workDirPath_.c_str(), 
        (NAEEM_string)"mco", 
        (NAEEM_data)&(runtime_->messageIdCounter_), 
        (NAEEM_length)sizeof(runtime_->messageIdCounter_)
      );
    }
    std::stringstream ss;
    ss << messageId;
    {
      std::lock_guard<std::mutex> guard(runtime_->mainLock_);
      ::ir::ntnaeem::gate::Message message;
      std::stringstream rss;
      rss << sourceMessageId;
      try {
        message.SetId(0);
        message.SetLabel(enqueueLabel_);
        if (
          NAEEM_os__file_exists (
            (NAEEM_path)(workDirPath_ + "/s").c_str(),
            (NAEEM_string)(rss.str() + ".gid").c_str()
          )
        ) {
          uint64_t relatedGateId;
          NAEEM_os__read_file3 (
            (NAEEM_path)(workDirPath_ + "/s/" + rss.str() + ".gid").c_str(),
            (NAEEM_data)(&relatedGateId),
            0
          );
          message.SetRelId(relatedGateId);
        } else {
          throw std::runtime_error("Reply failed. Source message is not found.");
        }
        message.SetContent(::naeem::hottentot::runtime::types::ByteArray(data, length));
        NAEEM_length dataLength;
        NAEEM_data data = message.Serialize(&dataLength);
        NAEEM_os__write_to_file (
          (NAEEM_path)(workDirPath_ + "/e").c_str(), 
          (NAEEM_string)ss.str().c_str(), 
          data, 
          dataLength
        );
        delete [] data;
        runtime_->enqueued_.push_back(messageId);
      } catch (std::exception &e) {
        ::naeem::hottentot::runtime::Logger::GetError() << 
          "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " <<
            "ERROR: " << e.what() << std::endl;
          throw std::runtime_error(e.what());
      } catch (...) {
        ::naeem::hottentot::runtime::Logger::GetError() << 
          "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " <<
            "ERROR: Unknown error." << std::endl;
        throw std::runtime_error("Unknown error at submitting reply message.");
      }
    }
    return messageId;
  }
}
}
}