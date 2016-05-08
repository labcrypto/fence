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

#include <org/labcrypto/fence/client/default_message_submitter.h>
#include <org/labcrypto/fence/client/runtime.h>


namespace org {
namespace labcrypto {
namespace fence {
namespace client {
  void 
  DefaultMessageSubmitter::Init (
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
    submitterThread_ = new SubmitterThread(fenceHost_, fencePort_, enqueueLabel_, workDirPath_,runtime_);
    submitterThread_->Start();
  }
  void 
  DefaultMessageSubmitter::Shutdown() {
    submitterThread_->Shutdown();
    delete submitterThread_;
    runtime_->Shutdown();
    // delete runtime_;
    /* ::org::labcrypto::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::org::labcrypto::hottentot::runtime::Logger::Shutdown(); */
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
      ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
        (ORG_LABCRYPTO_ABETTOR_path)workDirPath_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"mco", 
        (ORG_LABCRYPTO_ABETTOR_data)&(runtime_->messageIdCounter_), 
        (ORG_LABCRYPTO_ABETTOR_length)sizeof(runtime_->messageIdCounter_)
      );
    }
    std::stringstream ss;
    ss << messageId;
    {
      std::lock_guard<std::mutex> guard(runtime_->mainLock_);
      ::org::labcrypto::fence::Message message;
      try {
        message.SetId(0);
        message.SetLabel(enqueueLabel_);
        message.SetRelId(0);
        message.SetContent(::org::labcrypto::hottentot::ByteArray(data, length));
        ORG_LABCRYPTO_ABETTOR_length dataLength;
        ORG_LABCRYPTO_ABETTOR_data data = message.Serialize(&dataLength);
        ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
          (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/e").c_str(), 
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(), 
          data, 
          dataLength
        );
        delete [] data;
        runtime_->enqueued_.push_back(messageId);
      } catch (std::exception &e) {
        ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
          "ERROR: " << e.what() << std::endl;
      } catch (...) {
        ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
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
      ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
        (ORG_LABCRYPTO_ABETTOR_path)workDirPath_.c_str(), 
        (ORG_LABCRYPTO_ABETTOR_string)"mco", 
        (ORG_LABCRYPTO_ABETTOR_data)&(runtime_->messageIdCounter_), 
        (ORG_LABCRYPTO_ABETTOR_length)sizeof(runtime_->messageIdCounter_)
      );
    }
    std::stringstream ss;
    ss << messageId;
    {
      std::lock_guard<std::mutex> guard(runtime_->mainLock_);
      ::org::labcrypto::fence::Message message;
      std::stringstream rss;
      rss << sourceMessageId;
      try {
        message.SetId(0);
        message.SetLabel(enqueueLabel_);
        if (
          ORG_LABCRYPTO_ABETTOR__fs__file_exists (
            (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/s").c_str(),
            (ORG_LABCRYPTO_ABETTOR_string)(rss.str() + ".gid").c_str()
          )
        ) {
          uint64_t relatedGateId;
          ORG_LABCRYPTO_ABETTOR__fs__read_file_into_buffer (
            (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/s/" + rss.str() + ".gid").c_str(),
            (ORG_LABCRYPTO_ABETTOR_data)(&relatedGateId),
            0
          );
          message.SetRelId(relatedGateId);
        } else {
          throw std::runtime_error("Reply failed. Source message is not found.");
        }
        message.SetContent(::org::labcrypto::hottentot::ByteArray(data, length));
        ORG_LABCRYPTO_ABETTOR_length dataLength;
        ORG_LABCRYPTO_ABETTOR_data data = message.Serialize(&dataLength);
        ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
          (ORG_LABCRYPTO_ABETTOR_path)(workDirPath_ + "/e").c_str(), 
          (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(), 
          data, 
          dataLength
        );
        delete [] data;
        runtime_->enqueued_.push_back(messageId);
      } catch (std::exception &e) {
        ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
          "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " <<
            "ERROR: " << e.what() << std::endl;
          throw std::runtime_error(e.what());
      } catch (...) {
        ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
          "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " <<
            "ERROR: Unknown error." << std::endl;
        throw std::runtime_error("Unknown error at submitting reply message.");
      }
    }
    return messageId;
  }
} // END NAMESAPCE client
} // END NAMESPACE fence
} // END NAMESPACE labcrypto
} // END NAMESPACE org