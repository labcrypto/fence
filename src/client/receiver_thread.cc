#include <thread>
#include <chrono>
#include <iostream>
#include <sstream>

#include <org/labcrypto/abettor/fs.h>

#include <org/labcrypto/abettor++/date/helper.h>

#include <org/labcrypto/hottentot/runtime/configuration.h>
#include <org/labcrypto/hottentot/runtime/logger.h>
#include <org/labcrypto/hottentot/runtime/proxy/proxy_runtime.h>

#include <fence/message.h>
#include <fence/proxy/fence_service_proxy.h>
#include <fence/proxy/fence_service_proxy_builder.h>


#include <org/labcrypto/fence/client/receiver_thread.h>
#include <org/labcrypto/fence/client/runtime.h>


namespace org {
namespace labcrypto {
namespace fence {
namespace client {
  void
  ReceiverThread::Start() { 
    pthread_t thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_create(&thread, &attr, ReceiverThread::ThreadBody, this);
  }
  void
  ReceiverThread::Shutdown() {
    {
      std::lock_guard<std::mutex> guard(terminationLock_);
      terminated_ = true;
    }
    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " <<
        "Waiting for submitter thread to exit ..." << std::endl;
    while (true) {
      std::lock_guard<std::mutex> guard(terminationLock_);
      if (threadTerminated_) {
        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
          "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " <<
            "Receiver thread exited." << std::endl;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  void*
  ReceiverThread::ThreadBody(void *thisObject) {
    ::org::labcrypto::fence::client::ReceiverThread *me =
      (::org::labcrypto::fence::client::ReceiverThread*)(thisObject);
    bool cont = true;
    time_t lastTime = time(NULL);
    while (cont) {
      try {
        if (cont) {
          std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        {
          std::lock_guard<std::mutex> guard(me->terminationLock_);
          if (me->terminated_) {
            if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
              ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " <<
                  "Receiver Thread: Received TERM SIGNAL ..." << std::endl;
            }
            cont = false;
            break;
          }
        }
        if (cont) {
          bool proceed = false;
          if ((time(NULL) - lastTime) > 5) {
            lastTime = time(NULL);
            proceed = true;
          }
          if (proceed) {
            /*
             * Aquiring main lock by creating guard object
             */
            std::lock_guard<std::mutex> guard(me->runtime_->mainLock_);
            /* ----------------------------------------------------
             * Reading messages
             * ----------------------------------------------------
             */
            ::org::labcrypto::fence::proxy::FenceService *proxy = 
              ::org::labcrypto::fence::proxy::FenceServiceProxyBuilder::Create(me->fenceHost_, me->fencePort_);
            if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
              ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                  "Proxy object is created." << std::endl;
            }
            uint64_t readMessages = 0;
            try {
              if (dynamic_cast< ::org::labcrypto::hottentot::runtime::proxy::Proxy*>(proxy)->IsServerAlive()) {
                ::org::labcrypto::hottentot::Boolean hasMore;
                ::org::labcrypto::hottentot::Utf8String labelString(me->popLabel_);
                proxy->HasMore(labelString, hasMore);
                while (hasMore.GetValue()) {
                  ::org::labcrypto::fence::Message message;
                  uint64_t messageId;
                  proxy->PopNext(labelString, message);
                  {
                    std::lock_guard<std::mutex> guard(me->runtime_->messageIdCounterLock_);
                    messageId = me->runtime_->messageIdCounter_;
                    me->runtime_->messageIdCounter_++;
                    ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                      (ORG_LABCRYPTO_ABETTOR_path)me->workDirPath_.c_str(), 
                      (ORG_LABCRYPTO_ABETTOR_string)"mco", 
                      (ORG_LABCRYPTO_ABETTOR_data)&(me->runtime_->messageIdCounter_), 
                      (ORG_LABCRYPTO_ABETTOR_length)sizeof(me->runtime_->messageIdCounter_)
                    );
                  }
                  std::stringstream ss;
                  ss << messageId;
                  ORG_LABCRYPTO_ABETTOR_data data;
                  ORG_LABCRYPTO_ABETTOR_length dataLength;
                  data = message.Serialize(&dataLength);
                  ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                    (ORG_LABCRYPTO_ABETTOR_path)(me->workDirPath_ + "/r").c_str(),
                    (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                    data,
                    dataLength
                  );
                  ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                    (ORG_LABCRYPTO_ABETTOR_path)(me->workDirPath_ + "/ra").c_str(),
                    (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                    data,
                    dataLength
                  );
                  delete [] data;
                  uint64_t fenceId = message.GetId().GetValue();
                  ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                    (ORG_LABCRYPTO_ABETTOR_path)(me->workDirPath_ + "/s").c_str(),
                    (ORG_LABCRYPTO_ABETTOR_string)(ss.str() + ".gid").c_str(),
                    (ORG_LABCRYPTO_ABETTOR_data)(&fenceId),
                    sizeof(fenceId)
                  );
                  if (me->runtime_->received_.find(me->popLabel_) == me->runtime_->received_.end()) {
                    me->runtime_->received_.insert(
                      std::pair<std::string, std::deque<uint64_t>*>(
                        me->popLabel_, new std::deque<uint64_t>));
                  }
                  me->runtime_->received_[me->popLabel_]->push_back(messageId);
                  ::org::labcrypto::hottentot::UInt64 messageIdVar(message.GetId().GetValue());
                  proxy->Ack(messageIdVar);
                  readMessages++;
                  proxy->HasMore(labelString, hasMore);
                }
              } else {
                if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                  std::cout << 
                    "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                      "[Fence-Client] Slave fence is not available. Send failed." << std::endl;
                }
              }
              if (::org::labcrypto::hottentot::runtime::Configuration::Verbose() || 
                    readMessages > 0) {
                std::cout << 
                  "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                    "[Fence-Client] Number of read messages: " << readMessages << std::endl;
              }
              ::org::labcrypto::fence::proxy::FenceServiceProxyBuilder::Destroy(proxy);
              if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                  "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                    "Proxy object is destroyed." << std::endl;
              }
            } catch (std::exception &e) {
              ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
                "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                  "ERROR: " << e.what() << std::endl;
              ::org::labcrypto::fence::proxy::FenceServiceProxyBuilder::Destroy(proxy);
              if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                  "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                    "Proxy object is destroyed." << std::endl;
              }
            } catch (...) {
              ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
                "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                  "[Fence-Client] Unknown error." << std::endl;
              ::org::labcrypto::fence::proxy::FenceServiceProxyBuilder::Destroy(proxy);
              if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                  "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                    "Proxy object is destroyed." << std::endl;
              }
            }
          }
        }
      } catch(std::exception &e) {
        ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
          "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
            "[Fence-Client] ERROR: " << e.what() << std::endl;
      } catch(...) {
        ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
          "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
            "[Fence-Client] Unknown error." << std::endl;
      }
    }
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "[Fence-Client] Receiver thread is exiting ..." << std::endl;
    }
    std::lock_guard<std::mutex> guard(me->terminationLock_);
    me->threadTerminated_ = true;
    pthread_exit(NULL);
  }
} // END NAMESAPCE client
} // END NAMESPACE fence
} // END NAMESPACE labcrypto
} // END NAMESPACE org