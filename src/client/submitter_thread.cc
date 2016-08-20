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


#include <org/labcrypto/fence/client/submitter_thread.h>
#include <org/labcrypto/fence/client/runtime.h>


namespace org {
namespace labcrypto {
namespace fence {
namespace client {
  void
  SubmitterThread::Start() { 
    pthread_t thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_create(&thread, &attr, SubmitterThread::ThreadBody, this);
  }
  void
  SubmitterThread::Shutdown() {
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
            "Submitter thread exited." << std::endl;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  void*
  SubmitterThread::ThreadBody(void *thisObject) {
    ::org::labcrypto::fence::client::SubmitterThread *me =
      (::org::labcrypto::fence::client::SubmitterThread*)(thisObject);
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
                  "Submitter Thread: Received TERM SIGNAL ..." << std::endl;
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
            std::lock_guard<std::mutex> guard(me->runtime_->mainLock_);
            ::org::labcrypto::fence::proxy::FenceService *proxy = 
              ::org::labcrypto::fence::proxy::FenceServiceProxyBuilder::Create(me->fenceHost_, me->fencePort_);
            if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
              ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                "Proxy object is created." << std::endl;
            }
            uint64_t enqueuedCounter = 0;
            if (dynamic_cast< ::org::labcrypto::hottentot::runtime::proxy::Proxy*>(proxy)->IsServerAlive()) {
              if (me->runtime_->enqueued_.size() > 0) {
                std::deque<uint64_t> enqueuedIds = std::move(me->runtime_->enqueued_);
                for (uint64_t i = 0; i < enqueuedIds.size(); i++) {
                  uint64_t messageId = enqueuedIds[i];
                  std::stringstream ss;
                  ss << messageId;
                  ORG_LABCRYPTO_ABETTOR_data data;
                  ORG_LABCRYPTO_ABETTOR_length dataLength;
                  if (!ORG_LABCRYPTO_ABETTOR__fs__file_exists (
                        (ORG_LABCRYPTO_ABETTOR_path)(me->workDirPath_ + "/e").c_str(),
                        (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
                      ) 
                  ) {
                    std::cout <<
                      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                        "WARNING: Message file does not exist for id: " << ss.str() << std::endl;
                    continue;
                  }
                  ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
                    (ORG_LABCRYPTO_ABETTOR_path)(me->workDirPath_ + "/e").c_str(),
                    (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                    &data,
                    &dataLength
                  );
                  ::org::labcrypto::fence::Message message;
                  message.Deserialize(data, dataLength);
                  free(data);
                  /* ----------------------------------------------------
                   * Sending enqueued message
                   * ----------------------------------------------------
                   */
                  try {
                    ::org::labcrypto::hottentot::UInt64 id;
                    proxy->Enqueue(message, id);
                    uint64_t assignedId = id.GetValue();
                    ORG_LABCRYPTO_ABETTOR__fs__copy_file (
                      (ORG_LABCRYPTO_ABETTOR_path)(me->workDirPath_ + "/e").c_str(),
                      (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                      (ORG_LABCRYPTO_ABETTOR_path)(me->workDirPath_ + "/a").c_str(),
                      (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
                    );
                    ORG_LABCRYPTO_ABETTOR__fs__move_file (
                      (ORG_LABCRYPTO_ABETTOR_path)(me->workDirPath_ + "/e").c_str(),
                      (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                      (ORG_LABCRYPTO_ABETTOR_path)(me->workDirPath_ + "/s").c_str(),
                      (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
                    );
                    ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                      (ORG_LABCRYPTO_ABETTOR_path)(me->workDirPath_ + "/s").c_str(),
                      (ORG_LABCRYPTO_ABETTOR_string)(ss.str() + ".gid").c_str(),
                      (ORG_LABCRYPTO_ABETTOR_data)(&assignedId),
                      sizeof(assignedId)
                    );
                    std::stringstream css;
                    css << assignedId;
                    ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                      (ORG_LABCRYPTO_ABETTOR_path)(me->workDirPath_ + "/s").c_str(),
                      (ORG_LABCRYPTO_ABETTOR_string)(css.str() + ".cid").c_str(),
                      (ORG_LABCRYPTO_ABETTOR_data)(&messageId),
                      sizeof(messageId)
                    );
                    enqueuedCounter++;
                  } catch (std::exception &e) {
                    ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
                      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                        "[Fence-Client] ERROR: " << e.what() << std::endl;
                    // ::org::labcrypto::fence::proxy::FenceServiceProxyBuilder::Destroy(proxy);
                    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                          "[Fence-Client] Proxy object is destroyed." << std::endl;
                    }
                    me->runtime_->enqueued_.push_back(messageId);
                  } catch (...) {
                    ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
                      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                        "[Fence-Client] Unknown error." << std::endl;
                    // ::org::labcrypto::fence::proxy::FenceServiceProxyBuilder::Destroy(proxy);
                    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                          "[Fence-Client] Proxy object is destroyed." << std::endl;
                    }
                    me->runtime_->enqueued_.push_back(messageId);
                  }
                }
              }
            } else {
              if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                std::cout << 
                  "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                    "[Fence-Client] Slave fence is not available. Send failed." << std::endl;
              }
            }
            if (::org::labcrypto::hottentot::runtime::Configuration::Verbose() || 
                me->runtime_->enqueued_.size() > 0 || enqueuedCounter > 0) {
              ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                  "[Fence-Client] Number of enqeueud messages: " << enqueuedCounter <<
                  ", Pending: " << me->runtime_->enqueued_.size() << std::endl;
            }
            ::org::labcrypto::fence::proxy::FenceServiceProxyBuilder::Destroy(proxy);
            if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
              ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                  "[Fence-Client] Proxy object is destroyed." << std::endl;
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
          "[Fence-Client] Submitter thread is exiting ..." << std::endl;
    }
    std::lock_guard<std::mutex> guard(me->terminationLock_);
    me->threadTerminated_ = true;
    pthread_exit(NULL);
  }
} // END NAMESAPCE client
} // END NAMESPACE fence
} // END NAMESPACE labcrypto
} // END NAMESPACE org