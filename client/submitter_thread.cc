#include <thread>
#include <chrono>
#include <iostream>
#include <sstream>

#include <naeem/os.h>

#include <naeem++/date/helper.h>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include <gate/message.h>
#include <gate/proxy/gate_service_proxy.h>
#include <gate/proxy/gate_service_proxy_builder.h>


#include <naeem/gate/client/submitter_thread.h>
#include <naeem/gate/client/runtime.h>


namespace naeem {
namespace gate {
namespace client {
  void
  SubmitterThread::Start() { 
    pthread_t thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_create(&thread, &attr, SubmitterThread::ThreadBody, this);
  }
  void*
  SubmitterThread::ThreadBody(void *thisObject) {
    ::naeem::gate::client::SubmitterThread *me =
      (::naeem::gate::client::SubmitterThread*)(thisObject);
    bool cont = true;
    time_t lastTime = time(NULL);
    while (cont) {
      try {
        if (cont) {
          std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        {
          std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
          if (Runtime::termSignal_) {
            if (::naeem::hottentot::runtime::Configuration::Verbose()) {
              ::naeem::hottentot::runtime::Logger::GetOut() << 
                "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
                  "Slave Thread: Received TERM SIGNAL ..." << std::endl;
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
            std::lock_guard<std::mutex> guard(Runtime::mainLock_);
            if (Runtime::enqueued_.size() > 0) {
              std::deque<uint64_t> enqueuedIds = std::move(Runtime::enqueued_);
              uint64_t enqueuedCounter = 0;
              for (uint64_t i = 0; i < enqueuedIds.size(); i++) {
                uint64_t messageId = enqueuedIds[i];
                std::stringstream ss;
                ss << messageId;
                NAEEM_data data;
                NAEEM_length dataLength;
                NAEEM_os__read_file_with_path (
                  (NAEEM_path)(me->workDirPath_ + "/e").c_str(),
                  (NAEEM_string)ss.str().c_str(),
                  &data,
                  &dataLength
                );
                ::ir::ntnaeem::gate::Message message;
                message.Deserialize(data, dataLength);
                free(data);
                ::ir::ntnaeem::gate::proxy::GateService *proxy = 
                  ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Create(me->gateHost_, me->gatePort_);
                if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                  ::naeem::hottentot::runtime::Logger::GetOut() << 
                    "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
                    "Proxy object is created." << std::endl;
                }
                /* ----------------------------------------------------
                 * Sending enqueued message
                 * ----------------------------------------------------
                 */
                try {
                  if (dynamic_cast< ::naeem::hottentot::runtime::proxy::Proxy*>(proxy)->IsServerAlive()) {
                    ::naeem::hottentot::runtime::types::UInt64 id;
                    proxy->Enqueue(message, id);
                    uint64_t assignedId = id.GetValue();
                    NAEEM_os__copy_file (
                      (NAEEM_path)(me->workDirPath_ + "/e").c_str(),
                      (NAEEM_string)ss.str().c_str(),
                      (NAEEM_path)(me->workDirPath_ + "/a").c_str(),
                      (NAEEM_string)ss.str().c_str()
                    );
                    NAEEM_os__move_file (
                      (NAEEM_path)(me->workDirPath_ + "/e").c_str(),
                      (NAEEM_string)ss.str().c_str(),
                      (NAEEM_path)(me->workDirPath_ + "/s").c_str(),
                      (NAEEM_string)ss.str().c_str()
                    );
                    NAEEM_os__write_to_file (
                      (NAEEM_path)(me->workDirPath_ + "/s").c_str(),
                      (NAEEM_string)(ss.str() + ".gid").c_str(),
                      (NAEEM_data)(&assignedId),
                      sizeof(assignedId)
                    );
                    std::stringstream css;
                    css << assignedId;
                    NAEEM_os__write_to_file (
                      (NAEEM_path)(me->workDirPath_ + "/s").c_str(),
                      (NAEEM_string)(css.str() + ".cid").c_str(),
                      (NAEEM_data)(&messageId),
                      sizeof(messageId)
                    );
                    enqueuedCounter++;
                  } else {
                    throw std::runtime_error("[" + ::naeem::date::helper::GetCurrentUTCTimeString() + "]: " + "Slave gate is not available. Send failed.");
                  }
                  ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
                  if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                    ::naeem::hottentot::runtime::Logger::GetOut() << 
                      "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
                        "Proxy object is destroyed." << std::endl;
                  }
                } catch (std::exception &e) {
                  ::naeem::hottentot::runtime::Logger::GetError() << 
                    "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
                      "ERROR: " << e.what() << std::endl;
                  ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
                  if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                    ::naeem::hottentot::runtime::Logger::GetOut() << 
                      "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
                        "Proxy object is destroyed." << std::endl;
                  }
                  Runtime::enqueued_.push_back(messageId);
                } catch (...) {
                  ::naeem::hottentot::runtime::Logger::GetError() << 
                    "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
                      "Unknown error." << std::endl;
                  ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
                  if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                    ::naeem::hottentot::runtime::Logger::GetOut() << 
                      "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
                        "Proxy object is destroyed." << std::endl;
                  }
                  Runtime::enqueued_.push_back(messageId);
                }
              }
              ::naeem::hottentot::runtime::Logger::GetOut() << 
                "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
                  "Number of enqeueud messages: " << enqueuedCounter << std::endl;
            }
          }
        }
      } catch(std::exception &e) {
        ::naeem::hottentot::runtime::Logger::GetError() << 
          "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
            "ERROR: " << e.what() << std::endl;
      } catch(...) {
        ::naeem::hottentot::runtime::Logger::GetError() << 
          "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
            "Unknown error." << std::endl;
      }
    }
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Slave thread is exiting ..." << std::endl;
    }
    std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
    Runtime::submitterThreadTerminated_ = true;
    pthread_exit(NULL);
  }
}
}
}