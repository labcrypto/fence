#include <thread>
#include <chrono>
#include <iostream>
#include <sstream>

#include <naeem/os.h>

#include <naeem++/conf/config_manager.h>

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
    std::string workDir = ::naeem::conf::ConfigManager::GetValueAsString("gate-client", "work_dir");
    std::string host = ::naeem::conf::ConfigManager::GetValueAsString("gate-client", "host");
    uint32_t port = ::naeem::conf::ConfigManager::GetValueAsUInt32("gate-client", "port");
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
            /*
             * Aquiring main lock by creating guard object
             */
            std::lock_guard<std::mutex> guard(Runtime::mainLock_);
            if (Runtime::enqueued_.size() == 0) {
              std::cout << "Nothing to send." << std::endl;
            } else {
              std::vector<uint64_t> waitingIds = std::move(Runtime::enqueued_);
              for (uint64_t i = 0; i < waitingIds.size(); i++) {
                uint64_t messageId = waitingIds[i];
                std::stringstream ss;
                ss << messageId;
                NAEEM_data data;
                NAEEM_length dataLength;
                NAEEM_os__read_file_with_path (
                  (NAEEM_path)(workDir + "/e").c_str(),
                  (NAEEM_string)ss.str().c_str(),
                  &data,
                  &dataLength
                );
                ::ir::ntnaeem::gate::Message message;
                message.Deserialize(data, dataLength);
                free(data);
                ::ir::ntnaeem::gate::proxy::GateService *proxy = 
                  ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Create(host, port);
                if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                  ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is created." << std::endl;
                }
                /* ----------------------------------------------------
                 * Sending enqueued messages
                 * ----------------------------------------------------
                 */
                try {
                  if (dynamic_cast< ::naeem::hottentot::runtime::proxy::Proxy*>(proxy)->IsServerAlive()) {
                    ::naeem::hottentot::runtime::types::UInt64 id;
                    proxy->Enqueue(message, id);
                    uint64_t assignedId = id.GetValue();
                    NAEEM_os__move_file (
                      (NAEEM_path)(workDir + "/e").c_str(),
                      (NAEEM_string)ss.str().c_str(),
                      (NAEEM_path)(workDir + "/s").c_str(),
                      (NAEEM_string)ss.str().c_str()
                    );
                    NAEEM_os__write_to_file (
                      (NAEEM_path)(workDir + "/s").c_str(),
                      (NAEEM_string)(ss.str() + ".gid").c_str(),
                      (NAEEM_data)(&assignedId),
                      sizeof(assignedId)
                    );
                    std::stringstream css;
                    css << assignedId;
                    NAEEM_os__write_to_file (
                      (NAEEM_path)(workDir + "/s").c_str(),
                      (NAEEM_string)(css.str() + ".cid").c_str(),
                      (NAEEM_data)(&messageId),
                      sizeof(messageId)
                    );
                  } else {
                    throw std::runtime_error("Slave gate is not available.");
                  }
                  ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
                  if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                    ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
                  }
                } catch (std::exception &e) {
                  ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: " << e.what() << std::endl;
                  ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
                  if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                    ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
                  }
                  Runtime::enqueued_.push_back(messageId);
                } catch (...) {
                  ::naeem::hottentot::runtime::Logger::GetError() << "Unknown error." << std::endl;
                  ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
                  if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                    ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
                  }
                  Runtime::enqueued_.push_back(messageId);
                }
                /* ----------------------------------------------------
                 * Reading messages
                 * ----------------------------------------------------
                 */
                try {
                  if (dynamic_cast< ::naeem::hottentot::runtime::proxy::Proxy*>(proxy)->IsServerAlive()) {
                    ::naeem::hottentot::runtime::types::Boolean hasMore;
                    ::naeem::hottentot::runtime::types::Utf8String labelString(me->label_);
                    proxy->HasMore(labelString, hasMore);
                    while (hasMore.GetValue()) {
                      ::ir::ntnaeem::gate::Message message;
                      uint64_t messageId;
                      proxy->PopNext(labelString, message);
                      {
                        std::lock_guard<std::mutex> guard(Runtime::messageIdCounterLock_);
                        messageId = Runtime::messageIdCounter_;
                        Runtime::messageIdCounter_++;
                        NAEEM_os__write_to_file (
                          (NAEEM_path)workDir.c_str(), 
                          (NAEEM_string)"mco", 
                          (NAEEM_data)&(Runtime::messageIdCounter_), 
                          (NAEEM_length)sizeof(Runtime::messageIdCounter_)
                        );
                      }
                      std::stringstream ss;
                      ss << messageId;
                      NAEEM_data data;
                      NAEEM_length dataLength;
                      data = message.Serialize(&dataLength);
                      NAEEM_os__write_to_file (
                        (NAEEM_path)(workDir + "/r").c_str(),
                        (NAEEM_string)ss.str().c_str(),
                        data,
                        dataLength
                      );
                      delete [] data;
                      Runtime::received_.push_back(messageId);
                      ::naeem::hottentot::runtime::types::UInt64 messageIdVar(messageId);
                      proxy->Ack(messageIdVar);
                      proxy->HasMore(labelString, hasMore);
                    }
                  } else {
                    throw std::runtime_error("Slave gate is not available.");
                  }
                  ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
                  if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                    ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
                  }
                } catch (std::exception &e) {
                  ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: " << e.what() << std::endl;
                  ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
                  if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                    ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
                  }
                  Runtime::enqueued_.push_back(messageId);
                } catch (...) {
                  ::naeem::hottentot::runtime::Logger::GetError() << "Unknown error." << std::endl;
                  ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
                  if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                    ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
                  }
                  Runtime::enqueued_.push_back(messageId);
                }
              }
            }
          }
        }
      } catch(std::exception &e) {
        ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: " << e.what() << std::endl;
      } catch(...) {
        ::naeem::hottentot::runtime::Logger::GetError() << "Unknown error." << std::endl;
      }
    }
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Slave thread is exiting ..." << std::endl;
    }
    std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
    Runtime::submitterThreadTerminated_ = true;
    pthread_exit(NULL);
  }
}
}
}