#include <thread>
#include <chrono>
#include <iostream>
#include <sstream>

#include <org/labcrypto/abettor/fs.h>

#include <org/labcrypto/abettor++/conf/config_manager.h>
#include <org/labcrypto/abettor++/date/helper.h>

#include <org/labcrypto/hottentot/runtime/configuration.h>
#include <org/labcrypto/hottentot/runtime/logger.h>
#include <org/labcrypto/hottentot/runtime/proxy/proxy_runtime.h>

#include <fence/message.h>

#include <transport/transport_message.h>
#include <transport/proxy/transport_service.h>
#include <transport/proxy/transport_service_proxy.h>
#include <transport/proxy/transport_service_proxy_builder.h>

#include "slave_thread.h"
#include "runtime.h"


namespace org {
namespace labcrypto {
namespace fence {
namespace slave {
  void
  SlaveThread::Start() { 
    pthread_t thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_create(&thread, &attr, SlaveThread::ThreadBody, NULL);
  }
  void*
  SlaveThread::ThreadBody(void *) {
    bool cont = true;
    time_t lastTime = time(NULL);
    ::org::labcrypto::hottentot::UInt32 slaveId = ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("slave", "id");
    uint32_t transferInterval = ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("slave", "transfer_interval");
    std::string workDir = ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("slave", "work_dir");
    while (cont) {
      try {
        if (cont) {
          std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        {
          std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
          if (Runtime::termSignal_) {
            if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
              ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                  "Slave Thread: Received TERM SIGNAL ..." << std::endl;
            }
            cont = false;
            break;
          }
        }
        if (cont) {
          bool proceed = false;
          if ((time(NULL) - lastTime) > transferInterval) {
            lastTime = time(NULL);
            proceed = true;
          }
          if (proceed) {
            /*
             * Aquiring main lock by creating guard object
             */
            std::lock_guard<std::mutex> guard(Runtime::mainLock_);
            if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
              ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                  "VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV" << std::endl;
              ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                  Runtime::GetCurrentStat();
            }
            // TODO: Disable LAN ethernet
            // TODO: Enable WAN ethernet
            /*
             * Create a proxy to Master Fence
             */
            if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
              ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                  "Connecting to master gate ..." << std::endl;
              ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                  "Making proxy object ..." << std::endl;
            }
            ::org::labcrypto::fence::transport::proxy::TransportService *transportProxy = 
              ::org::labcrypto::fence::transport::proxy::TransportServiceProxyBuilder::Create(
                ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("master", "ip"), 
                ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("master", "port")
              );
            try {
              /*
               * If server is not alive, postbone the operation and release the lock.
               */
              if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                  "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                    "Checking if server is available ..." << std::endl;
              }
              bool isServerAlive = 
                dynamic_cast<::org::labcrypto::fence::transport::proxy::TransportServiceProxy*>
                  (transportProxy)->IsServerAlive();
              if (isServerAlive) {
                if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                  ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                    "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                      "Server is up and running ..." << std::endl;
                }
                /*
                 * Make a list of transport messages
                 */
                {
                  if (Runtime::outbox_.size() > 0) {
                    std::vector<uint64_t> outboxIds = std::move(Runtime::outbox_);
                    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                          "Number of messages to send: " << outboxIds.size() << std::endl;
                    }
                    ::org::labcrypto::hottentot::List< 
                      ::org::labcrypto::fence::transport::TransportMessage> transportMessages;
                    std::map<uint64_t, ::org::labcrypto::fence::transport::TransportMessage*> map;
                    for (uint32_t i = 0; i < outboxIds.size(); i++) {
                      // Read message file
                      bool fileIsRead = false;
                      ORG_LABCRYPTO_ABETTOR_data data;
                      ORG_LABCRYPTO_ABETTOR_length dataLength;
                      try {
                        std::stringstream filePath;
                        filePath << outboxIds[i];
                        if (ORG_LABCRYPTO_ABETTOR__fs__file_exists (
                              (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/e").c_str(), 
                              (ORG_LABCRYPTO_ABETTOR_string)filePath.str().c_str()
                            )
                          ) {
                          ORG_LABCRYPTO_ABETTOR__fs__read_file_with_full_path (
                            (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/e/" + filePath.str()).c_str(),
                            &data,
                            &dataLength
                          );           
                          fileIsRead = true;         
                        } else {
                          // TODO: Message file is not found !
                        }
                      } catch (...) {
                        // TODO: Exception while reading the file.
                      }
                      if (fileIsRead) {
                        bool deserialized = false;
                        ::org::labcrypto::fence::Message *outboxMessage = 
                          new ::org::labcrypto::fence::Message;
                        try {
                          outboxMessage->Deserialize(data, dataLength);
                          deserialized = true;
                          free(data);
                        } catch (...) {
                          delete outboxMessage;
                          // TODO: Deserialization failed.
                        }
                        if (deserialized) {
                          ::org::labcrypto::fence::transport::TransportMessage *transportMessage =
                            new ::org::labcrypto::fence::transport::TransportMessage;
                          transportMessage->SetMasterMId(0);
                          transportMessage->SetSlaveId(slaveId);
                          transportMessage->SetSlaveMId(outboxMessage->GetId());
                          transportMessage->SetRelMId(0);
                          transportMessage->SetLabel(outboxMessage->GetLabel());
                          transportMessage->SetContent(outboxMessage->GetContent());
                          ::org::labcrypto::hottentot::UInt64 masterId;
                          transportMessages.Add(transportMessage);
                          map.insert(std::pair<uint64_t, 
                              ::org::labcrypto::fence::transport::TransportMessage*>(
                                  transportMessage->GetSlaveMId().GetValue(), transportMessage));
                          delete outboxMessage;
                        } else {
                          // TODO: Deserialization failed.
                        }
                      } else {
                        // TODO: File is not read.
                      }
                    }
                    /*
                     * Send queued messages to Master Fence
                     */
                    bool enqueueDone = false;
                    ::org::labcrypto::hottentot::List< 
                      ::org::labcrypto::fence::transport::EnqueueReport> enqueueReports;
                    try {
                      if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                          "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                            "Sending messages ..." << std::endl;
                      }
                      transportProxy->Transmit(transportMessages, enqueueReports);
                      enqueueDone = true;
                      if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                        ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                          "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                            "Message sent and added to sent queue." << std::endl;
                      }
                    } catch (std::exception &e) {
                      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
                        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                          "ERROR: " << e.what() << std::endl;
                      // TODO: Enqueue failed.
                    } catch (...) {
                      ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
                        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                          "Send error." << std::endl;
                      // TODO: Enqueue failed.
                    }
                    /*
                     * Analyse enqueue reports
                     */
                    if (enqueueDone) {
                      for (uint32_t i = 0; i < enqueueReports.Size(); i++) {
                        ::org::labcrypto::fence::transport::EnqueueReport *enqueueReport = enqueueReports.Get(i);
                        if (!enqueueReport->GetFailed().GetValue()) {
                          map[enqueueReport->GetSlaveMId().GetValue()]->SetMasterMId(enqueueReport->GetMasterMId());
                          // Runtime::sentQueue_->Put(map[enqueueReport->GetSlaveMId().GetValue()]);
                          ORG_LABCRYPTO_ABETTOR_data data;
                          ORG_LABCRYPTO_ABETTOR_length dataLength;
                          data = map[enqueueReport->GetSlaveMId().GetValue()]->Serialize(&dataLength);
                          std::stringstream ss;
                          ss << enqueueReport->GetSlaveMId().GetValue();
                          uint16_t status = (uint16_t)kMessageStatus___Transmitted;
                          ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                            (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/s").c_str(), 
                            (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                            (ORG_LABCRYPTO_ABETTOR_data)(&status),
                            sizeof(status)
                          );
                          Runtime::states_[enqueueReport->GetSlaveMId().GetValue()] = status;
                          ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                            (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/t").c_str(), 
                            (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                            data,
                            dataLength
                          );
                          delete [] data;
                          if (ORG_LABCRYPTO_ABETTOR__fs__file_exists (
                              (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/e").c_str(), 
                              (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
                            )
                          ) {
                            ORG_LABCRYPTO_ABETTOR__fs__delete_file (
                              (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/e").c_str(), 
                              (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
                            );
                          } else {
                            ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                              "WARNING: Enqueud file did not exist for deletion, id was " << 
                                ss.str() << std::endl;
                          }
                          Runtime::transmittedTotalCounter_++;
                          ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                            (ORG_LABCRYPTO_ABETTOR_path)workDir.c_str(), 
                            (ORG_LABCRYPTO_ABETTOR_string)"ttco", 
                            (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::transmittedTotalCounter_), 
                            (ORG_LABCRYPTO_ABETTOR_length)sizeof(Runtime::transmittedTotalCounter_)
                          );
                          if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                            ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                              "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                                "Message is sent successfully: slaveId(" << 
                                  enqueueReport->GetSlaveMId().GetValue() << "), masterId(" << enqueueReport->GetMasterMId().GetValue() << 
                                    ")" << std::endl;
                          }
                        } else {
                          ORG_LABCRYPTO_ABETTOR_data data;
                          ORG_LABCRYPTO_ABETTOR_length dataLength;
                          data = map[enqueueReport->GetSlaveMId().GetValue()]->Serialize(&dataLength);
                          std::stringstream ss;
                          ss << enqueueReport->GetSlaveMId().GetValue();
                          uint16_t status = (uint16_t)kMessageStatus___TransmissionFailed;
                          ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                            (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/s").c_str(), 
                            (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                            (ORG_LABCRYPTO_ABETTOR_data)(&status),
                            sizeof(status)
                          );
                          Runtime::states_[enqueueReport->GetSlaveMId().GetValue()] = status;
                          ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                            (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/f").c_str(), 
                            (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                            data,
                            dataLength
                          );
                          delete [] data;
                          if (ORG_LABCRYPTO_ABETTOR__fs__file_exists(
                              (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/e").c_str(), 
                              (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
                            )
                          ) {
                            ORG_LABCRYPTO_ABETTOR__fs__delete_file (
                              (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/e").c_str(), 
                              (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
                            );
                          } else {
                            ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                              "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                                "WARNING: Enqueud file did not exist for deletion, id was " << 
                                  ss.str() << std::endl;
                          }
                          Runtime::transmissionFailureTotalCounter_++;
                          ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                            (ORG_LABCRYPTO_ABETTOR_path)workDir.c_str(), 
                            (ORG_LABCRYPTO_ABETTOR_string)"ftco", 
                            (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::transmissionFailureTotalCounter_), 
                            (ORG_LABCRYPTO_ABETTOR_length)sizeof(Runtime::transmissionFailureTotalCounter_)
                          );
                          ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
                            "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                              "Message is NOT enqueued: slaveId(" << 
                                enqueueReport->GetSlaveMId().GetValue() << "), masterId(" << enqueueReport->GetMasterMId().GetValue() << 
                                  "), Reason: '" << enqueueReport->GetErrorMessage() << "'" << std::endl;
                        }
                      }
                    } else {
                      for (uint32_t i = 0; i < outboxIds.size(); i++) {
                        Runtime::outbox_.push_back(outboxIds[i]);
                      }
                    }
                    enqueueReports.Purge();
                    transportMessages.Purge();
                  }
                }
                /*
                 * Receive queued messages from Master Fence
                 */
                {
                  if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                        "Retrieving messages from master ..." << std::endl;
                  }
                  ::org::labcrypto::hottentot::List< 
                    ::org::labcrypto::fence::transport::TransportMessage> transportMessages;
                  ::org::labcrypto::hottentot::List< 
                    ::org::labcrypto::hottentot::UInt64> acks;
                  if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                        "Retrieving slave messages ..." << std::endl;
                  }
                  transportProxy->Retrieve(slaveId, transportMessages);
                  if (::org::labcrypto::hottentot::runtime::Configuration::Verbose() || transportMessages.Size() > 0) {
                    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                        "Messages retrieved from master: " << 
                      transportMessages.Size() << " messages" << std::endl;
                  }
                  for (uint32_t i = 0; i < transportMessages.Size(); i++) {
                    ::org::labcrypto::fence::transport::TransportMessage *transportMessage = transportMessages.Get(i);
                    /*
                     * Building up received message object
                     */
                    ::org::labcrypto::fence::Message *message = new ::org::labcrypto::fence::Message;
                    {
                      std::lock_guard<std::mutex> guard(Runtime::messageIdCounterLock_);
                      message->SetId(Runtime::messageIdCounter_);
                      Runtime::messageIdCounter_++;
                      ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                        (ORG_LABCRYPTO_ABETTOR_path)workDir.c_str(), 
                        (ORG_LABCRYPTO_ABETTOR_string)"mco", 
                        (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::messageIdCounter_), 
                        (ORG_LABCRYPTO_ABETTOR_length)sizeof(Runtime::messageIdCounter_)
                      );
                    }
                    uint64_t messageId = message->GetId().GetValue();
                    message->SetRelId(transportMessage->GetRelMId());
                    message->SetLabel(transportMessage->GetLabel());
                    message->SetContent(transportMessage->GetContent());
                    /*
                     * Persisting message object
                     */
                    std::stringstream ss;
                    ss << messageId;
                    uint16_t status = (uint16_t)kMessageStatus___ReadyForPop;
                    ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                      (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/s").c_str(), 
                      (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                      (ORG_LABCRYPTO_ABETTOR_data)(&status),
                      sizeof(status)
                    );
                    Runtime::states_[messageId] = status;
                    ORG_LABCRYPTO_ABETTOR_data data;
                    ORG_LABCRYPTO_ABETTOR_length dataLength;
                    data = message->Serialize(&dataLength);
                    ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                      (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/r").c_str(), 
                      (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                      data,
                      dataLength
                    );
                    delete [] data;
                    acks.Add(new ::org::labcrypto::hottentot::UInt64(
                      transportMessage->GetMasterMId().GetValue()));
                    if (Runtime::readyForPop_.find(message->GetLabel().ToStdString()) == 
                          Runtime::readyForPop_.end()) {
                      Runtime::readyForPop_.insert(std::pair<std::string, std::deque<uint64_t>*>
                        (message->GetLabel().ToStdString(), new std::deque<uint64_t>()));
                    }
                    Runtime::readyForPop_[message->GetLabel().ToStdString()]
                      ->push_back(messageId);
                    delete message;
                    Runtime::readyForPopTotalCounter_++;
                    ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                      (ORG_LABCRYPTO_ABETTOR_path)workDir.c_str(), 
                      (ORG_LABCRYPTO_ABETTOR_string)"rfptco", 
                      (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::readyForPopTotalCounter_), 
                      (ORG_LABCRYPTO_ABETTOR_length)sizeof(Runtime::readyForPopTotalCounter_)
                    );
                  }
                  if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                        "Sending acks ..." << std::endl;
                  }
                  transportProxy->Ack(acks);
                  if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                    ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                      "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                        "Messages are retrieved." << std::endl;
                  }
                  transportMessages.Purge();
                  acks.Purge();
                }
                /*
                 * Disconnect from Master Fence
                 */
                ::org::labcrypto::fence::transport::proxy::TransportServiceProxyBuilder::Destroy(transportProxy);
                // TODO: Disable WAN ethernet
                // TODO: Enable LAN ethernet
                /*
                 * Releasing main lock by leaving the scope
                 */
                if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                  ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                    "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                      "Send is complete." << std::endl;
                }
              } else {
                if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
                  ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                    "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                      "Master is not available now. We postbone the send to next try." << std::endl;
                }
                ::org::labcrypto::fence::transport::proxy::TransportServiceProxyBuilder::Destroy(transportProxy);
              }
            } catch (std::exception &e) {
              ::org::labcrypto::fence::transport::proxy::TransportServiceProxyBuilder::Destroy(transportProxy);
              throw std::runtime_error("[" + ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() + "]: " + e.what());
            } catch (...) {
              ::org::labcrypto::fence::transport::proxy::TransportServiceProxyBuilder::Destroy(transportProxy);
              throw std::runtime_error("[" + ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() + "]: Unknown error.");
            }
          }
        }
      } catch(std::exception &e) {
        ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
          "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
            "ERROR: " << e.what() << std::endl;
      } catch(...) {
        ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
          "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
            "Unknown error." << std::endl;
      }
    }
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Slave thread is exiting ..." << std::endl;
    }
    std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
    Runtime::slaveThreadTerminated_ = true;
    pthread_exit(NULL);
  }
}
}
}
}