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

#include <transport/transport_message.h>
#include <transport/proxy/transport_service.h>
#include <transport/proxy/transport_service_proxy.h>
#include <transport/proxy/transport_service_proxy_builder.h>

#include "slave_thread.h"
#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
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
    ::naeem::hottentot::runtime::types::UInt32 slaveId = ::naeem::conf::ConfigManager::GetValueAsUInt32("slave", "id");
    uint32_t transferInterval = ::naeem::conf::ConfigManager::GetValueAsUInt32("slave", "transfer_interval");
    std::string workDir = ::naeem::conf::ConfigManager::GetValueAsString("slave", "work_dir");
    while (cont) {
      try {
        if (cont) {
          std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        {
          std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
          if (Runtime::termSignal_) {
            if (::naeem::hottentot::runtime::Configuration::Verbose()) {
              ::naeem::hottentot::runtime::Logger::GetOut() << "Slave Thread: Received TERM SIGNAL ..." << std::endl;
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
            if (::naeem::hottentot::runtime::Configuration::Verbose()) {
              ::naeem::hottentot::runtime::Logger::GetOut() << "VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV" << std::endl;
              ::naeem::hottentot::runtime::Logger::GetOut() << Runtime::GetCurrentStat();
            }
            // TODO: Disable LAN ethernet
            // TODO: Enable WAN ethernet
            /*
             * Create a proxy to Master Gate
             */
            if (::naeem::hottentot::runtime::Configuration::Verbose()) {
              ::naeem::hottentot::runtime::Logger::GetOut() << "Connecting to master gate ..." << std::endl;
              ::naeem::hottentot::runtime::Logger::GetOut() << "Making proxy object ..." << std::endl;
            }
            ::ir::ntnaeem::gate::transport::proxy::TransportService *transportProxy = 
              ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Create(
                ::naeem::conf::ConfigManager::GetValueAsString("master", "ip"), 
                ::naeem::conf::ConfigManager::GetValueAsUInt32("master", "port")
              );
            try {
              /*
               * If server is not alive, postbone the operation and release the lock.
               */
              if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                ::naeem::hottentot::runtime::Logger::GetOut() << "Checking if server is available ..." << std::endl;
              }
              bool isServerAlive = 
                dynamic_cast<::ir::ntnaeem::gate::transport::proxy::TransportServiceProxy*>
                  (transportProxy)->IsServerAlive();
              if (isServerAlive) {
                if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                  ::naeem::hottentot::runtime::Logger::GetOut() << "Server is up and running ..." << std::endl;
                }
                /*
                 * Make a list of transport messages
                 */
                {
                  if (Runtime::outbox_.size() > 0) {
                    std::vector<uint64_t> outboxIds = std::move(Runtime::outbox_);
                    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                      ::naeem::hottentot::runtime::Logger::GetOut() << "Number of messages to send: " << outboxIds.size() << std::endl;
                    }
                    ::naeem::hottentot::runtime::types::List< 
                      ::ir::ntnaeem::gate::transport::TransportMessage> transportMessages;
                    std::map<uint64_t, ::ir::ntnaeem::gate::transport::TransportMessage*> map;
                    for (uint32_t i = 0; i < outboxIds.size(); i++) {
                      // Read message file
                      bool fileIsRead = false;
                      NAEEM_data data;
                      NAEEM_length dataLength;
                      try {
                        std::stringstream filePath;
                        filePath << outboxIds[i];
                        if (NAEEM_os__file_exists (
                              (NAEEM_path)(workDir + "/e").c_str(), 
                              (NAEEM_string)filePath.str().c_str()
                            )
                          ) {
                          NAEEM_os__read_file2 (
                            (NAEEM_path)(workDir + "/e/" + filePath.str()).c_str(),
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
                        ::ir::ntnaeem::gate::Message *outboxMessage = 
                          new ::ir::ntnaeem::gate::Message;
                        try {
                          outboxMessage->Deserialize(data, dataLength);
                          deserialized = true;
                          free(data);
                        } catch (...) {
                          delete outboxMessage;
                          // TODO: Deserialization failed.
                        }
                        if (deserialized) {
                          ::ir::ntnaeem::gate::transport::TransportMessage *transportMessage =
                            new ::ir::ntnaeem::gate::transport::TransportMessage;
                          transportMessage->SetMasterMId(0);
                          transportMessage->SetSlaveId(slaveId);
                          transportMessage->SetSlaveMId(outboxMessage->GetId());
                          transportMessage->SetRelMId(0);
                          transportMessage->SetLabel(outboxMessage->GetLabel());
                          transportMessage->SetContent(outboxMessage->GetContent());
                          ::naeem::hottentot::runtime::types::UInt64 masterId;
                          transportMessages.Add(transportMessage);
                          map.insert(std::pair<uint64_t, ::ir::ntnaeem::gate::transport::TransportMessage*>(transportMessage->GetSlaveMId().GetValue(), transportMessage));
                          delete outboxMessage;
                        } else {
                          // TODO: Deserialization failed.
                        }
                      } else {
                        // TODO: File is not read.
                      }
                    }
                    /*
                     * Send queued messages to Master Gate
                     */
                    bool enqueueDone = false;
                    ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::EnqueueReport> enqueueReports;
                    try {
                      if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                        ::naeem::hottentot::runtime::Logger::GetOut() << "Sending messages ..." << std::endl;
                      }
                      transportProxy->Transmit(transportMessages, enqueueReports);
                      enqueueDone = true;
                      if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                        ::naeem::hottentot::runtime::Logger::GetOut() << "Message sent and added to sent queue." << std::endl;
                      }
                    } catch (std::exception &e) {
                      ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: " << e.what() << std::endl;
                      // TODO: Enqueue failed.
                    } catch (...) {
                      ::naeem::hottentot::runtime::Logger::GetError() << "Send error." << std::endl;
                      // TODO: Enqueue failed.
                    }
                    /*
                     * Analyse enqueue reports
                     */
                    if (enqueueDone) {
                      for (uint32_t i = 0; i < enqueueReports.Size(); i++) {
                        ::ir::ntnaeem::gate::transport::EnqueueReport *enqueueReport = enqueueReports.Get(i);
                        if (!enqueueReport->GetFailed().GetValue()) {
                          map[enqueueReport->GetSlaveMId().GetValue()]->SetMasterMId(enqueueReport->GetMasterMId());
                          // Runtime::sentQueue_->Put(map[enqueueReport->GetSlaveMId().GetValue()]);
                          NAEEM_data data;
                          NAEEM_length dataLength;
                          data = map[enqueueReport->GetSlaveMId().GetValue()]->Serialize(&dataLength);
                          std::stringstream ss;
                          ss << enqueueReport->GetSlaveMId().GetValue();
                          uint16_t status = (uint16_t)kMessageStatus___Transmitted;
                          NAEEM_os__write_to_file (
                            (NAEEM_path)(workDir + "/s").c_str(), 
                            (NAEEM_string)ss.str().c_str(),
                            (NAEEM_data)(&status),
                            sizeof(status)
                          );
                          Runtime::states_[enqueueReport->GetSlaveMId().GetValue()] = status;
                          NAEEM_os__write_to_file (
                            (NAEEM_path)(workDir + "/t").c_str(), 
                            (NAEEM_string)ss.str().c_str(),
                            data,
                            dataLength
                          );
                          delete [] data;
                          if (NAEEM_os__file_exists (
                              (NAEEM_path)(workDir + "/e").c_str(), 
                              (NAEEM_string)ss.str().c_str()
                            )
                          ) {
                            NAEEM_os__delete_file (
                              (NAEEM_path)(workDir + "/e").c_str(), 
                              (NAEEM_string)ss.str().c_str()
                            );
                          } else {
                            ::naeem::hottentot::runtime::Logger::GetOut() << 
                              "WARNING: Enqueud file did not exist for deletion, id was " << 
                                ss.str() << std::endl;
                          }
                          Runtime::transmittedTotalCounter_++;
                          NAEEM_os__write_to_file (
                            (NAEEM_path)workDir.c_str(), 
                            (NAEEM_string)"ttco", 
                            (NAEEM_data)&(Runtime::transmittedTotalCounter_), 
                            (NAEEM_length)sizeof(Runtime::transmittedTotalCounter_)
                          );
                          if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                            ::naeem::hottentot::runtime::Logger::GetOut() << "Message is sent successfully: slaveId(" << 
                              enqueueReport->GetSlaveMId().GetValue() << "), masterId(" << enqueueReport->GetMasterMId().GetValue() << 
                                ")" << std::endl;
                          }
                        } else {
                          NAEEM_data data;
                          NAEEM_length dataLength;
                          data = map[enqueueReport->GetSlaveMId().GetValue()]->Serialize(&dataLength);
                          std::stringstream ss;
                          ss << enqueueReport->GetSlaveMId().GetValue();
                          uint16_t status = (uint16_t)kMessageStatus___TransmissionFailed;
                          NAEEM_os__write_to_file (
                            (NAEEM_path)(workDir + "/s").c_str(), 
                            (NAEEM_string)ss.str().c_str(),
                            (NAEEM_data)(&status),
                            sizeof(status)
                          );
                          Runtime::states_[enqueueReport->GetSlaveMId().GetValue()] = status;
                          NAEEM_os__write_to_file (
                            (NAEEM_path)(workDir + "/f").c_str(), 
                            (NAEEM_string)ss.str().c_str(),
                            data,
                            dataLength
                          );
                          delete [] data;
                          if (NAEEM_os__file_exists(
                              (NAEEM_path)(workDir + "/e").c_str(), 
                              (NAEEM_string)ss.str().c_str()
                            )
                          ) {
                            NAEEM_os__delete_file (
                              (NAEEM_path)(workDir + "/e").c_str(), 
                              (NAEEM_string)ss.str().c_str()
                            );
                          } else {
                            ::naeem::hottentot::runtime::Logger::GetOut() << 
                              "WARNING: Enqueud file did not exist for deletion, id was " << 
                                ss.str() << std::endl;
                          }
                          Runtime::transmissionFailureTotalCounter_++;
                          NAEEM_os__write_to_file (
                            (NAEEM_path)workDir.c_str(), 
                            (NAEEM_string)"ftco", 
                            (NAEEM_data)&(Runtime::transmissionFailureTotalCounter_), 
                            (NAEEM_length)sizeof(Runtime::transmissionFailureTotalCounter_)
                          );
                          ::naeem::hottentot::runtime::Logger::GetError() << "Message is NOT enqueued: slaveId(" << 
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
                 * Receive queued messages from Master Gate
                 */
                {
                  if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                    ::naeem::hottentot::runtime::Logger::GetOut() << "Retrieving messages from master ..." << std::endl;
                  }
                  ::naeem::hottentot::runtime::types::List< 
                    ::ir::ntnaeem::gate::transport::TransportMessage> transportMessages;
                  ::naeem::hottentot::runtime::types::List< 
                    ::naeem::hottentot::runtime::types::UInt64> acks;
                  if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                    ::naeem::hottentot::runtime::Logger::GetOut() << "Retrieving slave messages ..." << std::endl;
                  }
                  transportProxy->Retrieve(slaveId, transportMessages);
                  if (::naeem::hottentot::runtime::Configuration::Verbose() || transportMessages.Size() > 0) {
                    ::naeem::hottentot::runtime::Logger::GetOut() << "Messages retrieved from master: " << 
                      transportMessages.Size() << " messages" << std::endl;
                  }
                  for (uint32_t i = 0; i < transportMessages.Size(); i++) {
                    ::ir::ntnaeem::gate::transport::TransportMessage *transportMessage = transportMessages.Get(i);
                    /*
                     * Building up received message object
                     */
                    ::ir::ntnaeem::gate::Message *message = new ::ir::ntnaeem::gate::Message;
                    {
                      std::lock_guard<std::mutex> guard(Runtime::messageIdCounterLock_);
                      message->SetId(Runtime::messageIdCounter_);
                      Runtime::messageIdCounter_++;
                      NAEEM_os__write_to_file (
                        (NAEEM_path)workDir.c_str(), 
                        (NAEEM_string)"mco", 
                        (NAEEM_data)&(Runtime::messageIdCounter_), 
                        (NAEEM_length)sizeof(Runtime::messageIdCounter_)
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
                    NAEEM_os__write_to_file (
                      (NAEEM_path)(workDir + "/s").c_str(), 
                      (NAEEM_string)ss.str().c_str(),
                      (NAEEM_data)(&status),
                      sizeof(status)
                    );
                    Runtime::states_[messageId] = status;
                    NAEEM_data data;
                    NAEEM_length dataLength;
                    data = message->Serialize(&dataLength);
                    NAEEM_os__write_to_file (
                      (NAEEM_path)(workDir + "/r").c_str(), 
                      (NAEEM_string)ss.str().c_str(),
                      data,
                      dataLength
                    );
                    delete [] data;
                    acks.Add(new ::naeem::hottentot::runtime::types::UInt64(
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
                    NAEEM_os__write_to_file (
                      (NAEEM_path)workDir.c_str(), 
                      (NAEEM_string)"rfptco", 
                      (NAEEM_data)&(Runtime::readyForPopTotalCounter_), 
                      (NAEEM_length)sizeof(Runtime::readyForPopTotalCounter_)
                    );
                  }
                  if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                    ::naeem::hottentot::runtime::Logger::GetOut() << "Sending acks ..." << std::endl;
                  }
                  transportProxy->Ack(acks);
                  if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                    ::naeem::hottentot::runtime::Logger::GetOut() << "Messages are retrieved." << std::endl;
                  }
                  transportMessages.Purge();
                  acks.Purge();
                }
                /*
                 * Disconnect from Master Gate
                 */
                ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Destroy(transportProxy);
                // TODO: Disable WAN ethernet
                // TODO: Enable LAN ethernet
                /*
                 * Releasing main lock by leaving the scope
                 */
                if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                  ::naeem::hottentot::runtime::Logger::GetOut() << "Send is complete." << std::endl;
                }
              } else {
                if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                  ::naeem::hottentot::runtime::Logger::GetOut() << "Master is not available now. We postbone the send to next try." << std::endl;
                }
                ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Destroy(transportProxy);
              }
            } catch (std::exception &e) {
              ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Destroy(transportProxy);
              throw std::runtime_error(e.what());
            } catch (...) {
              ::ir::ntnaeem::gate::transport::proxy::TransportServiceProxyBuilder::Destroy(transportProxy);
              throw std::runtime_error("Unknown error.");
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
    Runtime::slaveThreadTerminated_ = true;
    pthread_exit(NULL);
  }
}
}
}
}