#include <thread>
#include <chrono>
#include <iostream>
#include <sstream>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include <naeem/os.h>

#include <naeem++/conf/config_manager.h>

#include <gate/message.h>

#include <transport/transport_message.h>

#include "master_thread.h"
#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace master {
  void
  MasterThread::Start() {
    pthread_t thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_create(&thread, &attr, MasterThread::ThreadBody, NULL);
  }
  void*
  MasterThread::ThreadBody(void *) {
    bool cont = true;
    time_t lastTime = time(NULL);
    uint32_t transferInterval = ::naeem::conf::ConfigManager::GetValueAsUInt32("master", "transfer_interval");
    std::string workDir = ::naeem::conf::ConfigManager::GetValueAsString("master", "work_dir");
    while (cont) {
      {
        std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
        if (Runtime::termSignal_) {
          if (::naeem::hottentot::runtime::Configuration::Verbose()) {
            ::naeem::hottentot::runtime::Logger::GetOut() << "Master Thread: Received TERM SIGNAL ..." << std::endl;
          }
          cont = false;
          break;
        }
      }
      if (cont) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
      {
        std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
        if (Runtime::termSignal_) {
          if (::naeem::hottentot::runtime::Configuration::Verbose()) {
            ::naeem::hottentot::runtime::Logger::GetOut() << "Master Thread: Received TERM SIGNAL ..." << std::endl;
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
          if (::naeem::hottentot::runtime::Configuration::Verbose()) {
            ::naeem::hottentot::runtime::Logger::GetOut() << "VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV" << std::endl;
          }
          if (::naeem::hottentot::runtime::Configuration::Verbose()) {
            ::naeem::hottentot::runtime::Logger::GetOut() << "Waiting for main lock ..." << std::endl;
          }
          /*
           * Aquiring main lock by creating guard object
           */
          std::lock_guard<std::mutex> guard(Runtime::mainLock_);
          if (::naeem::hottentot::runtime::Configuration::Verbose()) {
            ::naeem::hottentot::runtime::Logger::GetOut() << "Main lock is acquired." << std::endl;
            ::naeem::hottentot::runtime::Logger::GetOut() << Runtime::GetCurrentStat();
          }
          /*
           * Copying from 'transport inbox queue' into 'inbox queue'
           */
          {
            std::vector<uint64_t> arrivedIds = std::move(Runtime::arrived_);
            // std::vector<ir::ntnaeem::gate::transport::TransportMessage*> inboxTransportMessages;
            if (::naeem::hottentot::runtime::Configuration::Verbose()) {
              ::naeem::hottentot::runtime::Logger::GetOut() << "Number of transport inbox messages: " << arrivedIds.size() << std::endl;
            }
            for (uint64_t i = 0; i < arrivedIds.size(); i++) {
              std::stringstream ss;
              ss << arrivedIds[i];
              /*
               * Reading transport message file
               */
              bool fileIsRead = false;
              NAEEM_data data;
              NAEEM_length dataLength;
              try {
                if (NAEEM_os__file_exists (
                      (NAEEM_path)(workDir + "/a").c_str(), 
                      (NAEEM_string)ss.str().c_str()
                    )
                ) {
                  NAEEM_os__read_file_with_path (
                    (NAEEM_path)(workDir + "/a").c_str(), 
                    (NAEEM_string)ss.str().c_str(),
                    &data, 
                    &dataLength
                  );
                  fileIsRead = true;
                } else {
                  // TODO: Message file is not found.
                  ::naeem::hottentot::runtime::Logger::GetError() << "File does not exist." << std::endl;
                }
              } catch (std::exception &e) {
                std::cout << "ERROR: " << e.what() << std::endl;
              } catch (...) {
                std::cout << "ERROR: Unknown." << std::endl;
              }
              /*
               * Transport message deserialization
               */
              bool deserialized = false;
              ::ir::ntnaeem::gate::Message inboxMessage;
              ::ir::ntnaeem::gate::transport::TransportMessage inboxTransportMessage;
              if (fileIsRead) {
                try {
                  inboxTransportMessage.Deserialize(data, dataLength);
                  deserialized = true;
                } catch (std::exception &e) {
                  std::cout << "ERROR: " << e.what() << std::endl;
                } catch (...) {
                  std::cout << "ERROR: Unknown." << std::endl;
                }
                free(data);
              }
              /*
               * Making message from transport message
               */
              if (deserialized) {
                inboxMessage.SetId(inboxTransportMessage.GetMasterMId());
                inboxMessage.SetRelId(0);
                inboxMessage.SetLabel(inboxTransportMessage.GetLabel());
                inboxMessage.SetContent(inboxTransportMessage.GetContent());
                data = inboxMessage.Serialize(&dataLength);
                NAEEM_os__write_to_file (
                  (NAEEM_path)(workDir + "/rfp").c_str(), 
                  (NAEEM_string)ss.str().c_str(), 
                  data, 
                  dataLength
                );
                delete [] data;
                if (NAEEM_os__file_exists (
                      (NAEEM_path)(workDir + "/a").c_str(), 
                      (NAEEM_string)ss.str().c_str()
                    )
                ) {
                  NAEEM_os__copy_file (
                    (NAEEM_path)(workDir + "/a").c_str(),
                    (NAEEM_string)ss.str().c_str(),
                    (NAEEM_path)(workDir + "/aa").c_str(),
                    (NAEEM_string)ss.str().c_str()
                  );
                  NAEEM_os__delete_file (
                    (NAEEM_path)(workDir + "/a").c_str(), 
                    (NAEEM_string)ss.str().c_str()
                  );
                }
                uint16_t status = 
                  (uint16_t)::ir::ntnaeem::gate::transport::kTransportMessageStatus___ReadyForPop;
                NAEEM_os__write_to_file (
                  (NAEEM_path)(workDir + "/s").c_str(), 
                  (NAEEM_string)ss.str().c_str(),
                  (NAEEM_data)(&status),
                  sizeof(status)
                );
                uint32_t slaveId = inboxTransportMessage.GetSlaveId().GetValue();
                NAEEM_os__write_to_file (
                  (NAEEM_path)(workDir + "/ss").c_str(), 
                  (NAEEM_string)(ss.str() + ".slaveid").c_str(),
                  (NAEEM_data)(&slaveId),
                  sizeof(slaveId)
                );
                uint64_t slaveMId = inboxTransportMessage.GetSlaveMId().GetValue();
                NAEEM_os__write_to_file (
                  (NAEEM_path)(workDir + "/ss").c_str(), 
                  (NAEEM_string)(ss.str() + ".slavemid").c_str(),
                  (NAEEM_data)(&slaveMId),
                  sizeof(slaveMId)
                );
                Runtime::states_[inboxMessage.GetId().GetValue()] = status;
                if (Runtime::readyForPop_.find(inboxMessage.GetLabel().ToStdString()) == 
                      Runtime::readyForPop_.end()) {
                  Runtime::readyForPop_.insert(std::pair<std::string, std::deque<uint64_t>*>
                    (inboxMessage.GetLabel().ToStdString(), new std::deque<uint64_t>()));
                }
                Runtime::readyForPop_[inboxMessage.GetLabel().ToStdString()]
                  ->push_back(inboxMessage.GetId().GetValue());
                Runtime::readyForPopTotalCounter_++;
                NAEEM_os__write_to_file (
                  (NAEEM_path)workDir.c_str(), 
                  (NAEEM_string)"rfptco", 
                  (NAEEM_data)&(Runtime::readyForPopTotalCounter_), 
                  (NAEEM_length)sizeof(Runtime::readyForPopTotalCounter_)
                );
              } else {
                // TODO : Message is not deserialized.
              }
            }
          }
          if (::naeem::hottentot::runtime::Configuration::Verbose()) {
            ::naeem::hottentot::runtime::Logger::GetOut() << "Messages moved from transport inbox to gate inbox." << std::endl;
          }
          /* 
           * Copying 'enqueued' messages to 'ready for retrieval' queue
           */
          {
            std::vector<uint64_t> enqueuedIds = std::move(Runtime::enqueued_);
            if (::naeem::hottentot::runtime::Configuration::Verbose()) {
              ::naeem::hottentot::runtime::Logger::GetOut() << "Number of enqueued messages: " << enqueuedIds.size() << std::endl;
            }
            for (uint64_t i = 0; i < enqueuedIds.size(); i++) {
              std::stringstream ss;
              ss << enqueuedIds[i];
              if (NAEEM_os__file_exists (
                    (NAEEM_path)(workDir + "/e").c_str(),
                    (NAEEM_string)ss.str().c_str()
                  )
              ) {
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
                std::stringstream rss;
                rss << message.GetRelId().GetValue();
                if (NAEEM_os__file_exists (
                      (NAEEM_path)(workDir + "/ss").c_str(),
                      (NAEEM_string)(ss.str() + ".slaveid").c_str()
                    )
                ) {
                  uint32_t slaveId;
                  NAEEM_os__read_file3 (
                    (NAEEM_path)(workDir + "/ss/" + ss.str() + ".slaveid").c_str(),
                    (NAEEM_data)&slaveId,
                    0
                  );
                  if (NAEEM_os__file_exists (
                        (NAEEM_path)(workDir + "/ss").c_str(),
                        (NAEEM_string)(ss.str() + ".slavemid").c_str()
                      )
                  ) {
                    uint64_t slaveMId;
                    NAEEM_os__read_file3 (
                      (NAEEM_path)(workDir + "/ss/" + ss.str() + ".slavemid").c_str(),
                      (NAEEM_data)&slaveMId,
                      0
                    );
                    uint16_t status = 
                      (uint16_t)::ir::ntnaeem::gate::transport::kTransportMessageStatus___ReadyForRetrieval;
                    NAEEM_os__write_to_file (
                      (NAEEM_path)(workDir + "/s").c_str(), 
                      (NAEEM_string)ss.str().c_str(),
                      (NAEEM_data)(&status),
                      sizeof(status)
                    );
                    Runtime::states_[message.GetId().GetValue()] = status;
                    NAEEM_os__copy_file (
                      (NAEEM_path)(workDir + "/e").c_str(),
                      (NAEEM_string)ss.str().c_str(),
                      (NAEEM_path)(workDir + "/ea").c_str(),
                      (NAEEM_string)ss.str().c_str()
                    );
                    NAEEM_os__delete_file (
                      (NAEEM_path)(workDir + "/e").c_str(), 
                      (NAEEM_string)ss.str().c_str()
                    );
                    ::ir::ntnaeem::gate::transport::TransportMessage transportMessage;
                    transportMessage.SetMasterMId(message.GetId());
                    transportMessage.SetSlaveId(slaveId);
                    transportMessage.SetSlaveMId(0);
                    transportMessage.SetRelMId(slaveMId);
                    transportMessage.SetLabel(message.GetLabel());
                    transportMessage.SetContent(message.GetContent());
                    data = transportMessage.Serialize(&dataLength);
                    NAEEM_os__write_to_file (
                      (NAEEM_path)(workDir + "/rfr").c_str(), 
                      (NAEEM_string)ss.str().c_str(),
                      data,
                      dataLength
                    );
                    delete [] data;
                    if (Runtime::readyForRetrieval_.find(slaveId) == Runtime::readyForRetrieval_.end()) {
                      Runtime::readyForRetrieval_.insert(std::pair<uint32_t, std::vector<uint64_t>*>
                        (slaveId, new std::vector<uint64_t>()));
                    }
                    Runtime::readyForRetrieval_[slaveId]->push_back(message.GetId().GetValue());
                    Runtime::readyForRetrievalTotalCounter_++;
                    NAEEM_os__write_to_file (
                      (NAEEM_path)workDir.c_str(), 
                      (NAEEM_string)"rfrtco", 
                      (NAEEM_data)&(Runtime::readyForRetrievalTotalCounter_), 
                      (NAEEM_length)sizeof(Runtime::readyForRetrievalTotalCounter_)
                    );
                  } else {
                    // TODO: Message status file does not exist.
                  }
                } else {
                  // TODO: Message status file does not exist.
                }
              } else {
                // TODO: Message file does not exist.
              }
            }
            /* std::vector<ir::ntnaeem::gate::Message*> outboxMessages = 
              Runtime::outboxQueue_->PopAll();
            if (::naeem::hottentot::runtime::Configuration::Verbose()) {
              ::naeem::hottentot::runtime::Logger::GetOut() << "Number of outbox messages: " << outboxMessages.size() << std::endl;
            }
            for (uint32_t i = 0; i < outboxMessages.size(); i++) {
              ::ir::ntnaeem::gate::Message *outboxMessage = 
                outboxMessages[i];
              if (Runtime::slaveMessageMap_.find(outboxMessage->GetRelId().GetValue()) == Runtime::slaveMessageMap_.end() ||
                  Runtime::masterIdToSlaveIdMap_.find(Runtime::slaveMessageMap_[outboxMessage->GetRelId().GetValue()]) == Runtime::masterIdToSlaveIdMap_.end() ||
                  Runtime::masterIdToSlaveIdMap_[Runtime::slaveMessageMap_[outboxMessage->GetRelId().GetValue()]]->find(outboxMessage->GetRelId().GetValue()) == Runtime::masterIdToSlaveIdMap_[Runtime::slaveMessageMap_[outboxMessage->GetRelId().GetValue()]]->end()) {
                if (::naeem::hottentot::runtime::Configuration::Verbose()) {
                  ::naeem::hottentot::runtime::Logger::GetOut() << "Outbox message is dropped." << std::endl;
                }
                continue;
              }
              ::ir::ntnaeem::gate::transport::TransportMessage *outboxTransportMessage =
                new ::ir::ntnaeem::gate::transport::TransportMessage;
              outboxTransportMessage->SetMasterMId(outboxMessage->GetId());
              outboxTransportMessage->SetSlaveId(Runtime::slaveMessageMap_[outboxMessage->GetRelId().GetValue()]);
              outboxTransportMessage->SetSlaveMId(0);
              outboxTransportMessage->SetRelMId(Runtime::masterIdToSlaveIdMap_[outboxTransportMessage->GetSlaveId().GetValue()]->at(outboxMessage->GetRelId().GetValue()));
              outboxTransportMessage->SetRelLabel(outboxMessage->GetRelLabel());
              outboxTransportMessage->SetLabel(outboxMessage->GetLabel());
              outboxTransportMessage->SetContent(outboxMessage->GetContent());
              Runtime::transportOutboxQueue_->Put(outboxTransportMessage->GetSlaveId().GetValue(), outboxTransportMessage);
            }
            if (::naeem::hottentot::runtime::Configuration::Verbose()) {
              ::naeem::hottentot::runtime::Logger::GetOut() << "Messages moved from gate outbox to transport outbox." << std::endl;
            } */
          }
          if (::naeem::hottentot::runtime::Configuration::Verbose()) {
            ::naeem::hottentot::runtime::Logger::GetOut() << "Main lock is released." << std::endl;
          }
        }
      }
    }
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Master thread is exiting ..." << std::endl;
    }
    std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
    Runtime::masterThreadTerminated_ = true;
    pthread_exit(NULL);
  }
}
}
}
}