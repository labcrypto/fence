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
            for (uint32_t i = 0; i < arrivedIds.size(); i++) {
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
                inboxMessage.SetRelLabel("");
                inboxMessage.SetLabel(inboxTransportMessage.GetLabel());
                inboxMessage.SetContent(inboxTransportMessage.GetContent());
                data = inboxMessage.Serialize(&dataLength);
                NAEEM_os__write_to_file (
                  (NAEEM_path)(workDir + "/qfp").c_str(), 
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
                  NAEEM_os__delete_file (
                    (NAEEM_path)(workDir + "/a").c_str(), 
                    (NAEEM_string)ss.str().c_str()
                  );
                }
                uint16_t status = (uint16_t)::ir::ntnaeem::gate::transport::kTransportMessageStatus___ReadyForPop;
                NAEEM_os__write_to_file (
                  (NAEEM_path)(workDir + "/s").c_str(), 
                  (NAEEM_string)ss.str().c_str(),
                  (NAEEM_data)(&status),
                  sizeof(status)
                );
                Runtime::states_[inboxMessage.GetId().GetValue()] = status;
                Runtime::readyForPop_.push_back(inboxMessage.GetId().GetValue());
                uint32_t slaveId = inboxTransportMessage.GetSlaveId().GetValue();
                NAEEM_os__write_to_file (
                  (NAEEM_path)(workDir + "/s").c_str(), 
                  (NAEEM_string)(ss.str() + ".slaveid").c_str(),
                  (NAEEM_data)(&slaveId),
                  sizeof(slaveId)
                );
                uint64_t slaveMId = inboxTransportMessage.GetSlaveMId().GetValue();
                NAEEM_os__write_to_file (
                  (NAEEM_path)(workDir + "/s").c_str(), 
                  (NAEEM_string)(ss.str() + ".slavemid").c_str(),
                  (NAEEM_data)(&slaveMId),
                  sizeof(slaveMId)
                );
              } else {
                // TODO : Message is not deserialized.
              }
              // Runtime::inboxQueue_->Put(inboxMessage->GetLabel().ToStdString(), inboxMessage);
              // Runtime::slaveMessageMap_.insert(
              //  std::pair<uint64_t, uint64_t>(inboxTransportMessage->GetMasterMId().GetValue(), 
              //    inboxTransportMessage->GetSlaveId().GetValue()));
              // if (Runtime::masterIdToSlaveIdMap_.find(inboxTransportMessage->GetSlaveId().GetValue()) == 
              //     Runtime::masterIdToSlaveIdMap_.end()) {
              //   Runtime::masterIdToSlaveIdMap_.insert(
              //     std::pair<uint64_t, std::map<uint64_t, uint64_t>*>(
              //       inboxTransportMessage->GetSlaveId().GetValue(), 
              //         new std::map<uint64_t, uint64_t>()));
              // }
              // Runtime::masterIdToSlaveIdMap_[inboxTransportMessage->GetSlaveId().GetValue()]->insert(
              //   std::pair<uint64_t, uint64_t>(inboxTransportMessage->GetMasterMId().GetValue(), 
              //     inboxTransportMessage->GetSlaveMId().GetValue()));
            }
          }
          if (::naeem::hottentot::runtime::Configuration::Verbose()) {
            ::naeem::hottentot::runtime::Logger::GetOut() << "Messages moved from transport inbox to gate inbox." << std::endl;
          }
          /* 
           * Copying from 'outbox queue' to 'transport outbox queue'
           */
          {
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