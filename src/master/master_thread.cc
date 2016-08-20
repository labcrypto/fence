#include <thread>
#include <chrono>
#include <iostream>
#include <sstream>

#include <org/labcrypto/hottentot/runtime/configuration.h>
#include <org/labcrypto/hottentot/runtime/logger.h>
#include <org/labcrypto/hottentot/runtime/proxy/proxy_runtime.h>

#include <org/labcrypto/abettor/fs.h>

#include <org/labcrypto/abettor++/conf/config_manager.h>
#include <org/labcrypto/abettor++/date/helper.h>

#include <fence/message.h>

#include <transport/transport_message.h>

#include "master_thread.h"
#include "runtime.h"


namespace org {
namespace labcrypto {
namespace fence {
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
    uint32_t transferInterval = ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("master", "transfer_interval");
    std::string workDir = ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("master", "work_dir");
    while (cont) {
      {
        std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
        if (Runtime::termSignal_) {
          if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
            ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
              "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                "Master Thread: Received TERM SIGNAL ..." << std::endl;
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
          if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
            ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
              "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                "Master Thread: Received TERM SIGNAL ..." << std::endl;
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
          if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
            ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
              "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                "VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV" << std::endl;
          }
          if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
            ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
              "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                "Waiting for main lock ..." << std::endl;
          }
          /*
           * Aquiring main lock by creating guard object
           */
          std::lock_guard<std::mutex> guard(Runtime::mainLock_);
          if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
            ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
              "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                "Main lock is acquired." << std::endl;
            ::org::labcrypto::hottentot::runtime::Logger::GetOut() << Runtime::GetCurrentStat();
          }
          /*
           * Copying from 'transport inbox queue' into 'inbox queue'
           */
          {
            std::vector<uint64_t> arrivedIds = std::move(Runtime::arrived_);
            if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
              ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                  "Number of transport inbox messages: " << arrivedIds.size() << std::endl;
            }
            for (uint64_t i = 0; i < arrivedIds.size(); i++) {
              std::stringstream ss;
              ss << arrivedIds[i];
              /*
               * Reading transport message file
               */
              bool fileIsRead = false;
              ORG_LABCRYPTO_ABETTOR_data data;
              ORG_LABCRYPTO_ABETTOR_length dataLength;
              try {
                if (ORG_LABCRYPTO_ABETTOR__fs__file_exists (
                      (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/a").c_str(), 
                      (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
                    )
                ) {
                  ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
                    (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/a").c_str(), 
                    (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                    &data, 
                    &dataLength
                  );
                  fileIsRead = true;
                } else {
                  // TODO: Message file is not found.
                  ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
                    "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                      "File does not exist." << std::endl;
                }
              } catch (std::exception &e) {
                ::org::labcrypto::hottentot::runtime::Logger::GetError() <<
                  "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                     "ERROR: " << e.what() << std::endl;
              } catch (...) {
                ::org::labcrypto::hottentot::runtime::Logger::GetError() <<
                  "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                     "ERROR: Unknown." << std::endl;
              }
              /*
               * Transport message deserialization
               */
              bool deserialized = false;
              ::org::labcrypto::fence::Message inboxMessage;
              ::org::labcrypto::fence::transport::TransportMessage inboxTransportMessage;
              if (fileIsRead) {
                try {
                  inboxTransportMessage.Deserialize(data, dataLength);
                  deserialized = true;
                } catch (std::exception &e) {
                  ::org::labcrypto::hottentot::runtime::Logger::GetError() <<
                    "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                      "ERROR: " << e.what() << std::endl;
                } catch (...) {
                  ::org::labcrypto::hottentot::runtime::Logger::GetError() << 
                    "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                      "ERROR: Unknown." << std::endl;
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
                ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                  (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/rfp").c_str(), 
                  (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(), 
                  data, 
                  dataLength
                );
                delete [] data;
                if (ORG_LABCRYPTO_ABETTOR__fs__file_exists (
                      (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/a").c_str(), 
                      (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
                    )
                ) {
                  ORG_LABCRYPTO_ABETTOR__fs__copy_file (
                    (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/a").c_str(),
                    (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                    (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/aa").c_str(),
                    (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
                  );
                  ORG_LABCRYPTO_ABETTOR__fs__delete_file (
                    (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/a").c_str(), 
                    (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
                  );
                }
                uint16_t status = 
                  (uint16_t)::org::labcrypto::fence::transport::kTransportMessageStatus___ReadyForPop;
                ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                  (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/s").c_str(), 
                  (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                  (ORG_LABCRYPTO_ABETTOR_data)(&status),
                  sizeof(status)
                );
                uint32_t slaveId = inboxTransportMessage.GetSlaveId().GetValue();
                ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                  (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/ss").c_str(), 
                  (ORG_LABCRYPTO_ABETTOR_string)(ss.str() + ".slaveid").c_str(),
                  (ORG_LABCRYPTO_ABETTOR_data)(&slaveId),
                  sizeof(slaveId)
                );
                uint64_t slaveMId = inboxTransportMessage.GetSlaveMId().GetValue();
                ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                  (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/ss").c_str(), 
                  (ORG_LABCRYPTO_ABETTOR_string)(ss.str() + ".slavemid").c_str(),
                  (ORG_LABCRYPTO_ABETTOR_data)(&slaveMId),
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
                ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                  (ORG_LABCRYPTO_ABETTOR_path)workDir.c_str(), 
                  (ORG_LABCRYPTO_ABETTOR_string)"rfptco", 
                  (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::readyForPopTotalCounter_), 
                  (ORG_LABCRYPTO_ABETTOR_length)sizeof(Runtime::readyForPopTotalCounter_)
                );
              } else {
                // TODO : Message is not deserialized.
              }
            }
          }
          if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
            ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
              "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                "Messages moved from transport inbox to fence inbox." << std::endl;
          }
          /* 
           * Copying 'enqueued' messages to 'ready for retrieval' queue
           */
          {
            std::vector<uint64_t> enqueuedIds = std::move(Runtime::enqueued_);
            if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
              ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
                "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                  "Number of enqueued messages: " << enqueuedIds.size() << std::endl;
            }
            for (uint64_t i = 0; i < enqueuedIds.size(); i++) {
              std::stringstream ss;
              ss << enqueuedIds[i];
              if (ORG_LABCRYPTO_ABETTOR__fs__file_exists (
                    (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/e").c_str(),
                    (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
                  )
              ) {
                ORG_LABCRYPTO_ABETTOR_data data;
                ORG_LABCRYPTO_ABETTOR_length dataLength;
                ORG_LABCRYPTO_ABETTOR__fs__read_file_with_base_dir (
                  (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/e").c_str(), 
                  (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                  &data, 
                  &dataLength
                );
                ::org::labcrypto::fence::Message message;
                message.Deserialize(data, dataLength);
                free(data);
                std::stringstream rss;
                rss << message.GetRelId().GetValue();
                if (ORG_LABCRYPTO_ABETTOR__fs__file_exists (
                      (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/ss").c_str(),
                      (ORG_LABCRYPTO_ABETTOR_string)(rss.str() + ".slaveid").c_str()
                    )
                ) {
                  uint32_t slaveId;
                  ORG_LABCRYPTO_ABETTOR__fs__read_file_into_buffer (
                    (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/ss/" + rss.str() + ".slaveid").c_str(),
                    (ORG_LABCRYPTO_ABETTOR_data)&slaveId,
                    0
                  );
                  if (ORG_LABCRYPTO_ABETTOR__fs__file_exists (
                        (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/ss").c_str(),
                        (ORG_LABCRYPTO_ABETTOR_string)(rss.str() + ".slavemid").c_str()
                      )
                  ) {
                    uint64_t slaveMId;
                    ORG_LABCRYPTO_ABETTOR__fs__read_file_into_buffer (
                      (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/ss/" + rss.str() + ".slavemid").c_str(),
                      (ORG_LABCRYPTO_ABETTOR_data)&slaveMId,
                      0
                    );
                    uint16_t status = 
                      (uint16_t)::org::labcrypto::fence::transport::kTransportMessageStatus___ReadyForRetrieval;
                    ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                      (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/s").c_str(), 
                      (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                      (ORG_LABCRYPTO_ABETTOR_data)(&status),
                      sizeof(status)
                    );
                    Runtime::states_[message.GetId().GetValue()] = status;
                    ORG_LABCRYPTO_ABETTOR__fs__copy_file (
                      (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/e").c_str(),
                      (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                      (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/ea").c_str(),
                      (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
                    );
                    ORG_LABCRYPTO_ABETTOR__fs__delete_file (
                      (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/e").c_str(), 
                      (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
                    );
                    ::org::labcrypto::fence::transport::TransportMessage transportMessage;
                    transportMessage.SetMasterMId(message.GetId());
                    transportMessage.SetSlaveId(slaveId);
                    transportMessage.SetSlaveMId(0);
                    transportMessage.SetRelMId(slaveMId);
                    transportMessage.SetLabel(message.GetLabel());
                    transportMessage.SetContent(message.GetContent());
                    data = transportMessage.Serialize(&dataLength);
                    ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                      (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/rfr").c_str(), 
                      (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
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
                    ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                      (ORG_LABCRYPTO_ABETTOR_path)workDir.c_str(), 
                      (ORG_LABCRYPTO_ABETTOR_string)"rfrtco", 
                      (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::readyForRetrievalTotalCounter_), 
                      (ORG_LABCRYPTO_ABETTOR_length)sizeof(Runtime::readyForRetrievalTotalCounter_)
                    );
                  } else {
                    // TODO: Message status file does not exist.
                  }
                } else {
                  uint16_t status = 
                    (uint16_t)::org::labcrypto::fence::transport::kTransportMessageStatus___EnqueueFailed;
                  ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                    (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/s").c_str(), 
                    (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                    (ORG_LABCRYPTO_ABETTOR_data)(&status),
                    sizeof(status)
                  );
                  Runtime::states_[message.GetId().GetValue()] = status;
                  ORG_LABCRYPTO_ABETTOR__fs__copy_file (
                    (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/e").c_str(),
                    (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str(),
                    (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/ea").c_str(),
                    (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
                  );
                  ORG_LABCRYPTO_ABETTOR__fs__delete_file (
                    (ORG_LABCRYPTO_ABETTOR_path)(workDir + "/e").c_str(), 
                    (ORG_LABCRYPTO_ABETTOR_string)ss.str().c_str()
                  );
                  Runtime::enqueueFailedTotalCounter_++;
                  ORG_LABCRYPTO_ABETTOR__fs__write_to_file (
                    (ORG_LABCRYPTO_ABETTOR_path)workDir.c_str(), 
                    (ORG_LABCRYPTO_ABETTOR_string)"eftco", 
                    (ORG_LABCRYPTO_ABETTOR_data)&(Runtime::enqueueFailedTotalCounter_), 
                    (ORG_LABCRYPTO_ABETTOR_length)sizeof(Runtime::enqueueFailedTotalCounter_)
                  );
                }
              } else {
                // TODO: Message file does not exist.
              }
            }
          }
          if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
            ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
              "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
                "Main lock is released." << std::endl;
          }
        }
      }
    }
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "Master thread is exiting ..." << std::endl;
    }
    std::lock_guard<std::mutex> guard(Runtime::termSignalLock_);
    Runtime::masterThreadTerminated_ = true;
    pthread_exit(NULL);
  }
} // END NAMESAPCE master
} // END NAMESAPCE fence
} // END NAMESPACE labcrypto
} // END NAMESAPCE org