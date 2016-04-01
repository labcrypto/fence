#include <sstream>

#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace master {
  
  bool Runtime::termSignal_;
  bool Runtime::masterThreadTerminated_;

  uint64_t Runtime::messageIdCounter_ = 0;
  uint64_t Runtime::arrivedTotalCounter_ = 0;
  uint64_t Runtime::readyForPopTotalCounter_ = 0;
  uint64_t Runtime::poppedAndAckedTotalCounter_ = 0;
  uint64_t Runtime::enqueuedTotalCounter_ = 0;
  uint64_t Runtime::enqueueFailedTotalCounter_ = 0;

  std::mutex Runtime::termSignalLock_;
  std::mutex Runtime::messageIdCounterLock_;
  std::mutex Runtime::mainLock_;
  std::mutex Runtime::readyForPopLock_;
  std::mutex Runtime::arrivedLock_;
  std::mutex Runtime::enqueueLock_;
  
  std::mutex Runtime::transportOutboxQueueLock_;

  std::vector<uint64_t> Runtime::arrived_;
  std::vector<uint64_t> Runtime::enqueued_;
  std::map<std::string, std::map<uint64_t, uint64_t>*> Runtime::poppedButNotAcked_;
  std::map<std::string, std::deque<uint64_t>*> Runtime::readyForPop_;
  std::map<uint64_t, uint16_t> Runtime::states_;

  // std::map<uint32_t, uint64_t> Runtime::slaveMessageMap_;
  // std::map<uint32_t, std::map<uint64_t, uint64_t>*> Runtime::masterIdToSlaveIdMap_;
  // LabelQueueMap< ::ir::ntnaeem::gate::Message>* Runtime::inboxQueue_ = NULL;
  // Bag< ::ir::ntnaeem::gate::Message>* Runtime::outboxQueue_ = NULL;
  // Bag< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::transportInboxQueue_ = NULL;
  SlaveBagMap< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::transportOutboxQueue_ = NULL;
  Bag< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::transportSentQueue_ = NULL;

  void
  Runtime::Init() {
    termSignal_ = false;
    masterThreadTerminated_ = false;

    messageIdCounter_ = 5000;

    // inboxQueue_ = new LabelQueueMap< ::ir::ntnaeem::gate::Message>;
    // outboxQueue_ = new Bag< ::ir::ntnaeem::gate::Message>;
    // transportInboxQueue_ = new Bag< ::ir::ntnaeem::gate::transport::TransportMessage>;
    transportOutboxQueue_ = new SlaveBagMap< ::ir::ntnaeem::gate::transport::TransportMessage>;
    transportSentQueue_ = new Bag< ::ir::ntnaeem::gate::transport::TransportMessage>;
  }
  void
  Runtime::Shutdown() {
    for (std::map<std::string, std::deque<uint64_t>*>::iterator it = Runtime::readyForPop_.begin();
         it != Runtime::readyForPop_.end();
        ) {
      delete it->second;
      Runtime::readyForPop_.erase(it++);
    }
    for (std::map<std::string, std::map<uint64_t, uint64_t>*>::iterator it = Runtime::poppedButNotAcked_.begin();
         it != Runtime::poppedButNotAcked_.end();
        ) {
      delete it->second;
      Runtime::poppedButNotAcked_.erase(it++);
    }
    // delete inboxQueue_;
    // delete outboxQueue_;
    // delete transportInboxQueue_;
    delete transportOutboxQueue_;
    delete transportSentQueue_;
  }
  std::string
  Runtime::GetCurrentStat() {
    std::stringstream ss;
    ss << "------------------------------" << std::endl;
    ss << "MESSAGE ID COUNTER: " << messageIdCounter_ << std::endl;
    /*ss << "Size(Runtime::slaveMessageMap_): " << Runtime::slaveMessageMap_.size() << std::endl;
    for (auto &kv : Runtime::slaveMessageMap_) {
      ss << "  '" << kv.first << "' -> '" << kv.second << "'" << std::endl;
    }
    ss << "Size(Runtime::masterIdToSlaveIdMap_): " << Runtime::masterIdToSlaveIdMap_.size() << std::endl;
    for (auto &kv : Runtime::masterIdToSlaveIdMap_) {
      ss << " For slave '" << kv.first << "': " << std::endl;
      for (auto &kv2 : *(kv.second)) {
        ss << "    '" << kv2.first << "' -> '" << kv2.second << "'" << std::endl;
      }
    }
    ss << "Size(Runtime::inboxQueue_): " << Runtime::inboxQueue_->Size() << " labels." << std::endl;*/
    /*for (std::map<std::string, Queue<::ir::ntnaeem::gate::Message>*>::iterator it = Runtime::inboxQueue_->queuesMap_.begin();
         it != Runtime::inboxQueue_->queuesMap_.end();
         it++) {
      ss << "  Size(Runtime::inboxQueue_['" << it->first << "']): " << it->second->Size() << std::endl;
    }*/
    
    ss << "# ARRIVED: " << Runtime::arrived_.size() << std::endl;
    uint64_t sumOfReadyForPop = 0;
    for (std::map<std::string, std::deque<uint64_t>*>::iterator it = Runtime::readyForPop_.begin();
         it != Runtime::readyForPop_.end();
         it++) {
      sumOfReadyForPop += it->second->size();
    }
    ss << "# READY FOR POP: " << sumOfReadyForPop << std::endl;
    for (std::map<std::string, std::deque<uint64_t>*>::iterator it = Runtime::readyForPop_.begin();
         it != Runtime::readyForPop_.end();
         it++) {
      ss << "  # LABEL['" << it->first << "']: " << it->second->size() << std::endl;;
    }
    uint64_t sumOfPoppedButNotAcked = 0;
    for (std::map<std::string, std::map<uint64_t, uint64_t>*>::iterator it = Runtime::poppedButNotAcked_.begin();
         it != Runtime::poppedButNotAcked_.end();
         it++) {
      sumOfPoppedButNotAcked += it->second->size();
    }
    ss << "# POPPED BUT NOT ACKED: " << sumOfPoppedButNotAcked << std::endl;
    for (std::map<std::string, std::map<uint64_t, uint64_t>*>::iterator it = Runtime::poppedButNotAcked_.begin();
         it != Runtime::poppedButNotAcked_.end();
         it++) {
      ss << "  # LABEL['" << it->first << "']: " << it->second->size() << std::endl;;
    }
    ss << "# ENQUEUED: " << Runtime::enqueued_.size() << std::endl;
    ss << "---" << std::endl;
    ss << "# TOTAL ARRIVED: " << Runtime::arrivedTotalCounter_ << std::endl;
    ss << "# TOTAL READY FOR POP: " << Runtime::readyForPopTotalCounter_ << std::endl;
    ss << "# TOTAL POPPED AND ACKED: " << Runtime::poppedAndAckedTotalCounter_ << std::endl;
    ss << "# TOTAL ENQUEUED: " << Runtime::enqueuedTotalCounter_ << std::endl;
    ss << "# TOTAL ENQUEUE FAILED: " << Runtime::enqueueFailedTotalCounter_ << std::endl;
    /* ss << "Size(Runtime::outboxQueue_): " << Runtime::outboxQueue_->Size() << std::endl;
    ss << "Size(Runtime::transportOutboxQueue_): " << Runtime::transportOutboxQueue_->Size() << " slaves." << std::endl;
    for (std::map<uint32_t, Bag<::ir::ntnaeem::gate::transport::TransportMessage>*>::iterator it = Runtime::transportOutboxQueue_->maps_.begin();
         it != Runtime::transportOutboxQueue_->maps_.end();
         it++) {
      ss << "  Size(Runtime::transportOutboxQueue_['" << it->first << "']): " << it->second->Size() << std::endl;
    }
    ss << "Size(Runtime::transportSentQueue_): " << Runtime::transportSentQueue_->Size() << std::endl; */
    ss << "------------------------------" << std::endl;
    return ss.str();
  }
}
}
}
}