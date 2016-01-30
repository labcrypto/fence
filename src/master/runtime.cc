#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace master {
  uint64_t Runtime::messageCounter_ = 0;
  std::mutex Runtime::counterLock_;
  std::mutex Runtime::mainLock_;
  std::mutex Runtime::inboxQueueLock_;
  std::mutex Runtime::outboxQueueLock_;
  std::mutex Runtime::transportInboxQueueLock_;
  std::mutex Runtime::transportOutboxQueueLock_;
  std::map<uint32_t, uint64_t> Runtime::slaveMessageMap_;
  std::map<uint32_t, std::map<uint64_t, uint64_t>*> Runtime::masterIdToSlaveIdMap_;
  LabelQueueMap< ::ir::ntnaeem::gate::Message>* Runtime::inboxQueue_ = NULL;
  Bag< ::ir::ntnaeem::gate::Message>* Runtime::outboxQueue_ = NULL;
  Bag< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::transportInboxQueue_ = NULL;
  SlaveBagMap< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::transportOutboxQueue_ = NULL;
  Bag< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::transportSentQueue_ = NULL;
  void
  Runtime::Init() {
    messageCounter_ = 5000;
    inboxQueue_ = new LabelQueueMap< ::ir::ntnaeem::gate::Message>;
    outboxQueue_ = new Bag< ::ir::ntnaeem::gate::Message>;
    transportInboxQueue_ = new Bag< ::ir::ntnaeem::gate::transport::TransportMessage>;
    transportOutboxQueue_ = new SlaveBagMap< ::ir::ntnaeem::gate::transport::TransportMessage>;
    transportSentQueue_ = new Bag< ::ir::ntnaeem::gate::transport::TransportMessage>;
  }
  void
  Runtime::PrintStatus() {
    std::cout << "------------------------------" << std::endl;
    std::cout << "MESSAGE COUNTER: " << messageCounter_ << std::endl;
    std::cout << "Size(Runtime::slaveMessageMap_): " << Runtime::slaveMessageMap_.size() << std::endl;
    for (auto &kv : Runtime::slaveMessageMap_) {
      std::cout << "  '" << kv.first << "' -> '" << kv.second << "'" << std::endl;
    }
    std::cout << "Size(Runtime::masterIdToSlaveIdMap_): " << Runtime::masterIdToSlaveIdMap_.size() << std::endl;
    for (auto &kv : Runtime::masterIdToSlaveIdMap_) {
      std::cout << " For slave '" << kv.first << "': " << std::endl;
      for (auto &kv2 : *(kv.second)) {
        std::cout << "    '" << kv2.first << "' -> '" << kv2.second << "'" << std::endl;
      }
    }
    std::cout << "Size(Runtime::inboxQueue_): " << Runtime::inboxQueue_->Size() << " labels." << std::endl;
    for (std::map<std::string, Queue<::ir::ntnaeem::gate::Message>*>::iterator it = Runtime::inboxQueue_->queuesMap_.begin();
         it != Runtime::inboxQueue_->queuesMap_.end();
         it++) {
      std::cout << "  Size(Runtime::inboxQueue_['" << it->first << "']): " << it->second->Size() << std::endl;
    }
    std::cout << "Size(Runtime::outboxQueue_): " << Runtime::outboxQueue_->Size() << std::endl;
    std::cout << "Size(Runtime::transportInboxQueue_): " << Runtime::transportInboxQueue_->Size() << std::endl;
    std::cout << "Size(Runtime::transportOutboxQueue_): " << Runtime::transportOutboxQueue_->Size() << " slaves." << std::endl;
    for (std::map<uint32_t, Bag<::ir::ntnaeem::gate::transport::TransportMessage>*>::iterator it = Runtime::transportOutboxQueue_->maps_.begin();
         it != Runtime::transportOutboxQueue_->maps_.end();
         it++) {
      std::cout << "  Size(Runtime::transportOutboxQueue_['" << it->first << "']): " << it->second->Size() << std::endl;
    }
    std::cout << "Size(Runtime::transportSentQueue_): " << Runtime::transportSentQueue_->Size() << std::endl;
    std::cout << "------------------------------" << std::endl;
  }
}
}
}
}