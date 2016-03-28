#include <sstream>

#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace slave {
  std::mutex Runtime::termSignalLock_;
  bool Runtime::termSignal_;
  bool Runtime::slaveThreadTerminated_;
  
  std::mutex Runtime::counterLock_;
  uint64_t Runtime::messageCounter_ = 0;
  uint64_t Runtime::transmittedCounter_ = 0;

  std::mutex Runtime::mainLock_;
  std::mutex Runtime::inboxQueueLock_;
  std::mutex Runtime::outboxQueueLock_;
  LabelQueueMap< ::ir::ntnaeem::gate::Message>* Runtime::inboxQueue_ = NULL;
  // Bag< ::ir::ntnaeem::gate::Message>* Runtime::outboxQueue_ = NULL;
  std::vector<uint64_t> Runtime::outbox_;
  // Bag< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::sentQueue_ = NULL;
  Bag< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::failedQueue_ = NULL;
  std::map<uint64_t, ::ir::ntnaeem::gate::MessageStatus> Runtime::states_;

  void
  Runtime::Init() {
    termSignal_ = false;
    slaveThreadTerminated_ = false;

    messageCounter_ = 1000;
    transmittedCounter_ = 0;

    inboxQueue_ = new LabelQueueMap< ::ir::ntnaeem::gate::Message>;
    // outboxQueue_ = new Bag< ::ir::ntnaeem::gate::Message>;
    // sentQueue_ = new Bag< ::ir::ntnaeem::gate::transport::TransportMessage>;
    failedQueue_ = new Bag< ::ir::ntnaeem::gate::transport::TransportMessage>;
  }
  void
  Runtime::Shutdown() {
    inboxQueue_->Purge();
    // outboxQueue_->Purge();
    // sentQueue_->Purge();
    failedQueue_->Purge();
    delete inboxQueue_;
    // delete outboxQueue_;
    // delete sentQueue_;
    delete failedQueue_;
  }
  std::string
  Runtime::GetCurrentStat() {
    std::stringstream ss;
    ss << "------------------------------" << std::endl;
    ss << "MESSAGE COUNTER: " << messageCounter_ << std::endl;
    ss << "Size(Runtime::inboxQueue_): " << Runtime::inboxQueue_->Size() << std::endl;
    for (std::map<std::string, Queue<::ir::ntnaeem::gate::Message>*>::iterator it = Runtime::inboxQueue_->queuesMap_.begin();
         it != Runtime::inboxQueue_->queuesMap_.end();
         it++) {
      ss << "  Size(Runtime::inboxQueue_['" << it->first << "']): " << it->second->Size() << std::endl;
    }
    ss << "Size(Runtime::outboxQueue_): " << Runtime::outbox_.size() << std::endl;
    // ss << "Size(Runtime::sentQueue_): " << Runtime::sentQueue_->Size() << std::endl;
    ss << "Number of Transmitted Messages: " << Runtime::transmittedCounter_ << std::endl;
    ss << "Size(Runtime::failedQueue_): " << Runtime::failedQueue_->Size() << std::endl;
    ss << "------------------------------" << std::endl;
    return ss.str();
  }
}
}
}
}