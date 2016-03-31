#include <sstream>

#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace slave {
  
  bool Runtime::termSignal_;
  bool Runtime::slaveThreadTerminated_;

  uint64_t Runtime::inboxMessageCounter_ = 0;
  uint64_t Runtime::outboxMessageCounter_ = 0;
  uint64_t Runtime::messageIdCounter_ = 0;
  uint64_t Runtime::transmittedCounter_ = 0;
  uint64_t Runtime::transmissionFailureCounter_ = 0;

  std::mutex Runtime::termSignalLock_;
  std::mutex Runtime::messageIdCounterLock_;
  std::mutex Runtime::mainLock_;
  std::mutex Runtime::inboxLock_;
  std::mutex Runtime::outboxLock_;

  std::map<uint64_t, uint16_t> Runtime::states_;
  std::map<std::string, std::vector<uint64_t>*> Runtime::inbox_;
  std::vector<uint64_t> Runtime::outbox_;

  // LabelQueueMap< ::ir::ntnaeem::gate::Message>* Runtime::inboxQueue_ = NULL;
  // Bag< ::ir::ntnaeem::gate::Message>* Runtime::outboxQueue_ = NULL;
  // Bag< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::sentQueue_ = NULL;
  // Bag< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::failedQueue_ = NULL;
  
  void
  Runtime::Init() {
    termSignal_ = false;
    slaveThreadTerminated_ = false;

    inboxMessageCounter_ = 0;
    outboxMessageCounter_ = 0;
    messageIdCounter_ = 1000;
    transmittedCounter_ = 0;
    transmissionFailureCounter_ = 0;

    // inboxQueue_ = new LabelQueueMap< ::ir::ntnaeem::gate::Message>;
    // outboxQueue_ = new Bag< ::ir::ntnaeem::gate::Message>;
    // sentQueue_ = new Bag< ::ir::ntnaeem::gate::transport::TransportMessage>;
    // failedQueue_ = new Bag< ::ir::ntnaeem::gate::transport::TransportMessage>;
  }
  void
  Runtime::Shutdown() {
    for (std::map<std::string, std::vector<uint64_t>*>::iterator it = Runtime::inbox_.begin();
         it != Runtime::inbox_.end();
        ) {
      delete it->second;
      Runtime::inbox_.erase(it++);
    }
    // inboxQueue_->Purge();
    // outboxQueue_->Purge();
    // sentQueue_->Purge();
    // failedQueue_->Purge();
    // delete inboxQueue_;
    // delete outboxQueue_;
    // delete sentQueue_;
    // delete failedQueue_;
  }
  std::string
  Runtime::GetCurrentStat() {
    std::stringstream ss;
    ss << "------------------------------" << std::endl;
    ss << "MESSAGE ID COUNTER: " << messageIdCounter_ << std::endl;
    ss << "# TOTAL RECEIVED: " << inboxMessageCounter_ << std::endl;
    ss << "# RECEIVED LABELS: " << Runtime::inbox_.size() << std::endl;
    for (std::map<std::string, std::vector<uint64_t>*>::iterator it = Runtime::inbox_.begin();
         it != Runtime::inbox_.end();
         it++) {
      ss << "  # LABEL['" << it->first << "']: " << it->second->size() << std::endl;
    }
    ss << "# TOTAL ENQUEUED: " << outboxMessageCounter_ << std::endl;
    ss << "# TRANSMITTED: " << Runtime::transmittedCounter_ << std::endl;
    ss << "# WAITING FOR TRANSMISSION: " << Runtime::outbox_.size() << std::endl;
    ss << "# TRANSMISSION FAILURE: " << Runtime::transmissionFailureCounter_<< std::endl;
    ss << "------------------------------" << std::endl;
    return ss.str();
  }
}
}
}
}