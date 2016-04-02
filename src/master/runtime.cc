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
  uint64_t Runtime::readyForRetrievalTotalCounter_ = 0;
  uint64_t Runtime::retrievedAndAckedTotalCounter_ = 0;

  std::mutex Runtime::termSignalLock_;
  std::mutex Runtime::messageIdCounterLock_;
  std::mutex Runtime::mainLock_;
  std::mutex Runtime::readyForPopLock_;
  std::mutex Runtime::arrivedLock_;
  std::mutex Runtime::enqueueLock_;
  std::mutex Runtime::readyForRetrievalLock_;

  std::map<uint64_t, uint16_t> Runtime::states_;
  std::vector<uint64_t> Runtime::arrived_;
  std::vector<uint64_t> Runtime::enqueued_;
  std::map<std::string, std::map<uint64_t, uint64_t>*> Runtime::poppedButNotAcked_;
  std::map<std::string, std::deque<uint64_t>*> Runtime::readyForPop_;
  std::map<uint32_t, std::vector<uint64_t>*> Runtime::readyForRetrieval_;
  std::map<uint32_t, std::map<uint64_t, uint64_t>*> Runtime::retrievedButNotAcked_;
  
  void
  Runtime::Init() {
    termSignal_ = false;
    masterThreadTerminated_ = false;
    messageIdCounter_ = 5000;
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
    for (std::map<uint32_t, std::vector<uint64_t>*>::iterator it = Runtime::readyForRetrieval_.begin();
         it != Runtime::readyForRetrieval_.end();
        ) {
      delete it->second;
      Runtime::readyForRetrieval_.erase(it++);
    }
    for (std::map<uint32_t, std::map<uint64_t, uint64_t>*>::iterator it = Runtime::retrievedButNotAcked_.begin();
         it != Runtime::retrievedButNotAcked_.end();
        ) {
      delete it->second;
      Runtime::retrievedButNotAcked_.erase(it++);
    }
  }
  std::string
  Runtime::GetCurrentStat() {
    std::stringstream ss;
    ss << "------------------------------" << std::endl;
    ss << "MESSAGE ID COUNTER: " << messageIdCounter_ << std::endl;
    ss << "# ARRIVED: " << Runtime::arrived_.size() << std::endl;
    ss << "# ENQUEUED: " << Runtime::enqueued_.size() << std::endl;
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
      ss << "  # LABEL['" << it->first << "']: " << it->second->size() << std::endl;
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
      ss << "  # LABEL['" << it->first << "']: " << it->second->size() << std::endl;
    }
    uint64_t sumOfReadyForRetrieval = 0;
    for (std::map<uint32_t, std::vector<uint64_t>*>::iterator it = Runtime::readyForRetrieval_.begin();
         it != Runtime::readyForRetrieval_.end();
         it++) {
      sumOfReadyForRetrieval += it->second->size();
    }
    ss << "# READY FOR RETRIEVAL: " << sumOfReadyForRetrieval << std::endl;
    for (std::map<uint32_t, std::vector<uint64_t>*>::iterator it = Runtime::readyForRetrieval_.begin();
         it != Runtime::readyForRetrieval_.end();
         it++) {
      ss << "  # SLAVE['" << it->first << "']: " << it->second->size() << std::endl;
    }
    uint64_t sumOfRetrievedButNotAcked = 0;
    for (std::map<uint32_t, std::map<uint64_t, uint64_t>*>::iterator it = Runtime::retrievedButNotAcked_.begin();
         it != Runtime::retrievedButNotAcked_.end();
         it++) {
      sumOfRetrievedButNotAcked += it->second->size();
    }
    ss << "# RERIEVED BUT NOT ACKED: " << sumOfRetrievedButNotAcked << std::endl;
    for (std::map<uint32_t, std::map<uint64_t, uint64_t>*>::iterator it = Runtime::retrievedButNotAcked_.begin();
         it != Runtime::retrievedButNotAcked_.end();
         it++) {
      ss << "  # LABEL['" << it->first << "']: " << it->second->size() << std::endl;
    }
    ss << "---" << std::endl;
    ss << "# TOTAL ARRIVED: " << Runtime::arrivedTotalCounter_ << std::endl;
    ss << "# TOTAL READY FOR POP: " << Runtime::readyForPopTotalCounter_ << std::endl;
    ss << "# TOTAL POPPED AND ACKED: " << Runtime::poppedAndAckedTotalCounter_ << std::endl;
    ss << "# TOTAL ENQUEUED: " << Runtime::enqueuedTotalCounter_ << std::endl;
    ss << "# TOTAL ENQUEUE FAILED: " << Runtime::enqueueFailedTotalCounter_ << std::endl;
    ss << "# TOTAL READY FOR RETRIEVAL: " << Runtime::readyForRetrievalTotalCounter_ << std::endl;
    ss << "------------------------------" << std::endl;
    return ss.str();
  }
}
}
}
}