#include <sstream>

#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace slave {
  
  bool Runtime::termSignal_;
  bool Runtime::slaveThreadTerminated_;

  uint64_t Runtime::messageIdCounter_ = 0;
  uint64_t Runtime::enqueuedTotalCounter_ = 0;
  uint64_t Runtime::readyForPopTotalCounter_ = 0;
  uint64_t Runtime::poppedAndAckedTotalCounter_ = 0;
  uint64_t Runtime::transmittedTotalCounter_ = 0;
  uint64_t Runtime::transmissionFailureTotalCounter_ = 0;

  std::mutex Runtime::termSignalLock_;
  std::mutex Runtime::messageIdCounterLock_;
  std::mutex Runtime::mainLock_;
  std::mutex Runtime::readyForPopLock_;
  std::mutex Runtime::enqueueLock_;

  std::map<uint64_t, uint16_t> Runtime::states_;
  std::map<std::string, std::vector<uint64_t>*> Runtime::inbox_;
  std::vector<uint64_t> Runtime::outbox_;
  std::map<std::string, std::map<uint64_t, uint64_t>*> Runtime::poppedButNotAcked_;
  std::map<std::string, std::deque<uint64_t>*> Runtime::readyForPop_;
  
  void
  Runtime::Init() {
    termSignal_ = false;
    slaveThreadTerminated_ = false;
    messageIdCounter_ = 1000;
  }
  void
  Runtime::Shutdown() {
    for (std::map<std::string, std::vector<uint64_t>*>::iterator it = Runtime::inbox_.begin();
         it != Runtime::inbox_.end();
        ) {
      delete it->second;
      Runtime::inbox_.erase(it++);
    }
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
  }
  std::string
  Runtime::GetCurrentStat() {
    std::stringstream ss;
    ss << "------------------------------" << std::endl;
    ss << "MESSAGE ID COUNTER: " << messageIdCounter_ << std::endl;
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
    ss << "# WAITING FOR TRANSMISSION: " << Runtime::outbox_.size() << std::endl;
    ss << "---" << std::endl;
    ss << "# TOTAL ENQUEUED: " << enqueuedTotalCounter_ << std::endl;
    ss << "# TOTAL TRANSMITTED: " << Runtime::transmittedTotalCounter_ << std::endl;
    ss << "# TOTAL TRANSMISSION FAILURE: " << Runtime::transmissionFailureTotalCounter_<< std::endl;
    ss << "# TOTAL READY FOR POP: " << Runtime::readyForPopTotalCounter_ << std::endl;
    ss << "# TOTAL POPPED AND ACKED: " << Runtime::poppedAndAckedTotalCounter_ << std::endl;
    ss << "------------------------------" << std::endl;
    return ss.str();
  }
}
}
}
}