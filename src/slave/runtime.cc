#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace slave {
  uint64_t Runtime::messageCounter_ = 0;
  std::mutex Runtime::counterLock_;
  std::mutex Runtime::mainLock_;
  std::mutex Runtime::inboxQueueLock_;
  std::mutex Runtime::outboxQueueLock_;
  LabelQueueMap< ::ir::ntnaeem::gate::Message>* Runtime::inboxQueue_ = NULL;
  Bag< ::ir::ntnaeem::gate::Message>* Runtime::outboxQueue_ = NULL;
  Bag< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::sentQueue_ = NULL;
  Bag< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::failedQueue_ = NULL;
  void
  Runtime::Init() {
    messageCounter_ = 1000;
    inboxQueue_ = new LabelQueueMap< ::ir::ntnaeem::gate::Message>;
    outboxQueue_ = new Bag< ::ir::ntnaeem::gate::Message>;
    sentQueue_ = new Bag< ::ir::ntnaeem::gate::transport::TransportMessage>;
    failedQueue_ = new Bag< ::ir::ntnaeem::gate::transport::TransportMessage>;
  }
}
}
}
}