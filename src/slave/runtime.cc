#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace slave {
  uint64_t Runtime::messageCounter_ = 0;
  std::mutex Runtime::counterLock_;
  std::mutex Runtime::mainLock_;
  std::mutex inboxQueueLock_;
  std::mutex outboxQueueLock_;
  LabelQueueMap< ::ir::ntnaeem::gate::Message>* Runtime::inboxQueue_ = NULL;
  Bag< ::ir::ntnaeem::gate::Message>* Runtime::outboxQueue_ = NULL;
  Bag< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::sentQueue_ = NULL;
}
}
}
}