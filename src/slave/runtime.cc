#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace slave {
  uint32_t Runtime::messageCounter_ = 0;
  std::mutex Runtime::counterLock_;
  std::mutex Runtime::mainLock_;
  ::ir::ntnaeem::gate::Queue< ::ir::ntnaeem::gate::Message>* Runtime::inboxQueue_ = NULL;
  ::ir::ntnaeem::gate::Queue< ::ir::ntnaeem::gate::Message>* Runtime::outboxQueue_ = NULL;
  ::ir::ntnaeem::gate::Queue< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::sentQueue_ = NULL;
}
}
}
}