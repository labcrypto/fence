#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
  uint32_t Runtime::messageCounter_ = 0;
  std::mutex Runtime::mainLock_;
  Queue< ::ir::ntnaeem::gate::Message>* Runtime::mainQueue_ = NULL;
  Queue< ::ir::ntnaeem::gate::Message>* Runtime::sentQueue_ = NULL;
}
}
}