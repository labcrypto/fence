#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
  uint32_t Runtime::messageCounter_ = 0;
  std::mutex Runtime::gateServiceLock_;
  Queue< ::ir::ntnaeem::gate::Message>* Runtime::queue_ = NULL;
}
}
}