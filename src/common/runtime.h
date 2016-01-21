#ifndef _IR_NTNAEEM_GATE__RUNTIME_H_
#define _IR_NTNAEEM_GATE__RUNTIME_H_

#include <mutex>

#include "gate/message.h"
#include "queue.h"


namespace ir {
namespace ntnaeem {
namespace gate {
  class Runtime {
  public:
    static uint32_t messageCounter_;
    static std::mutex gateServiceLock_;
    static Queue< ::ir::ntnaeem::gate::Message> *queue_;
  };
}
}
}

#endif