#ifndef _IR_NTNAEEM_GATE__RUNTIME_H_
#define _IR_NTNAEEM_GATE__RUNTIME_H_

#include <mutex>

#include "../common/gate/message.h"
#include "../common/transport/transport_message.h"
#include "queue.h"


namespace ir {
namespace ntnaeem {
namespace gate {
  class Runtime {
  public:
    static uint32_t messageCounter_;
    static std::mutex counterLock_;
    static std::mutex mainLock_;
    static Queue< ::ir::ntnaeem::gate::Message> *inboxQueue_;
    static Queue< ::ir::ntnaeem::gate::Message> *outboxQueue_;
    static Queue< ::ir::ntnaeem::gate::transport::TransportMessage> *sentQueue_;
  };
}
}
}

#endif