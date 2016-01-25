#ifndef _IR_NTNAEEM_GATE__MASTER__RUNTIME_H_
#define _IR_NTNAEEM_GATE__MASTER__RUNTIME_H_

#include <mutex>

#include "../common/gate/message.h"
#include "../common/transport/transport_message.h"

#include "queue.h"
#include "bag.h"
#include "label_queue_map.h"
#include "slave_bag_map.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace master {
  class Runtime {
  public:
    static void Init();
  public:
    static uint32_t messageCounter_;
    static std::mutex counterLock_;
    static std::mutex mainLock_;
    static std::mutex inboxQueueLock_;
    static std::mutex outboxQueueLock_;
    static std::mutex transportInboxQueueLock_;
    static std::mutex transportOutboxQueueLock_;
    static LabelQueueMap< ::ir::ntnaeem::gate::Message> *inboxQueue_;
    static Bag< ::ir::ntnaeem::gate::Message> *outboxQueue_;
    static Bag< ::ir::ntnaeem::gate::transport::TransportMessage> *transportInboxQueue_;
    static SlaveBagMap< ::ir::ntnaeem::gate::transport::TransportMessage> *transportOutboxQueue_;
    static Bag< ::ir::ntnaeem::gate::transport::TransportMessage> *transportSentQueue_;
  };
}
}
}
}

#endif