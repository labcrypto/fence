#ifndef _IR_NTNAEEM_GATE__MASTER__RUNTIME_H_
#define _IR_NTNAEEM_GATE__MASTER__RUNTIME_H_

#include <mutex>

#include <gate/message.h>
#include <transport/transport_message.h>

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
    static void Shutdown();
    static std::string GetCurrentStat();
  public:
    static std::mutex termSignalLock_;
    static bool termSignal_;
    static bool masterThreadTerminated_;

    static std::mutex counterLock_;
    static uint64_t messageCounter_;
    
    static std::mutex mainLock_;
    static std::mutex inboxQueueLock_;
    static std::mutex outboxQueueLock_;
    static std::mutex transportInboxQueueLock_;
    static std::mutex transportOutboxQueueLock_;

    static std::map<uint32_t, uint64_t> slaveMessageMap_; // TODO: Replace with a persistent map
    static std::map<uint32_t, std::map<uint64_t, uint64_t>*> masterIdToSlaveIdMap_; // TODO: Replace with a persistent map
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