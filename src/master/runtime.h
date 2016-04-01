#ifndef _IR_NTNAEEM_GATE__MASTER__RUNTIME_H_
#define _IR_NTNAEEM_GATE__MASTER__RUNTIME_H_

#include <deque>
#include <map>
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
    
    static bool termSignal_;
    static bool masterThreadTerminated_;

    static uint64_t messageIdCounter_;
    static uint64_t arrivedTotalCounter_;
    static uint64_t readyForPopTotalCounter_;
    static uint64_t poppedAndAckedTotalCounter_;
    static uint64_t enqueuedTotalCounter_;
    static uint64_t enqueueFailedTotalCounter_;
    
    static std::mutex termSignalLock_;
    static std::mutex messageIdCounterLock_;
    static std::mutex mainLock_;
    static std::mutex readyForPopLock_;
    static std::mutex arrivedLock_;
    static std::mutex enqueueLock_;

    static std::mutex transportOutboxQueueLock_;

    static std::vector<uint64_t> arrived_;
    static std::vector<uint64_t> enqueued_;
    static std::map<std::string, std::map<uint64_t, uint64_t>*> poppedButNotAcked_;
    static std::map<std::string, std::deque<uint64_t>*> readyForPop_;
    static std::map<uint64_t, uint16_t> states_;

    // static std::map<uint32_t, uint64_t> slaveMessageMap_; // TODO: Replace with a persistent map
    // static std::map<uint32_t, std::map<uint64_t, uint64_t>*> masterIdToSlaveIdMap_; // TODO: Replace with a persistent map
    // static LabelQueueMap< ::ir::ntnaeem::gate::Message> *inboxQueue_;
    // static Bag< ::ir::ntnaeem::gate::Message> *outboxQueue_;
    // static Bag< ::ir::ntnaeem::gate::transport::TransportMessage> *transportInboxQueue_;
    static SlaveBagMap< ::ir::ntnaeem::gate::transport::TransportMessage> *transportOutboxQueue_;
    static Bag< ::ir::ntnaeem::gate::transport::TransportMessage> *transportSentQueue_;
  };
}
}
}
}

#endif
