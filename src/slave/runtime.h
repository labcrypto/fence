#ifndef _IR_NTNAEEM_GATE__RUNTIME_H_
#define _IR_NTNAEEM_GATE__RUNTIME_H_

#include <vector>
#include <map>
#include <mutex>

#include <gate/message.h>
#include <transport/transport_message.h>

// #include "label_queue_map.h"
// #include "bag.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace slave {
  class Runtime {
  public:
    static void Init();
    static void Shutdown();
    static std::string GetCurrentStat();
  public:
    
    static bool termSignal_;
    static bool slaveThreadTerminated_;

    static uint64_t outboxMessageCounter_;
    static uint64_t inboxMessageCounter_;
    static uint64_t messageIdCounter_;
    static uint64_t transmittedCounter_;
    static uint64_t transmissionFailureCounter_;
    
    static std::mutex termSignalLock_;
    static std::mutex messageIdCounterLock_;
    static std::mutex mainLock_;
    static std::mutex inboxLock_;
    static std::mutex outboxLock_;
    
    static std::map<uint64_t, ::ir::ntnaeem::gate::MessageStatus> states_;
    static std::map<std::string, std::vector<uint64_t>*> inbox_;
    static std::vector<uint64_t> outbox_;

    //static LabelQueueMap< ::ir::ntnaeem::gate::Message> *inboxQueue_;
    // static Bag< ::ir::ntnaeem::gate::Message> *outboxQueue_;
    // static Bag< ::ir::ntnaeem::gate::transport::TransportMessage> *sentQueue_;
    // static Bag< ::ir::ntnaeem::gate::transport::TransportMessage> *failedQueue_;
    
  };
}
}
}
}

#endif
