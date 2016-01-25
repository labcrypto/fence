#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace master {
  uint32_t Runtime::messageCounter_ = 0;
  std::mutex Runtime::counterLock_;
  std::mutex Runtime::mainLock_;
  std::mutex Runtime::inboxQueueLock_;
  std::mutex Runtime::outboxQueueLock_;
  std::mutex Runtime::transportInboxQueueLock_;
  std::mutex Runtime::transportOutboxQueueLock_;
  LabelQueueMap< ::ir::ntnaeem::gate::Message>* Runtime::inboxQueue_ = NULL;
  Bag< ::ir::ntnaeem::gate::Message>* Runtime::outboxQueue_ = NULL;
  Bag< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::transportInboxQueue_ = NULL;
  SlaveBagMap< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::transportOutboxQueue_ = NULL;
  Bag< ::ir::ntnaeem::gate::transport::TransportMessage>* Runtime::transportSentQueue_ = NULL;
  void
  Runtime::Init() {
    messageCounter_ = 1000;
    inboxQueue_ = new LabelQueueMap< ::ir::ntnaeem::gate::Message>;
    outboxQueue_ = new Bag< ::ir::ntnaeem::gate::Message>;
    transportInboxQueue_ = new Bag< ::ir::ntnaeem::gate::transport::TransportMessage>;
    transportOutboxQueue_ = new SlaveBagMap< ::ir::ntnaeem::gate::transport::TransportMessage>;
    transportSentQueue_ = new Bag< ::ir::ntnaeem::gate::transport::TransportMessage>;
  }
}
}
}
}