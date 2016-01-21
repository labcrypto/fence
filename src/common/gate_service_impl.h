#ifndef _IR_NTNAEEM_GATE__GATE_SERVICE_IMPL_H_
#define _IR_NTNAEEM_GATE__GATE_SERVICE_IMPL_H_

#include <stdint.h>
#include <string>

#include "gate/service/abstract_gate_service.h"


namespace ir {
namespace ntnaeem {
namespace gate {
  template<class M> class Queue;
  class GateServiceImpl : public ::ir::ntnaeem::gate::service::AbstractGateService {
  public:
    GateServiceImpl() {}
    virtual ~GateServiceImpl() {}
  public:
    virtual void OnInit();
    virtual void OnShutdown();
    virtual void EnqueueMessage(::ir::ntnaeem::gate::Message &message, ::naeem::hottentot::runtime::types::UInt32 &out);
    virtual void GetMessageStatus(::naeem::hottentot::runtime::types::UInt32 &id, ::naeem::hottentot::runtime::types::UInt32 &out);
    virtual void GetMessages(::naeem::hottentot::runtime::types::Utf8String &label, ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::Message> &out);
  private:
    
  };
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir

#endif