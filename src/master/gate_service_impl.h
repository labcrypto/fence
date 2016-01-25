#ifndef _IR_NTNAEEM_GATE__MASTER__GATE_SERVICE_IMPL_H_
#define _IR_NTNAEEM_GATE__MASTER__GATE_SERVICE_IMPL_H_

#include <stdint.h>
#include <string>

#include "../common/gate/service/abstract_gate_service.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace master {
  template<class M> class Queue;
  class GateServiceImpl : public ::ir::ntnaeem::gate::service::AbstractGateService {
  public:
    GateServiceImpl() {}
    virtual ~GateServiceImpl() {}
  public:
    virtual void OnInit();
    virtual void OnShutdown();
    virtual void EnqueueMessage(::ir::ntnaeem::gate::Message &message, ::naeem::hottentot::runtime::types::UInt64 &out);
    virtual void GetMessageStatus(::naeem::hottentot::runtime::types::UInt64 &id, ::ir::ntnaeem::gate::MessageStatus &out);
    virtual void HasMoreMessage(::naeem::hottentot::runtime::types::Utf8String &label, ::naeem::hottentot::runtime::types::Boolean &out);
    virtual void NextMessage(::naeem::hottentot::runtime::types::Utf8String &label, ::naeem::hottentot::runtime::types::Boolean &messageRetrieved, ::ir::ntnaeem::gate::Message &out);
  private:
  };
} // END OF NAMESPACE master
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir

#endif