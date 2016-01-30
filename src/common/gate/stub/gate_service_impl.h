/******************************************************************
 * Generated by Hottentot CC Generator
 * Date: 30-01-2016 02:30:44
 * Name: gate_service_impl.h
 * Description:
 *   This file contains definitions of sample stub.
 ******************************************************************/
 
#ifndef _IR_NTNAEEM_GATE__GATE_SERVICE_IMPL_H_
#define _IR_NTNAEEM_GATE__GATE_SERVICE_IMPL_H_

#include <stdint.h>
#include <string>

#include "../service/abstract_gate_service.h"


namespace ir {
namespace ntnaeem {
namespace gate {
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
    virtual void NextMessage(::naeem::hottentot::runtime::types::Utf8String &label, ::ir::ntnaeem::gate::Message &out);
  };
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir

#endif