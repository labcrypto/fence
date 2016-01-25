#ifndef _IR_NTNAEEM_GATE_TRANSPORT__TRANSPORT_SERVICE_IMPL_H_
#define _IR_NTNAEEM_GATE_TRANSPORT__TRANSPORT_SERVICE_IMPL_H_

#include <stdint.h>
#include <string>

#include "../common/transport/service/abstract_transport_service.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace master {
  class TransportServiceImpl : public ::ir::ntnaeem::gate::transport::service::AbstractTransportService {
  public:
    TransportServiceImpl() {}
    virtual ~TransportServiceImpl() {}
  public:
    virtual void OnInit();
    virtual void OnShutdown();
    virtual void AcceptSlaveMassages(::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::TransportMessage> &messages, ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::AcceptReport> &out);
    virtual void RetrieveSlaveMessages(::naeem::hottentot::runtime::types::UInt32 &slaveId, ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::TransportMessage> &out);
    virtual void Ack(::naeem::hottentot::runtime::types::List< ::naeem::hottentot::runtime::types::UInt64> &masterMIds);
    virtual void GetStatus(::naeem::hottentot::runtime::types::UInt64 &masterMId, ::ir::ntnaeem::gate::transport::TransportMessageStatus &out);
  };
} // END OF NAMESPACE master
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir

#endif