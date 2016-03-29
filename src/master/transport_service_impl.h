#ifndef _IR_NTNAEEM_GATE_TRANSPORT__TRANSPORT_SERVICE_IMPL_H_
#define _IR_NTNAEEM_GATE_TRANSPORT__TRANSPORT_SERVICE_IMPL_H_

#include <stdint.h>
#include <string>

#include <transport/service/abstract_transport_service.h>


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
    virtual void Transmit(
      ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::TransportMessage> &messages, 
      ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::EnqueueReport> &out, 
      ::naeem::hottentot::runtime::service::HotContext &hotContext
    );
    virtual void Retrieve(
      ::naeem::hottentot::runtime::types::UInt32 &slaveId, 
      ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::TransportMessage> &out, 
      ::naeem::hottentot::runtime::service::HotContext &hotContext
    );
    virtual void Ack(
      ::naeem::hottentot::runtime::types::List< ::naeem::hottentot::runtime::types::UInt64> &masterMIds, 
      ::naeem::hottentot::runtime::service::HotContext &hotContext
    );
    virtual void GetStatus(
      ::naeem::hottentot::runtime::types::UInt64 &masterMId, 
      ::naeem::hottentot::runtime::types::Enum< ::ir::ntnaeem::gate::transport::TransportMessageStatus> &out, 
      ::naeem::hottentot::runtime::service::HotContext &hotContext
    );
  };
} // END OF NAMESPACE master
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir

#endif