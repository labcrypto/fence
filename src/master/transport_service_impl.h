#ifndef _ORG_LABCRYPTO__FENCE__MASTER__TRANSPORT_SERVICE_IMPL_H_
#define _ORG_LABCRYPTO__FENCE__MASTER__TRANSPORT_SERVICE_IMPL_H_

#include <stdint.h>
#include <string>

#include <transport/service/abstract_transport_service.h>


namespace org {
namespace labcrypto {
namespace fence {
namespace master {
  class TransportServiceImpl : public ::org::labcrypto::fence::transport::service::AbstractTransportService {
  public:
    TransportServiceImpl() {}
    virtual ~TransportServiceImpl() {}
  public:
    virtual void OnInit();
    virtual void OnShutdown();
    virtual void Transmit(
      ::org::labcrypto::hottentot::List< ::org::labcrypto::fence::transport::TransportMessage> &messages, 
      ::org::labcrypto::hottentot::List< ::org::labcrypto::fence::transport::EnqueueReport> &out, 
      ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
    );
    virtual void Retrieve(
      ::org::labcrypto::hottentot::UInt32 &slaveId, 
      ::org::labcrypto::hottentot::List< ::org::labcrypto::fence::transport::TransportMessage> &out, 
      ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
    );
    virtual void Ack(
      ::org::labcrypto::hottentot::List< ::org::labcrypto::hottentot::UInt64> &masterMIds, 
      ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
    );
    virtual void GetStatus(
      ::org::labcrypto::hottentot::UInt64 &masterMId, 
      ::org::labcrypto::hottentot::UInt16 &out, 
      ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
    );
  private:
    uint32_t ackTimeout_;
    std::string workDir_;
  };
} // END NAMESPACE master
} // END NAMESPACE fence
} // END NAMESPACE labcrypto
} // END NAMESPACE org

#endif