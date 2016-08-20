#ifndef _ORG_LABCRYPTO__FENCE__SLAVE__FENCE_SERVICE_IMPL_H_
#define _ORG_LABCRYPTO__FENCE__SLAVE__FENCE_SERVICE_IMPL_H_

#include <stdint.h>
#include <string>

#include <fence/service/abstract_fence_service.h>


namespace org {
namespace labcrypto {
namespace fence {
namespace slave {
  class FenceServiceImpl : public ::org::labcrypto::fence::service::AbstractFenceService {
  public:
    FenceServiceImpl() {}
    virtual ~FenceServiceImpl() {}
  public:
    virtual void OnInit();
    virtual void OnShutdown();
    virtual void Enqueue(
      ::org::labcrypto::fence::Message &message, 
      ::org::labcrypto::hottentot::UInt64 &out, 
      ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
    );
    virtual void GetStatus(
      ::org::labcrypto::hottentot::UInt64 &id, 
      ::org::labcrypto::hottentot::UInt16 &out, 
      ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
    );
    virtual void Discard(
      ::org::labcrypto::hottentot::UInt64 &id, 
      ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
    );
    virtual void HasMore(
      ::org::labcrypto::hottentot::Utf8String &label, 
      ::org::labcrypto::hottentot::Boolean &out, 
      ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
    );
    virtual void PopNext(
      ::org::labcrypto::hottentot::Utf8String &label, 
      ::org::labcrypto::fence::Message &out, 
      ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
    );
    virtual void Ack(
      ::org::labcrypto::hottentot::UInt64 &id, 
      ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
    );
  private:
    uint32_t ackTimeout_;
    std::string workDir_;
  };
} // END OF NAMESPACE slave
} // END OF NAMESPACE fence
} // END OF NAMESPACE labcrypto
} // END OF NAMESPACE org

#endif