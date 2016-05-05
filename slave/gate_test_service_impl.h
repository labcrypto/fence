#ifndef _ORG_LABCRYPTO__FENCE__GATE_TEST_SERVICE_IMPL_H_
#define _ORG_LABCRYPTO__FENCE__GATE_TEST_SERVICE_IMPL_H_

#ifdef _MSC_VER
typedef __int8 int8_t;
typedef unsigned __int8 uint8_t;
typedef __int16 int16_t;
typedef unsigned __int16 uint16_t;
typedef __int32 int32_t;
typedef unsigned __int32 uint32_t;
typedef __int64 int64_t;
typedef unsigned __int64 uint64_t;
#else
#include <stdint.h>
#endif

#include <string>

#include <gate/service/abstract_gate_test_service.h>


namespace org {
namespace labcrypto {
namespace fence {
namespace slave {
  class GateTestServiceImpl : public ::ir::ntnaeem::gate::service::AbstractGateTestService {
  public:
    GateTestServiceImpl() {}
    virtual ~GateTestServiceImpl() {}
  public:
    virtual void OnInit();
    virtual void OnShutdown();
    virtual void EnqueueAsIncomingMessage(
      ::ir::ntnaeem::gate::Message &message, 
      ::naeem::hottentot::runtime::types::UInt64 &out, 
      ::naeem::hottentot::runtime::service::HotContext &hotContext
    );
  };
} // END OF NAMESPACE slave
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir

#endif