#ifndef _ORG_LABCRYPTO__FENCE__GATE_MONITOR_SERVICE_IMPL_H_
#define _ORG_LABCRYPTO__FENCE__GATE_MONITOR_SERVICE_IMPL_H_

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

#include <gate/service/abstract_gate_monitor_service.h>


namespace org {
namespace labcrypto {
namespace fence {
namespace slave {
  class GateMonitorServiceImpl : public ::ir::ntnaeem::gate::service::AbstractGateMonitorService {
  public:
    GateMonitorServiceImpl() {}
    virtual ~GateMonitorServiceImpl() {}
  public:
    virtual void OnInit();
    virtual void OnShutdown();
    virtual void GetCurrentStat(
      ::naeem::hottentot::runtime::types::Utf8String &out, 
      ::naeem::hottentot::runtime::service::HotContext &hotContext
    );
  };
} // END OF NAMESPACE slave
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir

#endif