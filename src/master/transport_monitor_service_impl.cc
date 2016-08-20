#include <org/labcrypto/abettor++/date/helper.h>

#include <org/labcrypto/hottentot/runtime/configuration.h>
#include <org/labcrypto/hottentot/runtime/logger.h>
#include <org/labcrypto/hottentot/runtime/utils.h>

#include <transport/enqueue_report.h>
#include <transport/transport_message.h>

#include "transport_monitor_service_impl.h"
#include "runtime.h"


namespace org {
namespace labcrypto {
namespace fence {
namespace master {
  void
  TransportMonitorServiceImpl::OnInit() {
    // TODO: Called when service is initializing.
  }
  void
  TransportMonitorServiceImpl::OnShutdown() {
    // TODO: Called when service is shutting down.
  }
  void
  TransportMonitorServiceImpl::GetCurrentStat(
      ::org::labcrypto::hottentot::Utf8String &out, 
      ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "TransportMonitorServiceImpl::GetCurrentStat() is called." << std::endl;
    }
    out = Runtime::GetCurrentStat();
  }
} // END OF NAMESPACE transport
} // END OF NAMESPACE fence
} // END OF NAMESPACE labcrypto
} // END OF NAMESPACE org