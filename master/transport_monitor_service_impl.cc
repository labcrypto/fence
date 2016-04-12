#include <naeem++/date/helper.h>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>

#include <transport/enqueue_report.h>
#include <transport/transport_message.h>

#include "transport_monitor_service_impl.h"
#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
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
      ::naeem::hottentot::runtime::types::Utf8String &out, 
      ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << 
        "[" << ::naeem::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "TransportMonitorServiceImpl::GetCurrentStat() is called." << std::endl;
    }
    out = Runtime::GetCurrentStat();
  }
} // END OF NAMESPACE transport
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir