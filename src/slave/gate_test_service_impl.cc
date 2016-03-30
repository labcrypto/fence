#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>

#include <gate/message.h>

#include "gate_test_service_impl.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace slave {
  void
  GateTestServiceImpl::OnInit() {
    // TODO: Called when service is initializing.
  }
  void
  GateTestServiceImpl::OnShutdown() {
    // TODO: Called when service is shutting down.
  }
  void
  GateTestServiceImpl::EnqueueAsIncomingMessage(
      ::ir::ntnaeem::gate::Message &message, 
      ::naeem::hottentot::runtime::types::UInt64 &out, 
      ::naeem::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateTestServiceImpl::EnqueueAsIncomingMessage() is called." << std::endl;
    }
    // TODO
  }
} // END OF NAMESPACE slave
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir