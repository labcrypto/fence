#include <org/labcrypto/abettor++/date/helper.h>

#include <org/labcrypto/hottentot/runtime/configuration.h>
#include <org/labcrypto/hottentot/runtime/logger.h>
#include <org/labcrypto/hottentot/runtime/utils.h>

#include <fence/message.h>

#include "fence_test_service_impl.h"


namespace org {
namespace labcrypto {
namespace fence {
namespace slave {
  void
  FenceTestServiceImpl::OnInit() {
    // TODO: Called when service is initializing.
  }
  void
  FenceTestServiceImpl::OnShutdown() {
    // TODO: Called when service is shutting down.
  }
  void
  FenceTestServiceImpl::EnqueueAsIncomingMessage(
      ::org::labcrypto::fence::Message &message, 
      ::org::labcrypto::hottentot::UInt64 &out, 
      ::org::labcrypto::hottentot::runtime::service::HotContext &hotContext
  ) {
    if (::org::labcrypto::hottentot::runtime::Configuration::Verbose()) {
      ::org::labcrypto::hottentot::runtime::Logger::GetOut() << 
        "[" << ::org::labcrypto::abettor::date::helper::GetCurrentUTCTimeString() << "]: " << 
          "FenceTestServiceImpl::EnqueueAsIncomingMessage() is called." << std::endl;
    }
    // TODO
  }
} // END OF NAMESPACE slave
} // END OF NAMESPACE fence
} // END OF NAMESPACE labcrypto
} // END OF NAMESPACE org