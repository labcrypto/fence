/******************************************************************
 * Generated by Hottentot CC Generator
 * Date: 22-01-2016 12:53:50
 * Name: gate_service_impl.cc
 * Description:
 *   This file contains empty implementation of sample stub.
 ******************************************************************/
 
#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>

#include "gate_slave_impl.h"

#include "../common/gate/request.h"
#include "../common/gate/response.h"


namespace ir {
namespace ntnaeem {
namespace gate {
  void
  GateSlaveImpl::OnInit() {
    // TODO: Called when service is initializing.
  }
  void
  GateSlaveImpl::OnShutdown() {
    // TODO: Called when service is shutting down.
  }
  void
  GateSlaveImpl::SendRequest(::ir::ntnaeem::gate::Request &request, ::naeem::hottentot::runtime::types::UInt32 &out) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateSlaveImpl::SendRequest() is called." << std::endl;
    }
    // TODO
  }
  void
  GateSlaveImpl::GetRequestStatus(::naeem::hottentot::runtime::types::UInt32 &id, ::naeem::hottentot::runtime::types::UInt32 &out) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateSlaveImpl::GetRequestStatus() is called." << std::endl;
    }
    // TODO
  }
  void
  GateSlaveImpl::GetResponses(::naeem::hottentot::runtime::types::Utf8String &label, ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::Response> &out) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "GateSlaveImpl::GetResponses() is called." << std::endl;
    }
    // TODO
  }
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir