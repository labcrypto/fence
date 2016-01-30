/******************************************************************
 * Generated by Hottentot CC Generator
 * Date: 30-01-2016 02:30:45
 * Name: transport_service_impl.cc
 * Description:
 *   This file contains empty implementation of sample stub.
 ******************************************************************/
 
#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>

#include "transport_service_impl.h"

#include "../accept_report.h"
#include "../transport_message_status.h"
#include "../transport_message.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace transport {
  void
  TransportServiceImpl::OnInit() {
    // TODO: Called when service is initializing.
  }
  void
  TransportServiceImpl::OnShutdown() {
    // TODO: Called when service is shutting down.
  }
  void
  TransportServiceImpl::AcceptSlaveMassages(::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::TransportMessage> &messages, ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::AcceptReport> &out) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "TransportServiceImpl::AcceptSlaveMassages() is called." << std::endl;
    }
    // TODO
  }
  void
  TransportServiceImpl::RetrieveSlaveMessages(::naeem::hottentot::runtime::types::UInt32 &slaveId, ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::TransportMessage> &out) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "TransportServiceImpl::RetrieveSlaveMessages() is called." << std::endl;
    }
    // TODO
  }
  void
  TransportServiceImpl::Ack(::naeem::hottentot::runtime::types::List< ::naeem::hottentot::runtime::types::UInt64> &masterMIds) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "TransportServiceImpl::Ack() is called." << std::endl;
    }
    // TODO
  }
  void
  TransportServiceImpl::GetStatus(::naeem::hottentot::runtime::types::UInt64 &masterMId, ::ir::ntnaeem::gate::transport::TransportMessageStatus &out) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "TransportServiceImpl::GetStatus() is called." << std::endl;
    }
    // TODO
  }
} // END OF NAMESPACE transport
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir