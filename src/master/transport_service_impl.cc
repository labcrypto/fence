#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>

#include "../common/transport/transport_message_status.h"
#include "../common/transport/transport_message.h"
#include "../common/transport/accept_report.h"

#include "transport_service_impl.h"
#include "runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace master {
  void
  TransportServiceImpl::OnInit() {
    ::naeem::hottentot::runtime::Logger::GetOut() << "Transport Service is initialized." << std::endl;
  }
  void
  TransportServiceImpl::OnShutdown() {
    // TODO: Called when service is shutting down.
  }
  void
  TransportServiceImpl::AcceptSlaveMassages(::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::TransportMessage> &messages, 
                                            ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::AcceptReport> &out) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "TransportServiceImpl::AcceptSlaveMassages() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      std::lock_guard<std::mutex> guard2(Runtime::transportInboxQueueLock_);
      for (uint32_t i = 0; i < messages.Size(); i++) {
        ::ir::ntnaeem::gate::transport::AcceptReport *acceptReport = 
          new ::ir::ntnaeem::gate::transport::AcceptReport;
        try {
          ::ir::ntnaeem::gate::transport::TransportMessage *transportMessage = 
            new ::ir::ntnaeem::gate::transport::TransportMessage();
          transportMessage->SetSlaveId(messages.Get(i)->GetSlaveId());
          transportMessage->SetSlaveMId(messages.Get(i)->GetSlaveMId());
          transportMessage->SetRelMId(messages.Get(i)->GetRelMId());
          transportMessage->SetRelLabel(messages.Get(i)->GetRelLabel());
          transportMessage->SetLabel(messages.Get(i)->GetLabel());
          transportMessage->SetContent(messages.Get(i)->GetContent());
          {
            std::lock_guard<std::mutex> guard(Runtime::counterLock_);
            transportMessage->SetMasterMId(Runtime::messageCounter_);
            Runtime::messageCounter_++;
          }
          acceptReport->SetMasterMId(transportMessage->GetMasterMId());
          acceptReport->SetSlaveMId(transportMessage->GetSlaveMId());
          Runtime::transportInboxQueue_->Put(transportMessage);
          acceptReport->SetStatusCode(0);
          acceptReport->SetErrorMessage("");
        } catch (...) {
          acceptReport->SetStatusCode(-1000);
          acceptReport->SetErrorMessage("Insertion error.");
        }
        out.Add(acceptReport);
      }
    }
  }
  void
  TransportServiceImpl::RetrieveSlaveMessages(::naeem::hottentot::runtime::types::UInt32 &slaveId, 
                                              ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::TransportMessage> &out) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "TransportServiceImpl::RetrieveSlaveMessages() is called." << std::endl;
    }
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      std::lock_guard<std::mutex> guard2(Runtime::transportOutboxQueueLock_);
      std::vector<::ir::ntnaeem::gate::transport::TransportMessage*> messages = 
        Runtime::transportOutboxQueue_->PopAll(slaveId.GetValue());
      for (uint32_t i = 0; i < messages.size(); i++) {
        out.Add(messages[i]);
        Runtime::transportSentQueue_->Put(messages[i]);
      }
    }
  }
  void
  TransportServiceImpl::Ack(::naeem::hottentot::runtime::types::List< ::naeem::hottentot::runtime::types::UInt64> &masterMIds) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "TransportServiceImpl::Ack() is called." << std::endl;
    }
    // TODO
  }
  void
  TransportServiceImpl::GetStatus(::naeem::hottentot::runtime::types::UInt64 &masterMId, 
                                  ::ir::ntnaeem::gate::transport::TransportMessageStatus &out) {
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "TransportServiceImpl::GetStatus() is called." << std::endl;
    }
    // TODO
  }
} // END OF NAMESPACE master
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir