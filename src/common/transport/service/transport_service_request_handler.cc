/****************************************************************************
 * Generated by Hottentot CC Generator
 * Date: 26-01-2016 12:58:21
 * Name: transport_service_request_handler.cc
 * Description:
 *   This file contains implementation of service's request handler class.
 ****************************************************************************/
 
#include <string.h>

#include <naeem/hottentot/runtime/request.h>
#include <naeem/hottentot/runtime/response.h>

#include "transport_service_request_handler.h"
#include "abstract_transport_service.h"
#include "../transport_service.h"
#include "../accept_report.h"
#include "../transport_message_status.h"
#include "../transport_message.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace transport {
namespace service {
  void 
  TransportServiceRequestHandler::HandleRequest(::naeem::hottentot::runtime::Request &request, ::naeem::hottentot::runtime::Response &response) {
    if (request.GetMethodId() == 3638449270) {
      ::ir::ntnaeem::gate::transport::service::AbstractTransportService *serviceObject = 
        dynamic_cast< ::ir::ntnaeem::gate::transport::service::AbstractTransportService*>(service_);
      /*
       * Deserialization and making input variables
       */
      ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::TransportMessage> messages;
      messages.Deserialize(request.GetArgumentData(0), request.GetArgumentLength(0));
      /*
       * Calling service method
       */
      ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::AcceptReport> result;
      serviceObject->AcceptSlaveMassages(messages, result);
      /*
       * Serializiation of returned object
       */
      uint32_t serializedDataLength = 0;
      unsigned char *serializedData = result.Serialize(&serializedDataLength);

      response.SetStatusCode(0);
      response.SetData(serializedData);
      response.SetDataLength(serializedDataLength);
      return;
    }
    if (request.GetMethodId() == 3165690925) {
      ::ir::ntnaeem::gate::transport::service::AbstractTransportService *serviceObject = 
        dynamic_cast< ::ir::ntnaeem::gate::transport::service::AbstractTransportService*>(service_);
      /*
       * Deserialization and making input variables
       */
      ::naeem::hottentot::runtime::types::UInt32 slaveId;
      slaveId.Deserialize(request.GetArgumentData(0), request.GetArgumentLength(0));
      /*
       * Calling service method
       */
      ::naeem::hottentot::runtime::types::List< ::ir::ntnaeem::gate::transport::TransportMessage> result;
      serviceObject->RetrieveSlaveMessages(slaveId, result);
      /*
       * Serializiation of returned object
       */
      uint32_t serializedDataLength = 0;
      unsigned char *serializedData = result.Serialize(&serializedDataLength);

      response.SetStatusCode(0);
      response.SetData(serializedData);
      response.SetDataLength(serializedDataLength);
      return;
    }
    if (request.GetMethodId() == 3866780306) {
      ::ir::ntnaeem::gate::transport::service::AbstractTransportService *serviceObject = 
        dynamic_cast< ::ir::ntnaeem::gate::transport::service::AbstractTransportService*>(service_);
      /*
       * Deserialization and making input variables
       */
      ::naeem::hottentot::runtime::types::List< ::naeem::hottentot::runtime::types::UInt64> masterMIds;
      masterMIds.Deserialize(request.GetArgumentData(0), request.GetArgumentLength(0));
      /*
       * Calling service method
       */
            serviceObject->Ack(masterMIds);
      /*
       * Serializiation of returned object
       */
      uint32_t serializedDataLength = 0;
      unsigned char *serializedData = 0;
      response.SetStatusCode(0);
      response.SetData(serializedData);
      response.SetDataLength(serializedDataLength);
      return;
    }
    if (request.GetMethodId() == 3403040507) {
      ::ir::ntnaeem::gate::transport::service::AbstractTransportService *serviceObject = 
        dynamic_cast< ::ir::ntnaeem::gate::transport::service::AbstractTransportService*>(service_);
      /*
       * Deserialization and making input variables
       */
      ::naeem::hottentot::runtime::types::UInt64 masterMId;
      masterMId.Deserialize(request.GetArgumentData(0), request.GetArgumentLength(0));
      /*
       * Calling service method
       */
      ::ir::ntnaeem::gate::transport::TransportMessageStatus result;
      serviceObject->GetStatus(masterMId, result);
      /*
       * Serializiation of returned object
       */
      uint32_t serializedDataLength = 0;
      unsigned char *serializedData = result.Serialize(&serializedDataLength);

      response.SetStatusCode(0);
      response.SetData(serializedData);
      response.SetDataLength(serializedDataLength);
      return;
    }
    char errorMessage[] = "Method not found.";
    response.SetStatusCode(1);
    response.SetData((unsigned char*)errorMessage);
    response.SetDataLength(strlen(errorMessage));
  }
} // END OF NAMESPACE service
} // END OF NAMESPACE transport
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir