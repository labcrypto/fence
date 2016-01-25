/***************************************************************
 * Generated by Hottentot CC Generator
 * Date: 26-01-2016 12:58:21
 * Name: transport_message_status.h
 * Description:
 *   This file contains definition of TransportMessageStatus class.
 ***************************************************************/

#ifndef _IR_NTNAEEM_GATE_TRANSPORT__TRANSPORT_MESSAGE_STATUS_H_
#define _IR_NTNAEEM_GATE_TRANSPORT__TRANSPORT_MESSAGE_STATUS_H_

#include <string>

#include <naeem/hottentot/runtime/types/primitives.h>
#include <naeem/hottentot/runtime/serializable.h>


namespace ir {
namespace ntnaeem {
namespace gate {
namespace transport {
  class TransportMessageStatus : public ::naeem::hottentot::runtime::Serializable {
  public:
    TransportMessageStatus() {}
    ~TransportMessageStatus() {}
  public:
    inline ::naeem::hottentot::runtime::types::Int16 GetStatusCode() const {
      return statusCode_;
    }
    inline void SetStatusCode(::naeem::hottentot::runtime::types::Int16 statusCode) {
      statusCode_ = statusCode;
    }
  public:
    virtual unsigned char * Serialize(uint32_t * /* Pointer to length */);
    virtual void Deserialize(unsigned char * /* Data */, uint32_t /* Data length */);
  private:
    ::naeem::hottentot::runtime::types::Int16 statusCode_;
  };
} // END OF NAMESPACE transport
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir

#endif