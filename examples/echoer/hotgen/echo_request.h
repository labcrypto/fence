/***************************************************************
 * Generated by Hottentot CC Generator
 * Date: 06-02-2016 08:10:48
 * Name: echo_request.h
 * Description:
 *   This file contains definition of EchoRequest class.
 ***************************************************************/

#ifndef _IR_NTNAEEM_GATE_EXAMPLES_ECHOER__ECHO_REQUEST_H_
#define _IR_NTNAEEM_GATE_EXAMPLES_ECHOER__ECHO_REQUEST_H_

#include <string>

#include <naeem/hottentot/runtime/types/primitives.h>
#include <naeem/hottentot/runtime/serializable.h>


namespace ir {
namespace ntnaeem {
namespace gate {
namespace examples {
namespace echoer {
  class EchoRequest : public ::naeem::hottentot::runtime::Serializable {
  public:
    EchoRequest() {}
    EchoRequest(const EchoRequest &);
    EchoRequest(EchoRequest *);
    virtual ~EchoRequest() {}
  public:
    inline ::naeem::hottentot::runtime::types::Utf8String GetName() const {
      return name_;
    }
    inline void SetName(::naeem::hottentot::runtime::types::Utf8String name) {
      name_ = name;
    }
  public:
    virtual unsigned char * Serialize(uint32_t * /* Pointer to length */);
    virtual void Deserialize(unsigned char * /* Data */, uint32_t /* Data length */);
  private:
    ::naeem::hottentot::runtime::types::Utf8String name_;
  };
} // END OF NAMESPACE echoer
} // END OF NAMESPACE examples
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir

#endif