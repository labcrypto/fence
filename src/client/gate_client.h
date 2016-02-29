#ifndef _IR_NTNAEEM_GATE__CLIENT__GATE_CLIENT_H_
#define _IR_NTNAEEM_GATE__CLIENT__GATE_CLIENT_H_

#include <stdint.h>
#include <string>


namespace ir {
namespace ntnaeem {
namespace gate {
namespace client {
  class GateClient {
  public:
    static void SubmitMessage(std::string label, std::string message);
    static void SubmitMessage(std::string label, unsigned char *data, uint32_t length);
  };
}
}
}
}