/******************************************************************
 * Generated by Hottentot CC Generator
 * Date: 22-01-2016 01:23:32
 * Name: gate_service_proxy_builder.cc
 * Description:
 *   This file contains implementation of the proxy builder class.
 ******************************************************************/

#include "gate_service_proxy_builder.h"
#include "gate_service_proxy.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace proxy {
  GateService*
  GateServiceProxyBuilder::Create(std::string host, uint32_t port) {
    return new GateServiceProxy(host, port);
  }
  void
  GateServiceProxyBuilder::Destroy(GateService *service) {
    delete service;
  }
} // END OF NAMESPACE proxy
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir