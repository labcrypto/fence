#include <sstream>

#include <naeem/os.h>

#include <naeem++/conf/config_manager.h>

#include <naeem/hottentot/runtime/configuration.h>
#include <naeem/hottentot/runtime/logger.h>
#include <naeem/hottentot/runtime/utils.h>
#include <naeem/hottentot/runtime/proxy/proxy.h>
#include <naeem/hottentot/runtime/proxy/proxy_runtime.h>

#include <gate/message.h>

#include <naeem/gate/client/simple_gate_client.h>
#include <naeem/gate/client/runtime.h>


namespace naeem {
namespace gate {
namespace client {
  void 
  SimpleGateClient::Init(int argc, char **argv) {
    ::naeem::hottentot::runtime::Logger::Init();
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy runtime is initialized." << std::endl;
    }
    ::naeem::gate::client::Runtime::Init(argc, argv);
  }
  void 
  SimpleGateClient::Shutdown() {
    ::naeem::gate::client::Runtime::Shutdown();
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::naeem::hottentot::runtime::Logger::Shutdown();
  }
  void 
  SimpleGateClient::SubmitMessage(std::string label, unsigned char *data, uint32_t length) {
    if (!::naeem::conf::ConfigManager::HasValue("gate-client", "work_dir")) {
      std::cout << "ERROR: Value 'gate-client.work_dir' is not found in configurations." << std::endl;
      exit(1);
    }
    std::string workDir = ::naeem::conf::ConfigManager::GetValueAsString("gate-client", "work_dir");
    uint64_t messageId;
    {
      std::lock_guard<std::mutex> guard(Runtime::messageIdCounterLock_);
      messageId = Runtime::messageIdCounter_;
      Runtime::messageIdCounter_++;
      NAEEM_os__write_to_file (
        (NAEEM_path)workDir.c_str(), 
        (NAEEM_string)"mco", 
        (NAEEM_data)&(Runtime::messageIdCounter_), 
        (NAEEM_length)sizeof(Runtime::messageIdCounter_)
      );
    }
    std::stringstream ss;
    ss << messageId;
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      ::ir::ntnaeem::gate::Message message;
      try {
        message.SetId(0);
        message.SetLabel(label);
        message.SetRelId(0);
        message.SetContent(::naeem::hottentot::runtime::types::ByteArray(data, length));
        NAEEM_length dataLength;
        NAEEM_data data = message.Serialize(&dataLength);
        NAEEM_os__write_to_file (
          (NAEEM_path)workDir.c_str(), 
          (NAEEM_string)ss.str().c_str(), 
          data, 
          dataLength
        );
      } catch (std::exception &e) {
        ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: " << e.what() << std::endl;
      } catch (...) {
        ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: Unknown error." << std::endl;
      }
    }
  }
}
}
}