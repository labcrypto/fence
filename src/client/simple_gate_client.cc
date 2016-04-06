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
#include <naeem/gate/client/submitter_thread.h>


namespace naeem {
namespace gate {
namespace client {
  void 
  SimpleGateClient::Init (
    int argc, 
    char **argv
  ) {
    ::naeem::hottentot::runtime::Logger::Init();
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Init(argc, argv);
    if (::naeem::hottentot::runtime::Configuration::Verbose()) {
      ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy runtime is initialized." << std::endl;
    }
    ::naeem::gate::client::Runtime::Init(argc, argv);
    SubmitterThread *submitterThread = new SubmitterThread(label_);
    submitterThread->Start();
  }
  void 
  SimpleGateClient::Shutdown() {
    ::naeem::gate::client::Runtime::Shutdown();
    ::naeem::hottentot::runtime::proxy::ProxyRuntime::Shutdown();
    ::naeem::hottentot::runtime::Logger::Shutdown();
  }
  uint64_t 
  SimpleGateClient::SubmitMessage (
    unsigned char *data, 
    uint32_t length
  ) {
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
        message.SetLabel(label_);
        message.SetRelId(0);
        message.SetContent(::naeem::hottentot::runtime::types::ByteArray(data, length));
        NAEEM_length dataLength;
        NAEEM_data data = message.Serialize(&dataLength);
        NAEEM_os__write_to_file (
          (NAEEM_path)(workDir + "/e").c_str(), 
          (NAEEM_string)ss.str().c_str(), 
          data, 
          dataLength
        );
        Runtime::enqueued_.push_back(messageId);
      } catch (std::exception &e) {
        ::naeem::hottentot::runtime::Logger::GetError() << 
          "ERROR: " << e.what() << std::endl;
      } catch (...) {
        ::naeem::hottentot::runtime::Logger::GetError() << 
          "ERROR: Unknown error." << std::endl;
      }
    }
    return messageId;
  }
  std::vector<Message*>
  SimpleGateClient::GetMessages () {
    if (!::naeem::conf::ConfigManager::HasValue("gate-client", "work_dir")) {
      std::cout << "ERROR: Value 'gate-client.work_dir' is not found in configurations." << std::endl;
      exit(1);
    }
    std::string workDir = ::naeem::conf::ConfigManager::GetValueAsString("gate-client", "work_dir");
    {
      std::lock_guard<std::mutex> guard(Runtime::mainLock_);
      /*::ir::ntnaeem::gate::proxy::GateService *proxy = 
        ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Create(host, port);
      if (::naeem::hottentot::runtime::Configuration::Verbose()) {
        ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is created." << std::endl;
      }
      try {
        if (dynamic_cast< ::naeem::hottentot::runtime::proxy::Proxy*>(proxy)->IsServerAlive()) {
          
          uint64_t assignedId = id.GetValue();
          // ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
          // if (::naeem::hottentot::runtime::Configuration::Verbose()) {
          //   ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
          // }
          NAEEM_os__move_file (
            (NAEEM_path)(workDir + "/w").c_str(),
            (NAEEM_string)ss.str().c_str(),
            (NAEEM_path)(workDir + "/s").c_str(),
            (NAEEM_string)ss.str().c_str()
          );
          NAEEM_os__write_to_file (
            (NAEEM_path)(workDir + "/s").c_str(),
            (NAEEM_string)(ss.str() + ".gid").c_str(),
            (NAEEM_data)(&assignedId),
            sizeof(assignedId)
          );
          std::stringstream css;
          css << assignedId;
          NAEEM_os__write_to_file (
            (NAEEM_path)(workDir + "/s").c_str(),
            (NAEEM_string)(css.str() + ".cid").c_str(),
            (NAEEM_data)(&messageId),
            sizeof(messageId)
          );
          std::cout << "Sent." << std::endl;
        } else {
          throw std::runtime_error("Gate is not available.");
        }
        ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
        if (::naeem::hottentot::runtime::Configuration::Verbose()) {
          ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
        }
      } catch (std::exception &e) {
        ::naeem::hottentot::runtime::Logger::GetError() << "ERROR: " << e.what() << std::endl;
        ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
        if (::naeem::hottentot::runtime::Configuration::Verbose()) {
          ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
        }
        Runtime::enqueued_.push_back(messageId);
      } catch (...) {
        ::naeem::hottentot::runtime::Logger::GetError() << "Unknown error." << std::endl;
        ::ir::ntnaeem::gate::proxy::GateServiceProxyBuilder::Destroy(proxy);
        if (::naeem::hottentot::runtime::Configuration::Verbose()) {
          ::naeem::hottentot::runtime::Logger::GetOut() << "Proxy object is destroyed." << std::endl;
        }
        Runtime::enqueued_.push_back(messageId);
      }*/

    }
  }
  void
  SimpleGateClient::Ack (
    std::vector<uint64_t> ids
  ) {

  }
}
}
}