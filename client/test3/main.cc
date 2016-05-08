#include <signal.h>
#include <unistd.h>

#include <thread>
#include <chrono>
#include <iostream>

#include <org/labcrypto/abettor++/os/proc.h>
#include <org/labcrypto/abettor++/conf/config_manager.h>

#include <org/labcrypto/fence/client/runtime.h>
#include <org/labcrypto/fence/client/default_message_receiver.h>
#include <org/labcrypto/fence/client/default_message_submitter.h>


bool cont = true;
::org::labcrypto::fence::client::MessageReceiver *messageReceiver = NULL;
::org::labcrypto::fence::client::MessageSubmitter *messageSubmitter = NULL;

void 
SigTermHanlder(int flag) {
  cont = false;
}

int main(int argc, char **argv) {

  struct sigaction sigIntHandler;
  sigIntHandler.sa_handler = SigTermHanlder;
  sigemptyset(&sigIntHandler.sa_mask);
  sigIntHandler.sa_flags = 0;
  sigaction(SIGINT, &sigIntHandler, NULL);
  signal(SIGPIPE, SIG_IGN);

  std::string execDir = ::org::labcrypto::abettor::os::GetExecDir();
  ::org::labcrypto::abettor::conf::ConfigManager::LoadFromFile(execDir + "/test3.conf");
  std::string gateHost = ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("gate-client", "host");
  uint16_t gatePort = ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("gate-client", "port");
  std::string workDirPath = ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsString("gate-client", "work_dir");
  uint32_t ackTimeout = ::org::labcrypto::abettor::conf::ConfigManager::GetValueAsUInt32("gate-client", "ack_timeout");
  messageSubmitter = 
    new ::org::labcrypto::fence::client::DefaultMessageSubmitter (
      gateHost,
      gatePort,
      "test3-request",
      workDirPath
    );
  messageReceiver = 
    new ::org::labcrypto::fence::client::DefaultMessageReceiver (
      gateHost,
      gatePort,
      "test3-response",
      workDirPath,
      ackTimeout
    );
  messageSubmitter->Init();
  messageReceiver->Init();
  uint64_t reqId = messageSubmitter->SubmitMessage((unsigned char *)"123456", 7);
  std::cout << "Message is enqueued with id: " << reqId << std::endl;
  std::vector<::org::labcrypto::fence::client::Message*> messages;
  messages = messageReceiver->GetMessages();
  std::vector<uint64_t> ids;
  while (cont) {
    for (uint32_t i = 0; i < messages.size(); i++) {
      std::cout << "Id: '" << messages[i]->GetId() << "'" << std::endl;
      std::cout << "RelId: '" << messages[i]->GetRelId() << "'" << std::endl;
      std::cout << "Label: '" << messages[i]->GetLabel() << "'" << std::endl;
      std::cout << messages[i]->GetContent() << std::endl;
      std::cout << "---------------------" << std::endl;
      ids.push_back(messages[i]->GetId());
      if (messages[i]->GetRelId() == reqId) {
        std::cout << "Request is found." << std::endl;
        cont = false;
      }
      delete messages[i];
    }
    messageReceiver->Ack(ids);
    messages = messageReceiver->GetMessages();
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  if (messageReceiver) {
    messageReceiver->Shutdown();
    delete messageReceiver;
  }
  if (messageSubmitter) {
    messageSubmitter->Shutdown();
    delete messageSubmitter;
  }
  ::org::labcrypto::abettor::conf::ConfigManager::Clear();
  return 0;
}