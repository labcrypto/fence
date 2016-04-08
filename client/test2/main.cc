#include <signal.h>
#include <unistd.h>

#include <thread>
#include <chrono>
#include <iostream>

#include <naeem++/os/proc.h>
#include <naeem++/conf/config_manager.h>

#include <naeem/gate/client/runtime.h>
#include <naeem/gate/client/default_message_receiver.h>


bool cont = true;
::naeem::gate::client::MessageReceiver *messageReceiver = NULL;

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

  std::string execDir = ::naeem::os::GetExecDir();
  ::naeem::conf::ConfigManager::LoadFromFile(execDir + "/test2.conf");
  std::string gateHost = ::naeem::conf::ConfigManager::GetValueAsString("gate-client", "host");
  uint16_t gatePort = ::naeem::conf::ConfigManager::GetValueAsUInt32("gate-client", "port");
  std::string workDirPath = ::naeem::conf::ConfigManager::GetValueAsString("gate-client", "work_dir");
  uint32_t ackTimeout = ::naeem::conf::ConfigManager::GetValueAsUInt32("gate-client", "ack_timeout");
  messageReceiver = 
    new ::naeem::gate::client::DefaultMessageReceiver (
      gateHost, 
      gatePort,
      "test2-response",
      workDirPath,
      ackTimeout
    );
  messageReceiver->Init();
  std::vector<::naeem::gate::client::Message*> messages;
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
  ::naeem::conf::ConfigManager::Clear();
  return 0;
}