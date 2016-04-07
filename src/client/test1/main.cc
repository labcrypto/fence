#include <signal.h>
#include <unistd.h>

#include <thread>
#include <chrono>
#include <iostream>

#include <naeem++/os/proc.h>
#include <naeem++/conf/config_manager.h>

#include <naeem/gate/client/runtime.h>
#include <naeem/gate/client/default_message_submitter.h>


bool cont = true;
::naeem::gate::client::MessageSubmitter *messageSubmitter = NULL;

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
  ::naeem::conf::ConfigManager::LoadFromFile(execDir + "/test1.conf");
  std::string gateHost = ::naeem::conf::ConfigManager::GetValueAsString("gate-client", "host");
  uint16_t gatePort = ::naeem::conf::ConfigManager::GetValueAsUInt32("gate-client", "port");
  std::string workDirPath = ::naeem::conf::ConfigManager::GetValueAsString("gate-client", "work_dir");
  messageSubmitter = 
    new ::naeem::gate::client::DefaultMessageSubmitter (
      gateHost, 
      gatePort,
      "test1-request",
      workDirPath
    );
  messageSubmitter->Init();
  uint16_t i = 0;
  while (cont) {
    messageSubmitter->SubmitMessage((unsigned char *)"123456", 7);
    i++;
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }
  if (messageSubmitter) {
    messageSubmitter->Shutdown();
    delete messageSubmitter;
  }
  ::naeem::conf::ConfigManager::Clear();
  return 0;
}