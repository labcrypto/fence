#include <thread>
#include <chrono>
#include <iostream>

#include <naeem++/os/proc.h>
#include <naeem++/conf/config_manager.h>

#include <naeem/gate/client/runtime.h>
#include <naeem/gate/client/simple_gate_client.h>


int main(int argc, char **argv) {
  std::string execDir = ::naeem::os::GetExecDir();
  ::naeem::conf::ConfigManager::LoadFromFile(execDir + "/test1.conf");
  ::naeem::gate::client::GateClient *gateClient = 
    new ::naeem::gate::client::SimpleGateClient();
  gateClient->Init();
  uint16_t i = 0;
  while (true) {
    gateClient->SubmitMessage("hop", (unsigned char *)"123456", 7);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::cout << "Sent." << std::endl;
    i++;
  }
  gateClient->Shutdown();
  delete gateClient;
  return 0;
}