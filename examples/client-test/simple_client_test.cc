#include <string.h>

#include <string>
#include <thread>
#include <chrono>
#include <iostream>

#include <naeem/gate/client/simple_gate_client.h>


int
main(int argc, char **argv) {
  ::naeem::gate::client::GateClient *gateClient = NULL;
  try {
    gateClient = new ::naeem::gate::client::SimpleGateClient(argv[1], 8765);
    gateClient->Init();
    for (uint32_t i = 0; i < 1; i++) {
      char message[100] = "Hello World!";
      uint64_t id = gateClient->SubmitMessage("hello-world-request", (unsigned char *)message, strlen(message) + 1);
      std::cout << "Message is submitted. Assigned id: " << id << std::endl;
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    gateClient->Shutdown();
  } catch (std::exception &e) {
    if (gateClient) {
      gateClient->Shutdown();
    }
    std::cout << "Error: " << e.what() << std::endl;
    return 1;
  } catch (...) {
    std::cout << "Error." << std::endl;
    return 1;
  }
  return 0;
}