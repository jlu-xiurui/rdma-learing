#include "rdma_client.h"
#include "rdma_proxy.h"
#include <unistd.h>
#include <vector>
#include <iostream>

void SendThread(int thread_id, RDMA_ECHO::RDMAProxy* proxy) {
    for (int i = 0; i < 10; i++) {
        std::string msg = "thread ";
        msg = msg + std::to_string(thread_id);
        msg = msg + " : ";
        msg = msg + std::to_string(i);
        proxy->SendMessage(msg);
    }
    std::cout << "thread " << thread_id << " done\n";
}
int main() {
    RDMA_ECHO::RDMAClient client("client.log");
    std::unique_ptr<RDMA_ECHO::RDMAProxy> proxy = std::move(client.Connect("10.0.2.15", "22222"));
    std::thread threads[3];
    for (int i = 0; i < 3; i++) {
        threads[i] = std::thread(&SendThread, i, proxy.get());
    }
    for (int i = 0; i < 3; i++) {
        threads[i].join();
    }
    sleep(3);
    proxy->Disconnect();
    std::cout << "DONE\n";
}