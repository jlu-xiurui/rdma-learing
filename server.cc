#include "rdma_server.h"
#include "rdma_proxy.h"

int main() {
    RDMA_ECHO::RDMAServer server("server.log");
    server.BindAndListen(22222);
    while (1) {
        std::string msg;
        auto proxy = server.Accept();
        while (proxy->IsActive()) {
            if (!proxy->RecvMessage(msg))
                printf("RecvMessage %s\n", msg.c_str());
        }
        printf("Proxy Die\n");
    }
}