#ifndef RDMA_SERVER_H
#define RDMA_SERVER_H

#include <memory>
#include <netdb.h>

#include "logger.h"
#include "rdma_proxy.h"

namespace RDMA_ECHO {
 

class RDMAServer {
  public:
    RDMAServer(const std::string& logger_file) {
        std::FILE* f = std::fopen(logger_file.c_str(), "w");
        TEST(f != nullptr);
        logger_ = std::make_shared<FileLogger>(f, true);
    }
    ~RDMAServer() {
        rdma_destroy_event_channel(ec_);
    }
    int BindAndListen(uint64_t port) {
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        Log(logger_.get(), "Listening on port %d", port);

        if ((ec_ = rdma_create_event_channel()) == nullptr) {
            Log(logger_.get(), "RDMAServer Listening: rdma_create_event_channel Fail", strerror(errno));
            return -1;
        }
        if(rdma_create_id(ec_, &listener_, nullptr, RDMA_PS_TCP)) {
            Log(logger_.get(), "RDMAServer Listening: create_id %d Fail(%s)", port, strerror(errno));
            return -1;
        }
        if (rdma_bind_addr(listener_, (sockaddr*)(&addr))) {
            Log(logger_.get(), "rdma_bind_addr in port:%d Fail", port, strerror(errno));
            return -1;
        }
        if (rdma_listen(listener_, 10)) {
            Log(logger_.get(), "rdma_listen in port:%d Fail", port, strerror(errno));
            return -1;
        }
        Log(logger_.get(), "RDMAServer BindAndListen Success");
        return 0;
    }
    std::unique_ptr<RDMAProxy> Accept() {
        rdma_cm_id* conn = nullptr;
        if (WaitListen(&conn)) {
            Log(logger_.get(), "RDMAServer WaitListen Fail(%s)", strerror(errno));
            return nullptr;
        }
        auto proxy = GenerateProxy(conn, logger_);
        if (!proxy) {
            Log(logger_.get(), "GenerateProxy Fail(%s)", strerror(errno));
            return nullptr;
        }
        if (WaitAccept(conn)) {
            Log(logger_.get(), "RDMAServer WaitAccept Fail(%s)", strerror(errno));
            return nullptr;
        }
        if (proxy->Detach(false)) {
            Log(logger_.get(), "RDMAServer Accept: Detach Fail(%s)", strerror(errno));
            return nullptr;
        }
        return proxy;
    }
  private:
    int WaitListen(rdma_cm_id** conn) {
        struct rdma_cm_event *event = nullptr;
        if (rdma_get_cm_event(ec_, &event)) {
            Log(logger_.get(), "RDMAServer Accepting: rdma_listen get event Fail(%s)", strerror(errno));
            return -1;
        }
        if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
            Log(logger_.get(), "Accept Unknown event type %d", event->event);
            return -1;
        }
        *conn = event->id;
        rdma_ack_cm_event(event);
        Log(logger_.get(), "RDMAServer Receive Connect Request");
        return 0;
    }
    int WaitAccept(rdma_cm_id* conn) {
        struct rdma_cm_event *event = nullptr;
        struct rdma_conn_param cm_params;
        memset(&cm_params, 0, sizeof(cm_params));

        if (rdma_accept(conn, &cm_params)) {
            Log(logger_.get(), "RDMAServer : rdma_accept Fail(%s)", strerror(errno));
            return -1;
        }
        if (rdma_get_cm_event(ec_, &event)) {
            Log(logger_.get(), "RDMAServer: rdma_accept get event Fail(%s)", strerror(errno));
            return -1;
        }
        if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
            Log(logger_.get(), "Accept Unknown event type %d", event->event);
            return -1;
        }
        rdma_ack_cm_event(event);
        Log(logger_.get(), "RDMAServer Accept Success");
        return 0;
    }
    rdma_cm_id* listener_;
    struct rdma_event_channel *ec_;
    std::shared_ptr<FileLogger> logger_;
};


}
#endif