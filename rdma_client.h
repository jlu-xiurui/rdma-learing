#ifndef RDMA_CLIENT_H
#define RDMA_CLIENT_H

#include <memory>
#include <netdb.h>

#include "logger.h"
#include "rdma_proxy.h"

namespace RDMA_ECHO {


class RDMAClient {
  
  public:
    RDMAClient(const std::string& logger_file) {
        std::FILE* f = std::fopen(logger_file.c_str(), "w");
        TEST(f != nullptr);
        logger_ = std::make_shared<FileLogger>(f, true);
    }
    ~RDMAClient() {
    }
    // 根据目的id:port，创建RDMA连接，并在其初始化后返回
    std::unique_ptr<RDMAProxy> Connect(const std::string& id, const std::string& port) {
        rdma_cm_id *conn;
        rdma_event_channel *ec = nullptr;
        if((ec = rdma_create_event_channel()) == nullptr) {
            Log(logger_.get(), "RDMAClient Connecting: create_event_channel %s:%s Fail(%s)"
                , id.c_str(), port.c_str(), strerror(errno));
            return nullptr;
        }
        if(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP)) {
            Log(logger_.get(), "RDMAClient Connecting: create_id %s:%s Fail(%s)"
                , id.c_str(), port.c_str(), strerror(errno));
            return nullptr;
        }
        // 解析地址
        if (WaitResolveAddr(conn, id, port)) {
            Log(logger_.get(), "RDMAClient Connecting: WaitResolveAddr %s:%s Fail(%s)"
                , id.c_str(), port.c_str(), strerror(errno));
            return nullptr;
        }
        // 解析路由
        if (WaitResolveRoute(conn)) {
            Log(logger_.get(), "RDMAClient Connecting: WaitResolveRoute %s:%s Fail(%s)"
                , id.c_str(), port.c_str(), strerror(errno));
            return nullptr;
        }
        auto proxy = GenerateProxy(conn, logger_);
        if (!proxy) {
            Log(logger_.get(), "GenerateProxy %s:%s Fail(%s)"
                , id.c_str(), port.c_str(), strerror(errno));
            return nullptr;
        }
        // 建立连接
        if (WaitConnected(conn)) {
            Log(logger_.get(), "RDMAClient Connecting: WaitConnected %s:%s Fail(%s)"
                , id.c_str(), port.c_str(), strerror(errno));
            return nullptr;
        }
        if (proxy->Detach(true)) {
            Log(logger_.get(), "RDMAClient Connecting: Detach Fail(%s)"
                , id.c_str(), port.c_str(), strerror(errno));
            return nullptr;
        }
        return proxy;
    }
  private:
    int WaitResolveAddr(rdma_cm_id *conn, const std::string& id, const std::string& port) {
        struct addrinfo *addr;
        struct rdma_cm_event *event = nullptr;
        if (getaddrinfo(id.c_str(), port.c_str(), nullptr, &addr)) {
            Log(logger_.get(), "RDMAClient Connecting: getaddrinfo %s:%s Fail(%s)\n"
                , id.c_str(), port.c_str(), strerror(errno));
            return -1;
        }
        if (rdma_resolve_addr(conn, nullptr, addr->ai_addr, 500)) {
            Log(logger_.get(), "RDMAClient Connecting: rdma_resolve_addr %s:%s Fail(%s)"
                , id.c_str(), port.c_str(), strerror(errno));
            return -1;
        }
        freeaddrinfo(addr);
        if (rdma_get_cm_event(conn->channel, &event)) {
            Log(logger_.get(), "RDMAClient Connecting: rdma_resolve_addr get event Fail(%s)"
                , id.c_str(), port.c_str(), strerror(errno));
            return -1;
        }
        TEST(event->event == RDMA_CM_EVENT_ADDR_RESOLVED);
        rdma_ack_cm_event(event);
        Log(logger_.get(), "RDMAClient Connecting: ResolveAddr Success");
        return 0;
    }
    int WaitResolveRoute(rdma_cm_id *conn) {
        struct rdma_cm_event *event = nullptr;
        if (rdma_resolve_route(conn, 500)) {
            Log(logger_.get(), "RDMAClient Connecting: rdma_resolve_route Fail(%s)", strerror(errno));
            return -1;
        }
        if (rdma_get_cm_event(conn->channel, &event)) {
            Log(logger_.get(), "RDMAClient Connecting: rdma_resolve_route get event Fail(%s)", strerror(errno));
            return -1;
        }
        TEST(event->event == RDMA_CM_EVENT_ROUTE_RESOLVED);
        rdma_ack_cm_event(event);
        Log(logger_.get(), "RDMAClient Connecting: ResolveRoute Success");
        return 0;
    }
    int WaitConnected(rdma_cm_id *conn) {
        struct rdma_cm_event *event = nullptr;
        rdma_conn_param conn_parm;
        memset(&conn_parm, 0, sizeof(conn_parm));
        if (rdma_connect(conn, &conn_parm)) {
            Log(logger_.get(), "RDMAClient Connecting: rdma_connect Fail(%s)", strerror(errno));
            return -1;
        }
        if (rdma_get_cm_event(conn->channel, &event)) {
            Log(logger_.get(), "RDMAClient Connecting: rdma_connect get event Fail(%s)", strerror(errno));
            return -1;
        }
        TEST(event->event == RDMA_CM_EVENT_ESTABLISHED);
        rdma_ack_cm_event(event);
        Log(logger_.get(), "RDMAClient Connect Success");
        return 0;
    }
    std::shared_ptr<FileLogger> logger_;
};


}
#endif