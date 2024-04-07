#ifndef RDMA_RDMA_PROXY_H
#define RDMA_RDMA_PROXY_H

#include <rdma/rdma_cma.h>

#include <thread>
#include <atomic>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <queue>

#include "logger.h"
#include "mr_manager.h"

namespace RDMA_ECHO {

#define TEST(x)  do { if (!(x)) { fprintf(stderr, "error: %s failed.\n", #x); exit(1); }} while (0)

constexpr int RDMABUFFERSIZE = 4096;

class RDMAClient;
class RDMAServer;

struct RDMAProxyContext {
    RDMAProxyContext(rdma_cm_id *id, std::shared_ptr<FileLogger> logger)
        : logger(logger),
          rdma_id(id),
          send_complete_queue(id->send_cq),
          recv_complete_queue(id->recv_cq) {}
    ~RDMAProxyContext() {
        auto ec = rdma_id->channel;
        rdma_destroy_qp(rdma_id);
        if (send_mr_manager->DeregisterMR()) {
            Log(logger.get(), "~RDMAProxyContext() send_mr_manager->DeregisterMR() Fail(%s)", strerror(errno));
        }
        if (recv_mr_manager->DeregisterMR()) {
            Log(logger.get(), "~RDMAProxyContext() recv_mr_manager->DeregisterMR() Fail(%s)", strerror(errno));
        }
        if (ibv_dealloc_pd(rdma_id->pd)) {
            Log(logger.get(), "~RDMAProxyContext() ibv_dealloc_pd Fail(%s)", strerror(errno));
        }
        if (rdma_destroy_id(rdma_id)) {
            Log(logger.get(), "~RDMAProxyContext() rdma_destroy_id Fail(%s)", strerror(errno));
        }
        rdma_destroy_event_channel(ec);
    }
    std::shared_ptr<FileLogger> logger;
    rdma_event_channel *ec;
    rdma_cm_id *rdma_id;
    std::unique_ptr<MRManager> send_mr_manager;
    std::unique_ptr<MRManager> recv_mr_manager;
    ibv_cq* send_complete_queue;
    ibv_cq* recv_complete_queue;
    int max_recv_cqe{30};
    int max_send_cqe{30};
};

class RDMAProxy;

std::unique_ptr<RDMAProxy> GenerateProxy(rdma_cm_id *conn, std::shared_ptr<FileLogger> logger);

int RegisterMemoryRegion(RDMAProxyContext* proxy_context, std::shared_ptr<FileLogger> logger);

class RDMAProxy {
  public:
    RDMAProxy(std::unique_ptr<RDMAProxyContext> context);
    
    ~RDMAProxy();
    // 异步提交发送请求，提交失败返回-1
    int SendMessage(const std::string& msg);

    // 从接受队列中获取一条消息，当队列为空时则阻塞地
    // 等待来自对端的请求，当连接关闭且队列为空时返回-1
    int RecvMessage(std::string& msg); 

    // 主动地关闭连接，失败时返回-1
    int Disconnect();

    inline bool IsActive() {
        return closing.load() == false;
    }
    
  private:
    friend class RDMAClient;
    friend class RDMAServer;

    struct RecvBuffer {
        RecvBuffer() : buffer(nullptr), sz(0) {}
        RecvBuffer(char* buffer_, uint32_t sz_) : buffer(buffer_), sz(sz_) {}
        char* buffer;
        size_t sz;
    };
    // 开启WaitDisconnected()，并当keep_ec为true时将rmda_cm_id托管至新的event channel
    int Detach(bool keep_ec);

    // 处理CQE
    void HandleWorkComplete(ibv_wc* wc);

    // 处理CQ中的完成事件
    void PollCQ();

    // 等待来自对端或本地的关闭请求
    void WaitDisconnected();

    // 提交一条接受指令
    int PostRecv();

    std::atomic<bool> closing{false}; // 连接是否被关闭
    std::unique_ptr<RDMAProxyContext> context_; // RDMA verbs所需的句柄集合
    std::thread poll_cq_thread;
    std::thread wait_disconnected_thread;

    std::mutex mtx_;
    std::condition_variable cv_;

    std::atomic<uint64_t> request_id_{0}; // 当前的WQE id

    std::queue<std::string> recv_msg_queue_; //接受队列
    std::unordered_map<uint64_t, RecvBuffer> recv_buffers_;  // 保存接受请求的<wr_id : 缓冲区地址及长度>

    std::atomic<uint64_t> in_flight_tasks_{0}; // 目前被提交但未被确认的WQE数量
};

}
#endif