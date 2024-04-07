#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>

#include "logger.h"
#include "rdma_proxy.h"

namespace RDMA_ECHO {

std::unique_ptr<RDMAProxy> GenerateProxy(rdma_cm_id *conn, std::shared_ptr<FileLogger> logger) {
    if((conn->pd = ibv_alloc_pd(conn->verbs)) == nullptr) {
        Log(logger.get(), "ibv_alloc_pd Fail(%s)", strerror(errno));
        return nullptr;
    }
    if((conn->send_cq = ibv_create_cq(conn->verbs, 30, nullptr, nullptr, 0)) == nullptr) {
        Log(logger.get(), "create send_cq Fail(%s)", strerror(errno));
        return nullptr;
    }
    if((conn->recv_cq = ibv_create_cq(conn->verbs, 30, nullptr, nullptr, 0)) == nullptr) {
        Log(logger.get(), "create recv_cq Fail(%s)", strerror(errno));
        return nullptr;
    }
    std::unique_ptr<RDMAProxyContext> proxy_context =
        std::unique_ptr<RDMAProxyContext>(new RDMAProxyContext(conn, logger));

    if (RegisterMemoryRegion(proxy_context.get(), logger)) {
        Log(logger.get(), "RegisterMemoryRegion Fail(%s)", strerror(errno));
        return nullptr;
    }

    ibv_qp_init_attr qp_init_attr;
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.send_cq = conn->send_cq;
    qp_init_attr.recv_cq = conn->recv_cq;
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.cap.max_recv_wr = 30;
    qp_init_attr.cap.max_send_wr = 30;
    qp_init_attr.cap.max_recv_sge = 1;
    qp_init_attr.cap.max_send_sge = 1;
    
    if (rdma_create_qp(conn, conn->pd, &qp_init_attr)) {
        Log(logger.get(), "rdma_create_qp Fail(%s)", strerror(errno));
        return nullptr;
    }
    std::unique_ptr<RDMAProxy> proxy(new RDMAProxy(std::move(proxy_context)));
    return proxy;
}

int RegisterMemoryRegion(RDMAProxyContext* proxy_context, std::shared_ptr<FileLogger> logger) {
    auto conn = proxy_context->rdma_id;
    char* send_buffer = new char[RDMABUFFERSIZE];
    proxy_context->send_mr_manager = std::unique_ptr<MRManager>(new MRManager(logger));
    if (proxy_context->send_mr_manager->RegisterMR(conn->pd, send_buffer, RDMABUFFERSIZE)) {
        Log(logger.get(), "reg send_mr Fail(%s)", strerror(errno));
        return -1;
    }
    char* recv_buffer = new char[RDMABUFFERSIZE];
    proxy_context->recv_mr_manager = std::unique_ptr<MRManager>(new MRManager(logger));
    if (proxy_context->recv_mr_manager->RegisterMR(conn->pd, recv_buffer, RDMABUFFERSIZE)) {
        Log(logger.get(), "reg recv_mr Fail(%s)", strerror(errno));
        return -1;
    }
    return 0;
}
RDMAProxy::RDMAProxy(std::unique_ptr<RDMAProxyContext> context)
         : context_(std::move(context)) {
    poll_cq_thread = std::thread(&RDMAProxy::PollCQ, this);
    for (int i = 0; i < context_->max_recv_cqe; i++) {
        PostRecv();
    }
}
RDMAProxy::~RDMAProxy() {
    closing = true;
    Disconnect();
    wait_disconnected_thread.join();
    poll_cq_thread.join();
    Log(context_->logger.get(), "~RDMAProxy() Done"); 
}

int RDMAProxy::SendMessage(const std::string& msg) {
    uint64_t wr_id = request_id_.fetch_add(1);
    Log(context_->logger.get(), "SEND Msg(%d)  : %s", wr_id, msg.c_str());  
    auto wr_wrapper = context_->send_mr_manager->AllocateSendWR(wr_id, msg);
    if (wr_wrapper == nullptr) {
        Log(context_->logger.get(), "SendMessage(%s): AllocateWR Fail", msg.c_str());
        return -1;
    }
    ibv_send_wr* bad_wr = nullptr;
    if(ibv_post_send(context_->rdma_id->qp, wr_wrapper->wr, &bad_wr)) {
        Log(context_->logger.get(), "ibv_post_send msg Fail(%s) : %s", strerror(errno), msg.c_str());
        return -1;
    }
    in_flight_tasks_.fetch_add(1);
    return 0;
}

int RDMAProxy::RecvMessage(std::string& msg) {
    std::unique_lock<std::mutex> lock(mtx_);
    while (recv_msg_queue_.empty() && IsActive()) {
        cv_.wait_for(lock, std::chrono::milliseconds(1000));
    }
    if (recv_msg_queue_.empty()) {
        Log(context_->logger.get(), "RecvMessage: Proxy Closing");
        return -1;
    }
    msg = recv_msg_queue_.front();
    recv_msg_queue_.pop();
    return 0;
}


void RDMAProxy::HandleWorkComplete(ibv_wc* wc) {
    in_flight_tasks_.fetch_sub(1);
    if (wc->status != IBV_WC_SUCCESS) {
        if (!closing) Log(context_->logger.get(), "HandleWorkComplete WorkRequest(%d) Fail(status:%d, opcode:%d)", wc->wr_id, wc->status, wc->opcode);
        return;
    }
    if (wc->opcode & IBV_WC_RECV) {
        std::unique_lock<std::mutex> lock(mtx_);
        recv_msg_queue_.emplace(recv_buffers_[wc->wr_id].buffer, recv_buffers_[wc->wr_id].sz);
        Log(context_->logger.get(), "RECV Msg(%d), addr:%ld : %s",wc->wr_id, recv_buffers_[wc->wr_id].buffer, recv_buffers_[wc->wr_id].buffer);
        recv_buffers_.erase(wc->wr_id);
        context_->recv_mr_manager->ReleaseMR(wc->wr_id);
        if (!closing) PostRecv();
        cv_.notify_one();
    } else if (wc->opcode == IBV_WC_SEND) {
        Log(context_->logger.get(), "SEND Msg(%d) SUCCESS", wc->wr_id);
        context_->send_mr_manager->ReleaseMR(wc->wr_id);
    } else {
        Log(context_->logger.get(), "Unknown opcode WC id : %d", wc->wr_id);
        return;
    }
}

int RDMAProxy::PostRecv() {
    struct ibv_recv_wr* bad_wr = nullptr;
    uint64_t wr_id = request_id_.fetch_add(1);
    auto wr_wrapper = context_->recv_mr_manager->AllocateRecvWR(wr_id, 50);
    if (wr_wrapper == nullptr) {
        Log(context_->logger.get(), "PostRecv(%d): AllocateWR Fail", 50);
        return -1;
    }
    recv_buffers_[wr_id] = RecvBuffer(wr_wrapper->addr, wr_wrapper->sz);
    if(ibv_post_recv(context_->rdma_id->qp, wr_wrapper->wr, &bad_wr)) {
        Log(context_->logger.get(), "ibv_post_recv Fail (%s)", strerror(errno));
        return -1;
    }
    in_flight_tasks_.fetch_add(1);
    Log(context_->logger.get(), "ibv_post_recv (%d)", wr_id);
    return 0;
}
void RDMAProxy::PollCQ() {
    struct ibv_wc wc;
    while (in_flight_tasks_.load() > 0 || !closing) {
        while(ibv_poll_cq(context_->send_complete_queue, 1, &wc)) {
            HandleWorkComplete(&wc);
        }
        while(ibv_poll_cq(context_->recv_complete_queue, 1, &wc)) {
            HandleWorkComplete(&wc);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(3));
    }
    Log(context_->logger.get(), "PollCQ() Exit");
}

int RDMAProxy::Detach(bool keep_ec) {
    if (keep_ec) {
        context_->ec = context_->rdma_id->channel;
    } else {
        if((context_->ec = rdma_create_event_channel()) == nullptr) {
            Log(context_->logger.get(), "RDMAProxy Detach: create_event_channel Fail", strerror(errno));
            return -1;
        }
        if (rdma_migrate_id(context_->rdma_id, context_->ec)) {
            Log(context_->logger.get(), "RDMAProxy Detach: rdma_migrate_id Fail", strerror(errno));
            return -1;
        }
    }
    
    wait_disconnected_thread = std::thread(&RDMAProxy::WaitDisconnected, this);
    Log(context_->logger.get(), "RDMAProxy Detach");
    return 0;
}

int RDMAProxy::Disconnect() {
    Log(context_->logger.get(), "Disconnect");
    closing = true;
    return rdma_disconnect(context_->rdma_id);
}

void RDMAProxy::WaitDisconnected() {
    struct rdma_cm_event *event = nullptr;
    if (rdma_get_cm_event(context_->ec, &event)) {
        Log(context_->logger.get(), "WaitDisconnect: rdma_accept get event Fail(%s)", strerror(errno));
        closing = true;
        return;
    }
    if (event->event != RDMA_CM_EVENT_DISCONNECTED) {
        Log(context_->logger.get(), "WaitDisconnect Don't get Disconnect Event %d", event->event);
        closing = true;
        return;
    }
    rdma_ack_cm_event(event);
    closing = true;
    Log(context_->logger.get(), "RDMAProxy Disconnected");
}
}
