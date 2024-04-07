#ifndef SEND_MR_MANAGER_H
#define SEND_MR_MANAGER_H
#include <rdma/rdma_cma.h>
#include <memory>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <condition_variable>

#include "logger.h"
namespace RDMA_ECHO {

struct SendWRWrapper {
    SendWRWrapper(ibv_send_wr* wr_, ibv_sge* sge_) : wr(wr_), sge(sge_) {}
    ~SendWRWrapper() {
        delete wr;
        delete sge;
    }
    ibv_send_wr* wr;
    ibv_sge* sge;
};

struct RecvWRWrapper {
    RecvWRWrapper(ibv_recv_wr* wr_, ibv_sge* sge_, char* addr_, uint32_t sz_)
         : wr(wr_), sge(sge_), addr(addr_), sz(sz_) {}
    ~RecvWRWrapper() {
        delete wr;
        delete sge;
    }
    ibv_recv_wr* wr;
    ibv_sge* sge;
    char* addr;
    uint32_t sz;
};

struct MemBlock {
    MemBlock()
            : addr(nullptr), sz(0), prev(nullptr), next(nullptr) {}
    MemBlock(char* buffer, size_t buffer_sz)
            : addr(buffer), sz(buffer_sz), prev(nullptr), next(nullptr) {}
    char* addr;
    size_t sz;
    MemBlock* prev;
    MemBlock* next;
};

// 用于管理Memory Region，创建WQE
class MRManager {
  public:
    MRManager(std::shared_ptr<FileLogger> logger) : logger_(logger) {}
    MRManager(const MRManager&) = delete;
    MRManager& operator=(const MRManager&) = delete;
    ~MRManager();

    void PrintBlock() {
        Log(logger_.get(), "=============Free list:===========");
        for (MemBlock* block = free_list_head_.next; block != nullptr; block = block->next) {
            Log(logger_.get(), "MemBlock %lu %d", block->addr, block->sz);
        }
        Log(logger_.get(), "=============Used list:===========");
        for (MemBlock* block = used_list_head_.next; block != nullptr; block = block->next) {
            Log(logger_.get(), "MemBlock %lu %d", block->addr, block->sz);
        }
        Log(logger_.get(), "==================================");
    }
    // 注册Memory Region
    int RegisterMR(ibv_pd* pd, char* buffer, size_t buffer_sz);
    // 解除Memory Region的注册
    int DeregisterMR();

    inline const MemBlock* FreeList() {return &free_list_head_; }
    inline const MemBlock* UsedList() {return &used_list_head_; }

    // 新建Send WQE
    std::unique_ptr<SendWRWrapper> AllocateSendWR(uint64_t wr_id, const std::string& msg);
    // 新建Recv WQE
    std::unique_ptr<RecvWRWrapper> AllocateRecvWR(uint64_t wr_id, uint32_t buffer_size);
    // 任何新建的WQE必须通过ReleaseMR释放缓冲区资源
    void ReleaseMR(uint64_t wr_id);

  private:
    std::pair<MemBlock*,MemBlock*> MergeBlock(MemBlock* new_block, MemBlock* prev_block, MemBlock* next_block);

    void InsertBlock(MemBlock* new_block, MemBlock* list, bool merge);

    void RemoveBlock(MemBlock* block);

    std::unique_ptr<SendWRWrapper> ConstructSendMR(uint64_t wr_id, char *addr, uint32_t sz);

    std::unique_ptr<RecvWRWrapper> ConstructRecvMR(uint64_t wr_id, char *addr, uint32_t sz);

    std::shared_ptr<FileLogger> logger_;
    char* buffer_{nullptr};
    size_t buffer_sz_{0};
    ibv_mr* mr_{nullptr};

    std::unordered_map<uint64_t, MemBlock*> used_blocks_;
    MemBlock free_list_head_;
    MemBlock used_list_head_;
    std::mutex mtx_;
};

}
#endif