#include "mr_manager.h"

namespace RDMA_ECHO {

MRManager::~MRManager() {
    Log(logger_.get(), "~MRManager()");
    if(mr_) ibv_dereg_mr(mr_);
    delete[] buffer_;
    for (MemBlock* block = free_list_head_.next; block != nullptr; block = block->next) {
        delete block;
    }
    for (MemBlock* block = used_list_head_.next; block != nullptr; block = block->next) {
        delete block;
    }
}

int MRManager::DeregisterMR() {
    int ret = 0;
    std::unique_lock<std::mutex> lock(mtx_);
    delete[] buffer_;
    for (MemBlock* block = free_list_head_.next; block != nullptr; block = block->next) {
        delete block;
    }
    for (MemBlock* block = used_list_head_.next; block != nullptr; block = block->next) {
        delete block;
    }
    if(mr_) ret = ibv_dereg_mr(mr_);
    mr_ = nullptr;
    buffer_ = nullptr;
    free_list_head_.next = nullptr;
    used_list_head_.next = nullptr;
    return ret;
}

int MRManager::RegisterMR(ibv_pd* pd, char* buffer, size_t buffer_sz) {
    std::unique_lock<std::mutex> lock(mtx_);
    if (mr_ != nullptr) {
        Log(logger_.get(), "MR has been register");
        return -1;
    }
    buffer_ = buffer;
    buffer_sz_ = buffer_sz;
    mr_ = ibv_reg_mr(pd, buffer, buffer_sz, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (mr_ == nullptr) {
        Log(logger_.get(), "MRManager reg_mr Fail(%s)", strerror(errno));
        return -1;
    }

    MemBlock* block = new MemBlock(buffer, buffer_sz);
    //Log(logger_.get(), "MemBlock new %lu", block);
    free_list_head_.next = block;
    block->prev = &free_list_head_;
    return 0;
}

std::unique_ptr<SendWRWrapper> MRManager::AllocateSendWR(uint64_t wr_id, const std::string& msg) {
    std::unique_lock<std::mutex> lock(mtx_);
    uint32_t buffer_size = msg.size() + 1;
    for (MemBlock* block = free_list_head_.next; block != nullptr; block = block->next) {
        if (block->sz >= buffer_size) {
            MemBlock* used_block = new MemBlock(block->addr, buffer_size);
            //Log(logger_.get(), "MemBlock new %lu", used_block);
            used_blocks_[wr_id] = used_block;
            InsertBlock(used_block, &used_list_head_, false);
            if (block->sz > buffer_size) {
                block->addr = block->addr + buffer_size;
                block->sz = block->sz - buffer_size;
            } else {
                RemoveBlock(block);
                delete block;
            }
            memcpy(used_block->addr, msg.c_str(), buffer_size);
            used_block->addr[buffer_size] = '\0';
            return ConstructSendMR(wr_id, used_block->addr, buffer_size);
        }
    }
    Log(logger_.get(), "No avaiable block for %lu size", buffer_size);
    return nullptr;
}

std::unique_ptr<SendWRWrapper> MRManager::ConstructSendMR(uint64_t wr_id, char *addr, uint32_t sz) {
    auto wr = new ibv_send_wr;
    struct ibv_sge* sge = new ibv_sge;

    memset(wr, 0, sizeof(*wr));
    wr->wr_id = wr_id;
    wr->opcode = IBV_WR_SEND;
    wr->sg_list = sge;
    wr->num_sge = 1;
    wr->send_flags = IBV_SEND_SIGNALED;

    sge->addr = (uintptr_t)addr;
    sge->length = sz;
    std::string msg(addr, sz);
    sge->lkey = mr_->lkey;
    return std::unique_ptr<SendWRWrapper>(new SendWRWrapper(wr, sge));
}

std::unique_ptr<RecvWRWrapper> MRManager::AllocateRecvWR(uint64_t wr_id, uint32_t buffer_size) {
    std::unique_lock<std::mutex> lock(mtx_);
    for (MemBlock* block = free_list_head_.next; block != nullptr; block = block->next) {
        if (block->sz >= buffer_size) {
            MemBlock* used_block = new MemBlock(block->addr, buffer_size);
            used_blocks_[wr_id] = used_block;
            InsertBlock(used_block, &used_list_head_, false);
            if (block->sz > buffer_size) {
                block->addr = block->addr + buffer_size;
                block->sz = block->sz - buffer_size;
            } else {
                RemoveBlock(block);
                delete block;
            }
            return ConstructRecvMR(wr_id, used_block->addr, buffer_size);
        }
    }
    Log(logger_.get(), "No avaiable block for %lu size", buffer_size);
    //PrintBlock();
    return nullptr;
}

std::unique_ptr<RecvWRWrapper> MRManager::ConstructRecvMR(uint64_t wr_id, char *addr, uint32_t sz) {
    ibv_recv_wr* wr = new ibv_recv_wr;
    ibv_sge* sge = new ibv_sge;

    memset(wr, 0, sizeof(*wr));
    wr->wr_id = wr_id;
    wr->sg_list = sge;
    wr->num_sge = 1;

    sge->addr = (uintptr_t)addr;
    sge->length = sz;
    sge->lkey = mr_->lkey;
    return std::unique_ptr<RecvWRWrapper>(new RecvWRWrapper(wr, sge, addr, sz));
}



void MRManager::ReleaseMR(uint64_t wr_id) {
    std::unique_lock<std::mutex> lock(mtx_);
    MemBlock* block = used_blocks_[wr_id];
    RemoveBlock(block);
    InsertBlock(block, &free_list_head_, true);
}

std::pair<MemBlock*,MemBlock*> MRManager::MergeBlock(MemBlock* new_block, MemBlock* prev_block, MemBlock* next_block) {
    // Merger front Blocks
    MemBlock* delete_block = nullptr;
    while (prev_block && prev_block->addr + prev_block->sz == new_block->addr) {
        new_block->addr -= prev_block->sz;
        new_block->sz += prev_block->sz;
        delete_block = prev_block;
        prev_block = prev_block->prev;
        RemoveBlock(delete_block);
        delete delete_block;
    }
    // Merger Behind Blocks
    while (next_block && new_block->addr + new_block->sz == next_block->addr) {
        new_block->sz += next_block->sz;
        delete_block = next_block;
        next_block = next_block->next;
        RemoveBlock(delete_block);
        delete delete_block;
    }
    return {prev_block, next_block};
}

void MRManager::InsertBlock(MemBlock* new_block, MemBlock* list, bool merge) {
    MemBlock* last_block = list;
    for (MemBlock* block = list->next; block != nullptr; block = block->next) {
        if (new_block->addr < block->addr) {
            MemBlock* prev_block = block->prev;
            MemBlock* next_block = block;
            if (merge) {
                auto pir = MergeBlock(new_block, block->prev, block);
                prev_block = pir.first;
                next_block = pir.second;
            }
            new_block->next = next_block;
            new_block->prev = prev_block;
            if (prev_block) prev_block->next = new_block;
            if (next_block) next_block->prev = new_block;
            return;
        }
        last_block = block;
    }
    new_block->next = nullptr;
    new_block->prev = last_block;
    last_block->next = new_block;
}

void MRManager::RemoveBlock(MemBlock* block) {
    if (block->prev) block->prev->next = block->next;
    if (block->next) block->next->prev = block->prev;
}

}