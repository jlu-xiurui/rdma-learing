#include "mr_manager.h"
#include <gtest/gtest.h>
// Demonstrate some basic assertions.
TEST(MRManagerTest, Allocate) {
    std::FILE* f = std::fopen("test.log", "w");
    auto logger_ = std::make_shared<RDMA_ECHO::FileLogger>(f, true);
    RDMA_ECHO::MRManager mr_manager(logger_);
    char* buffer = new char[1024];

    auto dev_list = ibv_get_device_list(NULL);
    auto ctx = ibv_open_device(*dev_list);
    auto pd = ibv_alloc_pd(ctx);
    EXPECT_NE(pd, nullptr);
    EXPECT_EQ(mr_manager.RegisterMR(pd, buffer, 1024), 0);
    for (int i = 0; i < 10; i++) {
        mr_manager.AllocateSendWR(i, std::string(10, 'a'));
    }
    mr_manager.PrintBlock();
    const RDMA_ECHO::MemBlock* freelist = mr_manager.FreeList();
    const RDMA_ECHO::MemBlock* usedlist = mr_manager.UsedList();
    
    EXPECT_EQ(freelist->next->addr, buffer + 10*10);
    RDMA_ECHO::MemBlock* b = usedlist->next;
    for (int i = 0; i < 10; i++) {
        EXPECT_NE(b, nullptr);
        EXPECT_EQ(b->addr, buffer + i*10);
        EXPECT_EQ(b->sz, 10);
        b = b->next;
    }
    EXPECT_EQ(b, nullptr);
    ibv_free_device_list (dev_list);
    ibv_close_device (ctx);
    ibv_dealloc_pd(pd);
}

TEST(MRManagerTest, AllocateAndRelease) {
    std::FILE* f = std::fopen("test.log", "w");
    auto logger_ = std::make_shared<RDMA_ECHO::FileLogger>(f, true);
    RDMA_ECHO::MRManager mr_manager(logger_);
    char* buffer = new char[1024];

    auto dev_list = ibv_get_device_list(NULL);
    auto ctx = ibv_open_device(*dev_list);
    auto pd = ibv_alloc_pd(ctx);
    EXPECT_NE(pd, nullptr);
    EXPECT_EQ(mr_manager.RegisterMR(pd, buffer, 1024), 0);
    for (int i = 0; i < 10; i++) {
        mr_manager.AllocateSendWR(i, std::string(10, 'a'));
    }
    for (int i =0; i < 10; i++) {
        mr_manager.ReleaseMR(i);
    }
    mr_manager.PrintBlock();
    const RDMA_ECHO::MemBlock* freelist = mr_manager.FreeList();
    const RDMA_ECHO::MemBlock* usedlist = mr_manager.UsedList();
    
    EXPECT_EQ(freelist->next->addr, buffer);
    EXPECT_EQ(freelist->next->next, nullptr);
    ibv_free_device_list (dev_list);
    ibv_close_device (ctx);
    ibv_dealloc_pd(pd);
}