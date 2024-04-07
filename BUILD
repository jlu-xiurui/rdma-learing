cc_library (
    name = "rdma_proxy",
    hdrs = ["rdma_proxy.h",
            "logger.h",
            "mr_manager.h"],
    srcs = ["rdma_proxy.cc",
            "logger.cc",
            "mr_manager.cc"],
    linkopts = ["-lrdmacm","-libverbs", "-pthread"],
)
cc_library(
    name = "rdma_client",
    hdrs = ["rdma_client.h"],
    deps = [":rdma_proxy"],
    linkopts = ["-lrdmacm","-libverbs", "-pthread"],
)
cc_binary(
    name = "client",
    srcs = ["client.cc"],
    deps = [
        ":rdma_client",
    ],
    copts = ["-g"],
)

cc_library(
    name = "rdma_server",
    hdrs = ["rdma_server.h"],
    deps = [":rdma_proxy"],
    linkopts = ["-lrdmacm","-libverbs", "-pthread"],
)
cc_binary(
    name = "server",
    srcs = ["server.cc"],
    deps = [
        ":rdma_server",
    ],
    copts = ["-g"],
)

cc_test(
  name = "mr_manager_test",
  srcs = ["mr_manager_test.cc"],
  deps = ["@googletest//:gtest_main",
          ":rdma_proxy"],
)