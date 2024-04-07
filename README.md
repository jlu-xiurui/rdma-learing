使用librdmacm实现了RDMA发送字符串和接受字符串的基本功能，其中：

- RDMAProxy：实现了发送信息SendMessage、接受信息RecvMessage、主动关闭链接功能；
- RDMAClient：根据目标id:port建立RDMA链接的客户端；
- RDMAServer：在端口port监听RDMA链接请求；

使用方法

```shell
// terminal a:
bazel build client
./bazel-bin/client

// terminal b:
bazel build server
./bazel-bin/server
```
