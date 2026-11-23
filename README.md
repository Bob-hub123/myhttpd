# myhttpd

从零手写的 Linux 高性能 Web 服务器（C++ / epoll）。

> 设计参考 TinyWebServer（qinguoyi），代码独立实现，并做了若干改进。

## 目标

- 掌握 epoll 事件驱动 + 半同步半反应堆线程池的完整实现
- 用可运行的最小里程碑推进，每个阶段都能编译、能 curl、能压测
- 产出可用于 C++ 后台实习简历的项目（含压测数据与架构文档）

## 里程碑

见 `重写路线图.md`（M0 echo → M7 压测收尾）。

当前进度：

- `v0.1-echo`：单线程 LT epoll echo 服务器
- `v0.2-http`：GET 静态文件、404、半包读取、非阻塞写缓冲
- M2：主线程事件分发 + 固定线程池处理连接

## 当前实现

- 单线程 epoll（LT）事件循环，监听 fd 非阻塞，accept 循环到 `EAGAIN`
- 每个连接独立保存输入缓冲和输出缓冲，按 fd 按需创建，不做 65536 预分配
- 解析请求行 `GET <path> HTTP/1.x`，返回 `root/` 下的静态文件
- 支持 `Content-Length`、常见 MIME 类型、404/405/400/413/431
- URL 解码后拦截 `..` 和反斜杠，避免基础路径穿越
- 写缓冲 + `EPOLLOUT` 处理部分发送
- 4 个 worker 线程 + 有界任务队列（1024），主线程只负责 epoll / accept / 派发
- `EPOLLONESHOT` 保证同一个连接同一时刻只被一个 worker 处理
- 连接用 `shared_ptr` 管理，连接表由互斥锁保护，避免 worker 关闭时悬空
- 队列满时关闭新请求，作为当前版本的背压策略

## 与原版的差异（逐步补齐）

- [x] CMake 构建（替代裸 makefile）
- [ ] 定时器用小根堆 / timerfd（替代升序链表 O(n)）
- [x] 连接对象按需分配（替代 65536 预分配数组）
- [x] 线程池任务队列有界，队列满时主动拒绝连接

## 构建与运行

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
./build/myhttpd
```

请在项目根目录运行，因为静态文件目录 `root/` 是相对工作目录解析的。

测试：

```bash
curl -v http://127.0.0.1:9006/
curl -v http://127.0.0.1:9006/welcome.html
curl -v http://127.0.0.1:9006/missing.html
```

端口目前硬编码在 `src/main.cpp` 中（M0 验收标准为 9006），命令行参数解析留到后续里程碑。

线程数和任务队列上限也在 `src/main.cpp` 顶部：`THREAD_NUMBER`、`MAX_REQUESTS`。

> 开发方式：CLion Remote Host 连云服务器（Ubuntu），CMake 在远端 configure/编译；改完 `CMakeLists.txt` 需在 CLion 里 Reload CMake Project。

## 压测

```bash
./webbench -c 1000 -t 30 http://127.0.0.1:9006/
```
