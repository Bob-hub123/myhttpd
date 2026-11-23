# myhttpd

从零手写的 Linux 高性能 Web 服务器（C++ / epoll）。

> 设计参考 TinyWebServer（qinguoyi），代码独立实现，并做了若干改进。

## 目标

- 掌握 epoll 事件驱动 + 半同步半反应堆线程池的完整实现
- 用可运行的最小里程碑推进，每个阶段都能编译、能 curl、能压测
- 产出可用于 C++ 后台实习简历的项目（含压测数据与架构文档）

## 里程碑

见 `重写路线图.md`（M0 echo → M7 压测收尾）。

## 与原版的差异（逐步补齐）

- [ ] CMake 构建（替代裸 makefile）
- [ ] 定时器用小根堆 / timerfd（替代升序链表 O(n)）
- [ ] 连接对象按需分配（替代 65536 预分配数组）

## 构建与运行

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
./build/myhttpd
```

端口目前硬编码在 `src/main.cpp` 中（M0 验收标准为 9006），命令行参数解析留到后续里程碑。

> 开发方式：CLion Remote Host 连云服务器（Ubuntu），CMake 在远端 configure/编译；改完 `CMakeLists.txt` 需在 CLion 里 Reload CMake Project。

## 压测

```bash
./webbench -c 1000 -t 30 http://127.0.0.1:9006/
```
