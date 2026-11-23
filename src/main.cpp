#include <iostream>
#include <cstdio>
#include <cerrno>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include<csignal>
using namespace std;

const int PORT       = 9006;
const int MAX_EVENTS = 1024;   // epoll_wait 的 maxevents 必须和数组长度一致

// 把 fd 设为非阻塞：先 F_GETFL 取原标志位，再按位或上 O_NONBLOCK
// 不能直接 F_SETFL 传 O_NONBLOCK，那样会把原有的标志位全部清掉
bool setNonBlocking(int fd)
{
	int flags = fcntl(fd, F_GETFL);
	if (flags == -1) { return false; }
	return fcntl(fd, F_SETFL, flags | O_NONBLOCK) != -1;
}
bool sendAll(int fd, const char* data, size_t len)
{
	size_t sent = 0;

	while (sent < len) {
		ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);

		if (n > 0) {
			sent += n;          // 这次发出去了 n 字节
			continue;
		}

		if (n == -1 && errno == EINTR) {
			continue;           // 被信号打断，重试
		}

		if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			std::cerr << "send buffer full, fd=" << fd << std::endl;
			return false;       // M0 暂时关闭连接
		}

		return false;           // n == 0 或其它错误
	}

	return true;
}

int main()
{
	std::signal(SIGPIPE, SIG_IGN);
	int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (listen_fd == -1) { perror("socket fail"); return 1; }

	int opt = 1;
	if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
		perror("setsockopt fail");
		return 1;
	}

	sockaddr_in local{};
	local.sin_family = AF_INET;
	local.sin_addr.s_addr = htonl(INADDR_ANY);
	local.sin_port = htons(PORT);

	if (bind(listen_fd, (sockaddr *)&local, sizeof(local)) == -1) {
		perror("bind fail");
		return 1;
	}
	// backlog 是全连接队列长度，不是并发上限
	if (listen(listen_fd, SOMAXCONN) == -1) { perror("listen fail"); return 1; }

	int epfd = epoll_create1(0);
	if (epfd == -1) { perror("epoll_create1 fail"); return 1; }

	epoll_event ev{};
	ev.events = EPOLLIN;
	ev.data.fd = listen_fd;
	if (epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev) == -1) {
		perror("epoll_ctl add listen_fd fail");
		return 1;
	}

	epoll_event events[MAX_EVENTS];

	while (true) {
		int num = epoll_wait(epfd, events, MAX_EVENTS, -1);
		if (num == -1) {
			if (errno == EINTR) { continue; }   // 被信号打断，重新等
			perror("epoll_wait fail");
			break;
		}

		for (int i = 0; i < num; i++) {
			int fd = events[i].data.fd;

			if (fd == listen_fd) {              // 监听 fd 可读 = 有新连接
				sockaddr_in clientAddr{};
				socklen_t addrLen = sizeof(clientAddr);
				int client_fd = accept(listen_fd, (sockaddr *)&clientAddr, &addrLen);
				if (client_fd == -1) {
					if (errno == EAGAIN || errno == EWOULDBLOCK) { continue; }
					perror("accept fail");
					continue;
				}
				// 关键：accept 出来的 fd 默认是阻塞的，必须手动改成非阻塞
				if (!setNonBlocking(client_fd)) {
					perror("setNonBlocking fail");
					close(client_fd);
					continue;
				}

				epoll_event evs{};
				evs.events = EPOLLIN;
				evs.data.fd = client_fd;
				if (epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &evs) == -1) {
					perror("epoll_ctl add client_fd fail");
					close(client_fd);
					continue;
				}
				cout << "有新客户端连接：fd = " << client_fd << endl;
			}
			else {                              // 已连接 fd 可读
				char buffer[1024];
				while (true) {
					// 留 1 字节给 '\0'，所以最多读 sizeof(buffer)-1
					int len = recv(fd, buffer, sizeof(buffer) - 1, 0);

					if (len == 0) {             // 对端正常关闭，不是错误
						cout << "客户端断开：fd = " << fd << endl;
						epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
						close(fd);
						break;
					}
					if (len < 0) {
						if (errno == EAGAIN || errno == EWOULDBLOCK) { break; }  // 数据读干净了
						if (errno == EINTR) { continue; }                        // 被信号打断，重试
						perror("recv fail");                                     // 真正的错误
						epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
						close(fd);
						break;
					}

					buffer[len] = '\0';         // recv 不补终止符，先补再打印
					cout << "收到 " << len << " 字节：" << buffer << endl;

					// 发实际读到的字节数 len，不是缓冲区总大小
					if (!sendAll(fd, buffer, static_cast<size_t>(len))) {
						epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
						close(fd);
						break;
					}
				}
			}
		}
	}

	close(epfd);
	close(listen_fd);
	return 0;
}
