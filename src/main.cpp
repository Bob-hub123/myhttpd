#include <iostream>
#include <cstdio>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <string>
#include <sstream>
#include <unordered_map>
#include <algorithm>

#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

using namespace std;

namespace {

const int PORT = 9006;
const int MAX_EVENTS = 1024;
const size_t MAX_HEADER_SIZE = 8 * 1024;
const size_t MAX_FILE_SIZE = 8 * 1024 * 1024;
const char* DOC_ROOT = "root";

bool setNonBlocking(int fd)
{
    int flags = fcntl(fd, F_GETFL);
    if (flags == -1) {
        return false;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) != -1;
}

struct Connection {
    explicit Connection(int fd)
        : m_fd(fd), m_outSent(0), m_closeAfterWrite(false) {}

    int m_fd;
    std::string m_inBuffer;
    std::string m_outBuffer;
    std::size_t m_outSent;
    bool m_closeAfterWrite;
};

using ConnectionMap = std::unordered_map<int, Connection>;

enum class IoResult {
    Done,
    Pending,
    Error
};

enum class FileResult {
    Ok,
    NotFound,
    Forbidden,
    TooLarge,
    Error
};

enum class ParseResult {
    Incomplete,
    Complete,
    BadRequest,
    TooLarge
};

bool updateEpoll(int epfd, int fd, uint32_t events)
{
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev) == -1) {
        perror("epoll_ctl MOD fail");
        return false;
    }
    return true;
}

void closeConnection(int epfd, int fd, ConnectionMap& connections)
{
    if (epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr) == -1) {
        if (errno != ENOENT && errno != EBADF) {
            perror("epoll_ctl DEL fail");
        }
    }
    connections.erase(fd);
    close(fd);
}

std::string toLower(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) {
                       return static_cast<char>(std::tolower(c));
                   });
    return value;
}

std::string mimeType(const std::string& path)
{
    const std::size_t slash = path.find_last_of('/');
    const std::size_t dot = path.find_last_of('.');
    if (dot == std::string::npos || (slash != std::string::npos && dot < slash)) {
        return "application/octet-stream";
    }

    const std::string ext = toLower(path.substr(dot + 1));
    if (ext == "html" || ext == "htm") {
        return "text/html; charset=utf-8";
    }
    if (ext == "css") {
        return "text/css; charset=utf-8";
    }
    if (ext == "js") {
        return "application/javascript; charset=utf-8";
    }
    if (ext == "txt") {
        return "text/plain; charset=utf-8";
    }
    if (ext == "json") {
        return "application/json; charset=utf-8";
    }
    if (ext == "png") {
        return "image/png";
    }
    if (ext == "jpg" || ext == "jpeg") {
        return "image/jpeg";
    }
    if (ext == "gif") {
        return "image/gif";
    }
    if (ext == "svg") {
        return "image/svg+xml";
    }
    return "application/octet-stream";
}

IoResult flushOutput(Connection& conn)
{
    while (conn.m_outSent < conn.m_outBuffer.size()) {
        const ssize_t n = send(conn.m_fd,
                               conn.m_outBuffer.data() + conn.m_outSent,
                               conn.m_outBuffer.size() - conn.m_outSent,
                               MSG_NOSIGNAL);
        if (n > 0) {
            conn.m_outSent += static_cast<std::size_t>(n);
            continue;
        }
        if (n == -1 && errno == EINTR) {
            continue;
        }
        if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return IoResult::Pending;
        }
        return IoResult::Error;
    }
    return IoResult::Done;
}

void prepareResponse(Connection& conn,
                     const std::string& status,
                     const std::string& contentType,
                     const std::string& body)
{
    conn.m_outBuffer.clear();
    conn.m_outBuffer.reserve(body.size() + 160);
    conn.m_outBuffer += "HTTP/1.1 ";
    conn.m_outBuffer += status;
    conn.m_outBuffer += "\r\n";
    conn.m_outBuffer += "Content-Type: ";
    conn.m_outBuffer += contentType;
    conn.m_outBuffer += "\r\n";
    conn.m_outBuffer += "Content-Length: ";
    conn.m_outBuffer += std::to_string(body.size());
    conn.m_outBuffer += "\r\n";
    conn.m_outBuffer += "Connection: close\r\n";
    conn.m_outBuffer += "Server: myhttpd\r\n";
    conn.m_outBuffer += "\r\n";
    conn.m_outBuffer += body;
    conn.m_outSent = 0;
    conn.m_closeAfterWrite = true;
}

std::string errorBody(int code, const std::string& reason)
{
    std::ostringstream out;
    out << "<!doctype html><html><head><meta charset=\"utf-8\"><title>"
        << code << ' ' << reason
        << "</title></head><body><h1>"
        << code << ' ' << reason
        << "</h1><hr><p>myhttpd</p></body></html>";
    return out.str();
}

int hexValue(char c)
{
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

bool urlDecode(const std::string& input, std::string& output)
{
    output.clear();
    output.reserve(input.size());

    for (std::size_t i = 0; i < input.size(); ++i) {
        if (input[i] != '%') {
            output.push_back(input[i]);
            continue;
        }
        if (i + 2 >= input.size()) {
            return false;
        }

        const int high = hexValue(input[i + 1]);
        const int low = hexValue(input[i + 2]);
        if (high < 0 || low < 0) {
            return false;
        }

        const char decoded = static_cast<char>((high << 4) | low);
        if (decoded == '\0') {
            return false;
        }
        output.push_back(decoded);
        i += 2;
    }
    return true;
}

bool isSafePath(const std::string& path)
{
    if (path.empty() || path[0] != '/') {
        return false;
    }
    if (path.find('\\') != std::string::npos) {
        return false;
    }

    std::size_t start = 1;
    while (start <= path.size()) {
        const std::size_t end = path.find('/', start);
        const std::string segment =
            (end == std::string::npos)
                ? path.substr(start)
                : path.substr(start, end - start);

        if (segment == "..") {
            return false;
        }
        if (end == std::string::npos) {
            break;
        }
        start = end + 1;
    }
    return true;
}

bool buildFilePath(const std::string& target, std::string& filePath)
{
    std::string path = target;
    const std::size_t query = path.find('?');
    if (query != std::string::npos) {
        path = path.substr(0, query);
    }

    std::string decoded;
    if (!urlDecode(path, decoded) || !isSafePath(decoded)) {
        return false;
    }

    if (decoded == "/") {
        decoded = "/index.html";
    } else if (!decoded.empty() && decoded.back() == '/') {
        decoded += "index.html";
    }

    filePath = std::string(DOC_ROOT) + decoded;
    return true;
}

FileResult loadFile(const std::string& path, std::string& body)
{
    const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        if (errno == ENOENT || errno == ENOTDIR) {
            return FileResult::NotFound;
        }
        if (errno == EACCES) {
            return FileResult::Forbidden;
        }
        return FileResult::Error;
    }

    struct stat info {};
    if (fstat(fd, &info) == -1) {
        close(fd);
        return FileResult::Error;
    }
    if (!S_ISREG(info.st_mode)) {
        close(fd);
        return FileResult::NotFound;
    }
    if (info.st_size < 0 ||
        static_cast<unsigned long long>(info.st_size) > MAX_FILE_SIZE) {
        close(fd);
        return FileResult::TooLarge;
    }

    body.resize(static_cast<std::size_t>(info.st_size));
    std::size_t total = 0;
    while (total < body.size()) {
        const ssize_t n = read(fd, &body[total], body.size() - total);
        if (n > 0) {
            total += static_cast<std::size_t>(n);
            continue;
        }
        if (n == -1 && errno == EINTR) {
            continue;
        }
        close(fd);
        return FileResult::Error;
    }

    close(fd);
    return FileResult::Ok;
}

ParseResult parseRequest(const Connection& conn,
                         std::string& method,
                         std::string& target,
                         std::string& version)
{
    const std::size_t headerEnd = conn.m_inBuffer.find("\r\n\r\n");
    if (headerEnd == std::string::npos) {
        return conn.m_inBuffer.size() > MAX_HEADER_SIZE
                   ? ParseResult::TooLarge
                   : ParseResult::Incomplete;
    }
    if (headerEnd + 4 > MAX_HEADER_SIZE) {
        return ParseResult::TooLarge;
    }

    const std::size_t lineEnd = conn.m_inBuffer.find("\r\n");
    if (lineEnd == std::string::npos || lineEnd > headerEnd) {
        return ParseResult::BadRequest;
    }

    std::istringstream input(conn.m_inBuffer.substr(0, lineEnd));
    std::string extra;
    if (!(input >> method >> target >> version)) {
        return ParseResult::BadRequest;
    }
    if (input >> extra) {
        return ParseResult::BadRequest;
    }
    if (version != "HTTP/1.1" && version != "HTTP/1.0") {
        return ParseResult::BadRequest;
    }
    return ParseResult::Complete;
}

bool startResponse(Connection& conn, int epfd, ConnectionMap& connections);
bool sendError(Connection& conn,
               int epfd,
               ConnectionMap& connections,
               int code,
               const std::string& reason);
bool handleRequest(Connection& conn, int epfd, ConnectionMap& connections);
void handleRead(Connection& conn, int epfd, ConnectionMap& connections);
void acceptConnections(int listenFd, int epfd, ConnectionMap& connections);

bool startResponse(Connection& conn, int epfd, ConnectionMap& connections)
{
    const IoResult result = flushOutput(conn);
    if (result == IoResult::Done) {
        if (conn.m_closeAfterWrite) {
            closeConnection(epfd, conn.m_fd, connections);
            return false;
        }

        conn.m_inBuffer.clear();
        conn.m_outBuffer.clear();
        conn.m_outSent = 0;
        if (!updateEpoll(epfd, conn.m_fd, EPOLLIN)) {
            closeConnection(epfd, conn.m_fd, connections);
            return false;
        }
        return true;
    }

    if (result == IoResult::Pending) {
        if (!updateEpoll(epfd, conn.m_fd, EPOLLOUT)) {
            closeConnection(epfd, conn.m_fd, connections);
            return false;
        }
        return true;
    }

    closeConnection(epfd, conn.m_fd, connections);
    return false;
}

bool sendError(Connection& conn,
               int epfd,
               ConnectionMap& connections,
               int code,
               const std::string& reason)
{
    std::cout << "response " << code << ' ' << reason << std::endl;
    prepareResponse(conn,
                    std::to_string(code) + " " + reason,
                    "text/html; charset=utf-8",
                    errorBody(code, reason));
    return startResponse(conn, epfd, connections);
}

bool handleRequest(Connection& conn, int epfd, ConnectionMap& connections)
{
    std::string method;
    std::string target;
    std::string version;
    const ParseResult parsed = parseRequest(conn, method, target, version);

    if (parsed == ParseResult::Incomplete) {
        return true;
    }
    if (parsed == ParseResult::TooLarge) {
        return sendError(conn, epfd, connections, 431,
                         "Request Header Fields Too Large");
    }
    if (parsed == ParseResult::BadRequest) {
        return sendError(conn, epfd, connections, 400, "Bad Request");
    }
    if (method != "GET") {
        return sendError(conn, epfd, connections, 405, "Method Not Allowed");
    }

    std::string filePath;
    if (!buildFilePath(target, filePath)) {
        return sendError(conn, epfd, connections, 400, "Bad Request");
    }

    std::string body;
    const FileResult file = loadFile(filePath, body);
    if (file == FileResult::Ok) {
        std::cout << method << ' ' << target << " -> 200" << std::endl;
        prepareResponse(conn, "200 OK", mimeType(filePath), body);
        return startResponse(conn, epfd, connections);
    }
    if (file == FileResult::NotFound) {
        return sendError(conn, epfd, connections, 404, "Not Found");
    }
    if (file == FileResult::Forbidden) {
        return sendError(conn, epfd, connections, 403, "Forbidden");
    }
    if (file == FileResult::TooLarge) {
        return sendError(conn, epfd, connections, 413, "Payload Too Large");
    }
    return sendError(conn, epfd, connections, 500, "Internal Server Error");
}

void handleRead(Connection& conn, int epfd, ConnectionMap& connections)
{
    char buffer[4096];
    bool peerClosed = false;

    while (true) {
        const ssize_t n = recv(conn.m_fd, buffer, sizeof(buffer), 0);
        if (n > 0) {
            conn.m_inBuffer.append(buffer, static_cast<std::size_t>(n));
            if (conn.m_inBuffer.find("\r\n\r\n") != std::string::npos) {
                break;
            }
            if (conn.m_inBuffer.size() > MAX_HEADER_SIZE) {
                break;
            }
            continue;
        }
        if (n == 0) {
            peerClosed = true;
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }

        perror("recv fail");
        closeConnection(epfd, conn.m_fd, connections);
        return;
    }

    if (conn.m_inBuffer.empty()) {
        closeConnection(epfd, conn.m_fd, connections);
        return;
    }

    const bool alive = handleRequest(conn, epfd, connections);
    if (!alive) {
        return;
    }

    if (peerClosed && conn.m_outBuffer.empty()) {
        prepareResponse(conn,
                        "400 Bad Request",
                        "text/html; charset=utf-8",
                        errorBody(400, "Bad Request"));
        startResponse(conn, epfd, connections);
    }
}

void acceptConnections(int listenFd, int epfd, ConnectionMap& connections)
{
    while (true) {
        sockaddr_in clientAddr {};
        socklen_t addrLen = sizeof(clientAddr);
        const int clientFd =
            accept(listenFd, reinterpret_cast<sockaddr*>(&clientAddr), &addrLen);

        if (clientFd == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            if (errno == EINTR || errno == ECONNABORTED) {
                continue;
            }
            perror("accept fail");
            break;
        }

        if (!setNonBlocking(clientFd)) {
            perror("setNonBlocking client fail");
            close(clientFd);
            continue;
        }

        const auto inserted =
            connections.emplace(clientFd, Connection(clientFd));
        if (!inserted.second) {
            close(clientFd);
            continue;
        }

        epoll_event ev {};
        ev.events = EPOLLIN;
        ev.data.fd = clientFd;
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, clientFd, &ev) == -1) {
            perror("epoll_ctl ADD client fail");
            connections.erase(clientFd);
            close(clientFd);
            continue;
        }

        char ip[INET_ADDRSTRLEN] = "unknown";
        inet_ntop(AF_INET, &clientAddr.sin_addr, ip, sizeof(ip));
        std::cout << "new connection fd=" << clientFd
                  << " from " << ip << ':' << ntohs(clientAddr.sin_port)
                  << std::endl;
    }
}

} // namespace

int main()
{
    std::signal(SIGPIPE, SIG_IGN);

    const int listenFd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd == -1) {
        perror("socket fail");
        return 1;
    }
    if (!setNonBlocking(listenFd)) {
        perror("setNonBlocking listen fd fail");
        close(listenFd);
        return 1;
    }

    int opt = 1;
    if (setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        perror("setsockopt fail");
        close(listenFd);
        return 1;
    }

    sockaddr_in local {};
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = htonl(INADDR_ANY);
    local.sin_port = htons(PORT);

    if (bind(listenFd, reinterpret_cast<sockaddr*>(&local), sizeof(local)) == -1) {
        perror("bind fail");
        close(listenFd);
        return 1;
    }
    if (listen(listenFd, SOMAXCONN) == -1) {
        perror("listen fail");
        close(listenFd);
        return 1;
    }

    const int epfd = epoll_create1(0);
    if (epfd == -1) {
        perror("epoll_create1 fail");
        close(listenFd);
        return 1;
    }

    epoll_event ev {};
    ev.events = EPOLLIN;
    ev.data.fd = listenFd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, listenFd, &ev) == -1) {
        perror("epoll_ctl ADD listen fd fail");
        close(epfd);
        close(listenFd);
        return 1;
    }

    ConnectionMap connections;
    epoll_event events[MAX_EVENTS];
    bool running = true;
    int exitCode = 0;

    while (running) {
        const int num = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (num == -1) {
            if (errno == EINTR) {
                continue;
            }
            perror("epoll_wait fail");
            exitCode = 1;
            break;
        }

        for (int i = 0; i < num && running; ++i) {
            const int fd = events[i].data.fd;
            const uint32_t eventMask = events[i].events;

            if (fd == listenFd) {
                if (eventMask & (EPOLLERR | EPOLLHUP)) {
                    std::cerr << "listen fd error" << std::endl;
                    exitCode = 1;
                    running = false;
                    break;
                }
                if (eventMask & EPOLLIN) {
                    acceptConnections(listenFd, epfd, connections);
                }
                continue;
            }

            auto it = connections.find(fd);
            if (it == connections.end()) {
                epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                continue;
            }

            if (eventMask & EPOLLERR) {
                closeConnection(epfd, fd, connections);
                continue;
            }

            if (eventMask & EPOLLIN) {
                handleRead(it->second, epfd, connections);
                if (connections.find(fd) == connections.end()) {
                    continue;
                }
            }

            if (eventMask & EPOLLOUT) {
                auto writeIt = connections.find(fd);
                if (writeIt == connections.end()) {
                    continue;
                }
                const IoResult result = flushOutput(writeIt->second);
                if (result == IoResult::Done || result == IoResult::Error) {
                    closeConnection(epfd, fd, connections);
                    continue;
                }
            }

            if ((eventMask & EPOLLHUP) &&
                connections.find(fd) != connections.end()) {
                closeConnection(epfd, fd, connections);
            }
        }
    }

    for (const auto& entry : connections) {
        close(entry.first);
    }
    connections.clear();
    close(epfd);
    close(listenFd);
    return exitCode;
}
