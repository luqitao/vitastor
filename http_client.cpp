#include <netinet/tcp.h>
#include <sys/epoll.h>

#include <net/if.h>
#include <arpa/inet.h>
#include <ifaddrs.h>

#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>

#include "json11/json11.hpp"
#include "http_client.h"
#include "timerfd_manager.h"

#define READ_BUFFER_SIZE 9000

static int extract_port(std::string & host);
static std::string strtolower(const std::string & in);
static std::string trim(const std::string & in);
static std::string ws_format_frame(int type, uint64_t size);
static bool ws_parse_frame(std::string & buf, int & type, std::string & res);

// FIXME: Use keepalive
struct http_co_t
{
    timerfd_manager_t *tfd;

    int request_timeout = 0;
    std::string host;
    std::string request;
    std::string ws_outbox;
    std::string response;
    bool want_streaming;

    http_response_t parsed;
    uint64_t target_response_size = 0;

    int state = 0;
    int peer_fd = -1;
    int timeout_id = -1;
    int epoll_events = 0;
    int sent = 0;
    std::vector<char> rbuf;
    iovec read_iov, send_iov;
    msghdr read_msg = { 0 }, send_msg = { 0 };

    std::function<void(const http_response_t*)> callback;

    websocket_t ws;

    ~http_co_t();
    void start_connection();
    void handle_connect_result();
    void submit_read();
    void submit_send();
    bool handle_read();
    void post_message(int type, const std::string & msg);
};

#define HTTP_CO_CONNECTING 1
#define HTTP_CO_SENDING_REQUEST 2
#define HTTP_CO_REQUEST_SENT 3
#define HTTP_CO_HEADERS_RECEIVED 4
#define HTTP_CO_WEBSOCKET 5
#define HTTP_CO_CHUNKED 6

#define DEFAULT_TIMEOUT 5000

void http_request(timerfd_manager_t *tfd, const std::string & host, const std::string & request,
    const http_options_t & options, std::function<void(const http_response_t *response)> callback)
{
    http_co_t *handler = new http_co_t();
    handler->request_timeout = options.timeout < 0 ? 0 : (options.timeout == 0 ? DEFAULT_TIMEOUT : options.timeout);
    handler->want_streaming = options.want_streaming;
    handler->tfd = tfd;
    handler->host = host;
    handler->request = request;
    handler->callback = callback;
    handler->ws.co = handler;
    handler->start_connection();
}

void http_request_json(timerfd_manager_t *tfd, const std::string & host, const std::string & request,
    int timeout, std::function<void(std::string, json11::Json r)> callback)
{
    http_request(tfd, host, request, { .timeout = timeout }, [callback](const http_response_t* res)
    {
        if (res->error_code != 0)
        {
            callback("Error code: "+std::to_string(res->error_code)+" ("+std::string(strerror(res->error_code))+")", json11::Json());
            return;
        }
        if (res->status_code != 200)
        {
            callback("HTTP "+std::to_string(res->status_code)+" "+res->status_line+" body: "+trim(res->body), json11::Json());
            return;
        }
        std::string json_err;
        json11::Json data = json11::Json::parse(res->body, json_err);
        if (json_err != "")
        {
            callback("Bad JSON: "+json_err+" (response: "+trim(res->body)+")", json11::Json());
            return;
        }
        callback(std::string(), data);
    });
}

websocket_t* open_websocket(timerfd_manager_t *tfd, const std::string & host, const std::string & path,
    int timeout, std::function<void(const http_response_t *msg)> callback)
{
    std::string request = "GET "+path+" HTTP/1.1\r\n"
        "Host: "+host+"\r\n"
        "Upgrade: websocket\r\n"
        "Connection: upgrade\r\n"
        "Sec-WebSocket-Key: x3JJHMbDL1EzLkh9GBhXDw==\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n";
    http_co_t *handler = new http_co_t();
    handler->request_timeout = timeout < 0 ? -1 : (timeout == 0 ? DEFAULT_TIMEOUT : timeout);
    handler->want_streaming = false;
    handler->tfd = tfd;
    handler->host = host;
    handler->request = request;
    handler->callback = callback;
    handler->ws.co = handler;
    handler->start_connection();
    return &handler->ws;
}

void websocket_t::post_message(int type, const std::string & msg)
{
    co->post_message(type, msg);
}

void websocket_t::close()
{
    delete co;
}

http_co_t::~http_co_t()
{
    if (timeout_id >= 0)
    {
        tfd->clear_timer(timeout_id);
        timeout_id = -1;
    }
    if (peer_fd >= 0)
    {
        tfd->set_fd_handler(peer_fd, false, NULL);
        close(peer_fd);
        peer_fd = -1;
    }
    if (parsed.headers["transfer-encoding"] == "chunked")
    {
        int prev = 0, pos = 0;
        while ((pos = response.find("\r\n", prev)) >= prev)
        {
            uint64_t len = strtoull(response.c_str()+prev, NULL, 16);
            parsed.body += response.substr(pos+2, len);
            prev = pos+2+len+2;
        }
    }
    else
    {
        std::swap(parsed.body, response);
    }
    parsed.eof = true;
    callback(&parsed);
}

void http_co_t::start_connection()
{
    int port = extract_port(host);
    struct sockaddr_in addr;
    int r;
    if ((r = inet_pton(AF_INET, host.c_str(), &addr.sin_addr)) != 1)
    {
        parsed.error_code = ENXIO;
        // FIXME 'delete this' is ugly...
        delete this;
        return;
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port ? port : 80);
    peer_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (peer_fd < 0)
    {
        parsed.error_code = errno;
        delete this;
        return;
    }
    fcntl(peer_fd, F_SETFL, fcntl(peer_fd, F_GETFL, 0) | O_NONBLOCK);
    if (request_timeout > 0)
    {
        timeout_id = tfd->set_timer(request_timeout, false, [this](int timer_id)
        {
            if (response.length() == 0)
            {
                parsed.error_code = ETIME;
            }
            delete this;
        });
    }
    tfd->set_fd_handler(peer_fd, true, [this](int peer_fd, int epoll_events)
    {
        this->epoll_events |= epoll_events;
        handle_connect_result();
    });
    epoll_events = 0;
    // Finally call connect
    r = ::connect(peer_fd, (sockaddr*)&addr, sizeof(addr));
    if (r < 0 && errno != EINPROGRESS)
    {
        parsed.error_code = errno;
        delete this;
        return;
    }
    state = HTTP_CO_CONNECTING;
}

void http_co_t::handle_connect_result()
{
    if (epoll_events & (EPOLLOUT | EPOLLERR))
    {
        int result = 0;
        socklen_t result_len = sizeof(result);
        if (getsockopt(peer_fd, SOL_SOCKET, SO_ERROR, &result, &result_len) < 0)
        {
            result = errno;
        }
        if (result != 0)
        {
            parsed.error_code = result;
            delete this;
            return;
        }
        int one = 1;
        setsockopt(peer_fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
        tfd->set_fd_handler(peer_fd, false, [this](int peer_fd, int epoll_events)
        {
            this->epoll_events |= epoll_events;
            if (this->epoll_events & EPOLLIN)
            {
                submit_read();
            }
            else if (this->epoll_events & (EPOLLRDHUP|EPOLLERR))
            {
                delete this;
            }
        });
        state = HTTP_CO_SENDING_REQUEST;
        submit_send();
    }
    else
    {
        delete this;
    }
}

void http_co_t::submit_read()
{
    int res;
again:
    if (rbuf.size() != READ_BUFFER_SIZE)
    {
        rbuf.resize(READ_BUFFER_SIZE);
    }
    read_iov = { .iov_base = rbuf.data(), .iov_len = READ_BUFFER_SIZE };
    read_msg.msg_iov = &read_iov;
    read_msg.msg_iovlen = 1;
    epoll_events = epoll_events & ~EPOLLIN;
    res = recvmsg(peer_fd, &read_msg, 0);
    if (res < 0)
    {
        res = -errno;
    }
    if (res == -EAGAIN)
    {
        res = 0;
    }
    if (res < 0)
    {
        delete this;
        return;
    }
    response += std::string(rbuf.data(), res);
    if (res == READ_BUFFER_SIZE)
    {
        goto again;
    }
    if (!handle_read())
    {
        return;
    }
    if (res < READ_BUFFER_SIZE && (epoll_events & (EPOLLRDHUP|EPOLLERR)))
    {
        delete this;
        return;
    }
}

void http_co_t::submit_send()
{
    int res;
again:
    if (sent < request.size())
    {
        send_iov = (iovec){ .iov_base = (void*)(request.c_str()+sent), .iov_len = request.size()-sent };
        send_msg.msg_iov = &send_iov;
        send_msg.msg_iovlen = 1;
        res = sendmsg(peer_fd, &send_msg, 0);
        if (res < 0)
        {
            res = -errno;
        }
        if (res == -EAGAIN)
        {
            res = 0;
        }
        else if (res < 0)
        {
            delete this;
            return;
        }
        sent += res;
        if (state == HTTP_CO_SENDING_REQUEST)
        {
            if (sent >= request.size())
                state = HTTP_CO_REQUEST_SENT;
            else
                goto again;
        }
        else if (state == HTTP_CO_WEBSOCKET)
        {
            request = request.substr(sent);
            sent = 0;
            goto again;
        }
    }
}

bool http_co_t::handle_read()
{
    if (state == HTTP_CO_REQUEST_SENT)
    {
        int pos = response.find("\r\n\r\n");
        if (pos >= 0)
        {
            if (timeout_id >= 0)
            {
                tfd->clear_timer(timeout_id);
                timeout_id = -1;
            }
            state = HTTP_CO_HEADERS_RECEIVED;
            parse_http_headers(response, &parsed);
            if (parsed.status_code == 101 &&
                parsed.headers.find("sec-websocket-accept") != parsed.headers.end() &&
                parsed.headers["upgrade"] == "websocket" &&
                parsed.headers["connection"] == "upgrade")
            {
                // Don't care about validating the key
                state = HTTP_CO_WEBSOCKET;
                request = ws_outbox;
                ws_outbox = "";
                sent = 0;
                submit_send();
            }
            else if (parsed.headers["transfer-encoding"] == "chunked")
            {
                state = HTTP_CO_CHUNKED;
            }
            else if (parsed.headers["connection"] != "close")
            {
                target_response_size = stoull_full(parsed.headers["content-length"]);
                if (!target_response_size)
                {
                    // Sorry, unsupported response
                    delete this;
                    return false;
                }
            }
        }
    }
    if (state == HTTP_CO_HEADERS_RECEIVED && target_response_size > 0 && response.size() >= target_response_size)
    {
        delete this;
        return false;
    }
    if (state == HTTP_CO_CHUNKED && response.size() > 0)
    {
        int prev = 0, pos = 0;
        while ((pos = response.find("\r\n", prev)) >= prev)
        {
            uint64_t len = strtoull(response.c_str()+prev, NULL, 16);
            if (!len)
            {
                // Zero length chunk indicates EOF
                parsed.eof = true;
                break;
            }
            if (response.size() < pos+2+len+2)
            {
                break;
            }
            parsed.body += response.substr(pos+2, len);
            prev = pos+2+len+2;
        }
        if (prev > 0)
        {
            response = response.substr(prev);
        }
        if (parsed.eof)
        {
            delete this;
            return false;
        }
        if (want_streaming && parsed.body.size() > 0)
        {
            callback(&parsed);
            parsed.body = "";
        }
    }
    if (state == HTTP_CO_WEBSOCKET && response.size() > 0)
    {
        while (ws_parse_frame(response, parsed.ws_msg_type, parsed.body))
        {
            callback(&parsed);
            parsed.body = "";
        }
    }
    return true;
}

void http_co_t::post_message(int type, const std::string & msg)
{
    if (state == HTTP_CO_WEBSOCKET)
    {
        request += ws_format_frame(type, msg.size());
        request += msg;
        submit_send();
    }
    else
    {
        ws_outbox += ws_format_frame(type, msg.size());
        ws_outbox += msg;
    }
}

uint64_t stoull_full(const std::string & str, int base)
{
    if (isspace(str[0]))
    {
        return 0;
    }
    char *end = NULL;
    uint64_t r = strtoull(str.c_str(), &end, base);
    if (end != str.c_str()+str.length())
    {
        return 0;
    }
    return r;
}

void parse_http_headers(std::string & res, http_response_t *parsed)
{
    int pos = res.find("\r\n");
    pos = pos < 0 ? res.length() : pos+2;
    std::string status_line = res.substr(0, pos);
    int http_version;
    char *status_text = NULL;
    sscanf(status_line.c_str(), "HTTP/1.%d %d %ms", &http_version, &parsed->status_code, &status_text);
    if (status_text)
    {
        parsed->status_line = status_text;
        // %ms = allocate a buffer
        free(status_text);
        status_text = NULL;
    }
    int prev = pos;
    while ((pos = res.find("\r\n", prev)) >= prev)
    {
        if (pos == prev)
        {
            res = res.substr(pos+2);
            break;
        }
        std::string header = res.substr(prev, pos-prev);
        int p2 = header.find(":");
        if (p2 >= 0)
        {
            std::string key = strtolower(header.substr(0, p2));
            int p3 = p2+1;
            while (p3 < header.length() && isblank(header[p3]))
                p3++;
            parsed->headers[key] = key == "connection" || key == "upgrade" || key == "transfer-encoding"
                ? strtolower(header.substr(p3)) : header.substr(p3);
        }
        prev = pos+2;
    }
}

static std::string ws_format_frame(int type, uint64_t size)
{
    // Always zero mask
    std::string res;
    int p = 0;
    res.resize(2 + (size >= 126 ? 2 : 0) + (size >= 65536 ? 6 : 0) + /*mask*/4);
    res[p++] = 0x80 | type;
    if (size < 126)
        res[p++] = size | /*mask*/0x80;
    else if (size < 65536)
    {
        res[p++] = 126 | /*mask*/0x80;
        res[p++] = (size >> 8) & 0xFF;
        res[p++] = (size >> 0) & 0xFF;
    }
    else
    {
        res[p++] = 127 | /*mask*/0x80;
        res[p++] = (size >> 56) & 0xFF;
        res[p++] = (size >> 48) & 0xFF;
        res[p++] = (size >> 40) & 0xFF;
        res[p++] = (size >> 32) & 0xFF;
        res[p++] = (size >> 24) & 0xFF;
        res[p++] = (size >> 16) & 0xFF;
        res[p++] = (size >>  8) & 0xFF;
        res[p++] = (size >>  0) & 0xFF;
    }
    res[p++] = 0;
    res[p++] = 0;
    res[p++] = 0;
    res[p++] = 0;
    return res;
}

static bool ws_parse_frame(std::string & buf, int & type, std::string & res)
{
    uint64_t hdr = 2;
    if (buf.size() < hdr)
    {
        return false;
    }
    type = buf[0] & ~0x80;
    bool mask = !!(buf[1] & 0x80);
    hdr += mask ? 4 : 0;
    uint64_t len = ((uint8_t)buf[1] & ~0x80);
    if (len == 126)
    {
        hdr += 2;
        if (buf.size() < hdr)
        {
            return false;
        }
        len = ((uint64_t)(uint8_t)buf[2] << 8) | ((uint64_t)(uint8_t)buf[3] << 0);
    }
    else if (len == 127)
    {
        hdr += 8;
        if (buf.size() < hdr)
        {
            return false;
        }
        len = ((uint64_t)(uint8_t)buf[2] << 56) |
            ((uint64_t)(uint8_t)buf[3] << 48) |
            ((uint64_t)(uint8_t)buf[4] << 40) |
            ((uint64_t)(uint8_t)buf[5] << 32) |
            ((uint64_t)(uint8_t)buf[6] << 24) |
            ((uint64_t)(uint8_t)buf[7] << 16) |
            ((uint64_t)(uint8_t)buf[8] << 8) |
            ((uint64_t)(uint8_t)buf[9] << 0);
    }
    if (buf.size() < hdr+len)
    {
        return false;
    }
    if (mask)
    {
        for (int i = 0; i < len; i++)
            buf[hdr+i] ^= buf[hdr-4+(i & 3)];
    }
    res += buf.substr(hdr, len);
    buf = buf.substr(hdr+len);
    return true;
}

std::vector<std::string> getifaddr_list(bool include_v6)
{
    std::vector<std::string> addresses;
    ifaddrs *list, *ifa;
    if (getifaddrs(&list) == -1)
    {
        throw std::runtime_error(std::string("getifaddrs: ") + strerror(errno));
    }
    for (ifa = list; ifa != NULL; ifa = ifa->ifa_next)
    {
        if (!ifa->ifa_addr)
        {
            continue;
        }
        int family = ifa->ifa_addr->sa_family;
        if ((family == AF_INET || family == AF_INET6 && include_v6) &&
            (ifa->ifa_flags & (IFF_UP | IFF_RUNNING | IFF_LOOPBACK)) == (IFF_UP | IFF_RUNNING))
        {
            void *addr_ptr;
            if (family == AF_INET)
                addr_ptr = &((sockaddr_in *)ifa->ifa_addr)->sin_addr;
            else
                addr_ptr = &((sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
            char addr[INET6_ADDRSTRLEN];
            if (!inet_ntop(family, addr_ptr, addr, INET6_ADDRSTRLEN))
            {
                throw std::runtime_error(std::string("inet_ntop: ") + strerror(errno));
            }
            addresses.push_back(std::string(addr));
        }
    }
    freeifaddrs(list);
    return addresses;
}

static int extract_port(std::string & host)
{
    int port = 0;
    int pos = 0;
    if ((pos = host.find(':')) >= 0)
    {
        port = strtoull(host.c_str() + pos + 1, NULL, 10);
        if (port >= 0x10000)
        {
            port = 0;
        }
        host = host.substr(0, pos);
    }
    return port;
}

static std::string strtolower(const std::string & in)
{
    std::string s = in;
    for (int i = 0; i < s.length(); i++)
    {
        s[i] = tolower(s[i]);
    }
    return s;
}

static std::string trim(const std::string & in)
{
    int begin = in.find_first_not_of(" \n\r\t");
    if (begin == -1)
        return "";
    int end = in.find_last_not_of(" \n\r\t");
    return in.substr(begin, end+1-begin);
}
