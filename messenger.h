#pragma once

#include <sys/types.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <malloc.h>

#include <set>
#include <map>
#include <deque>
#include <vector>

#include "json11/json11.hpp"
#include "osd_ops.h"
#include "timerfd_manager.h"
#include "ringloop.h"

#define OSD_OP_IN 0
#define OSD_OP_OUT 1

#define CL_READ_HDR 1
#define CL_READ_DATA 2
#define CL_READ_REPLY_DATA 3
#define CL_WRITE_READY 1
#define CL_WRITE_REPLY 2
#define OSD_OP_INLINE_BUF_COUNT 16

#define PEER_CONNECTING 1
#define PEER_CONNECTED 2

#define DEFAULT_PEER_CONNECT_INTERVAL 5
#define DEFAULT_PEER_CONNECT_TIMEOUT 5

// Kind of a vector with small-list-optimisation
struct osd_op_buf_list_t
{
    int count = 0, alloc = OSD_OP_INLINE_BUF_COUNT, done = 0;
    iovec *buf = NULL;
    iovec inline_buf[OSD_OP_INLINE_BUF_COUNT];

    inline osd_op_buf_list_t()
    {
        buf = inline_buf;
    }

    inline osd_op_buf_list_t(const osd_op_buf_list_t & other)
    {
        buf = inline_buf;
        append(other);
    }

    inline osd_op_buf_list_t & operator = (const osd_op_buf_list_t & other)
    {
        reset();
        append(other);
        return *this;
    }

    inline ~osd_op_buf_list_t()
    {
        if (buf && buf != inline_buf)
        {
            free(buf);
        }
    }

    inline void reset()
    {
        count = 0;
        done = 0;
    }

    inline iovec* get_iovec()
    {
        return buf + done;
    }

    inline int get_size()
    {
        return count - done;
    }

    inline void append(const osd_op_buf_list_t & other)
    {
        if (count+other.count > alloc)
        {
            if (buf == inline_buf)
            {
                int old = alloc;
                alloc = (((count+other.count+15)/16)*16);
                buf = (iovec*)malloc(sizeof(iovec) * alloc);
                memcpy(buf, inline_buf, sizeof(iovec) * old);
            }
            else
            {
                alloc = (((count+other.count+15)/16)*16);
                buf = (iovec*)realloc(buf, sizeof(iovec) * alloc);
            }
        }
        for (int i = 0; i < other.count; i++)
        {
            buf[count++] = other.buf[i];
        }
    }

    inline void push_back(void *nbuf, size_t len)
    {
        if (count >= alloc)
        {
            if (buf == inline_buf)
            {
                int old = alloc;
                alloc = ((alloc/16)*16 + 1);
                buf = (iovec*)malloc(sizeof(iovec) * alloc);
                memcpy(buf, inline_buf, sizeof(iovec)*old);
            }
            else
            {
                alloc = alloc < 16 ? 16 : (alloc+16);
                buf = (iovec*)realloc(buf, sizeof(iovec) * alloc);
            }
        }
        buf[count++] = { .iov_base = nbuf, .iov_len = len };
    }

    inline void eat(int result)
    {
        while (result > 0 && done < count)
        {
            iovec & iov = buf[done];
            if (iov.iov_len <= result)
            {
                result -= iov.iov_len;
                done++;
            }
            else
            {
                iov.iov_len -= result;
                iov.iov_base += result;
                break;
            }
        }
    }
};

struct blockstore_op_t;

struct osd_primary_op_data_t;

struct osd_op_t
{
    timespec tv_begin;
    uint64_t op_type = OSD_OP_IN;
    int peer_fd;
    osd_any_op_t req;
    osd_any_reply_t reply;
    blockstore_op_t *bs_op = NULL;
    void *buf = NULL;
    void *rmw_buf = NULL;
    osd_primary_op_data_t* op_data = NULL;
    std::function<void(osd_op_t*)> callback;

    osd_op_buf_list_t iov;

    ~osd_op_t();
};

struct osd_client_t
{
    sockaddr_in peer_addr;
    int peer_port;
    int peer_fd;
    int peer_state;
    int connect_timeout_id = -1;
    osd_num_t osd_num = 0;

    void *in_buf = NULL;

    // Read state
    int read_ready = 0;
    osd_op_t *read_op = NULL;
    iovec read_iov;
    msghdr read_msg;
    int read_remaining = 0;
    int read_state = 0;
    osd_op_buf_list_t recv_list;

    // Incoming operations
    std::vector<osd_op_t*> received_ops;

    // Outbound operations
    std::deque<osd_op_t*> outbox;
    std::map<int, osd_op_t*> sent_ops;

    // PGs dirtied by this client's primary-writes
    std::set<pool_pg_num_t> dirty_pgs;

    // Write state
    osd_op_t *write_op = NULL;
    msghdr write_msg;
    int write_state = 0;
    osd_op_buf_list_t send_list;
};

struct osd_wanted_peer_t
{
    json11::Json address_list;
    int port;
    time_t last_connect_attempt;
    bool connecting, address_changed;
    int address_index;
    std::string cur_addr;
    int cur_port;
};

struct osd_op_stats_t
{
    uint64_t op_stat_sum[OSD_OP_MAX+1] = { 0 };
    uint64_t op_stat_count[OSD_OP_MAX+1] = { 0 };
    uint64_t op_stat_bytes[OSD_OP_MAX+1] = { 0 };
    uint64_t subop_stat_sum[OSD_OP_MAX+1] = { 0 };
    uint64_t subop_stat_count[OSD_OP_MAX+1] = { 0 };
};

struct osd_messenger_t
{
    timerfd_manager_t *tfd;
    ring_loop_t *ringloop;

    // osd_num_t is only for logging and asserts
    osd_num_t osd_num;
    int receive_buffer_size = 9000;
    int peer_connect_interval = DEFAULT_PEER_CONNECT_INTERVAL;
    int peer_connect_timeout = DEFAULT_PEER_CONNECT_TIMEOUT;
    int log_level = 0;
    bool use_sync_send_recv = false;

    std::map<osd_num_t, osd_wanted_peer_t> wanted_peers;
    std::map<uint64_t, int> osd_peer_fds;
    uint64_t next_subop_id = 1;

    std::map<int, osd_client_t> clients;
    std::vector<int> read_ready_clients;
    std::vector<int> write_ready_clients;
    std::vector<std::function<void()>> set_immediate;

    // op statistics
    osd_op_stats_t stats;

public:
    void connect_peer(uint64_t osd_num, json11::Json peer_state);
    void stop_client(int peer_fd);
    void outbox_push(osd_op_t *cur_op);
    std::function<void(osd_op_t*)> exec_op;
    std::function<void(osd_num_t)> repeer_pgs;
    void handle_peer_epoll(int peer_fd, int epoll_events);
    void read_requests();
    void send_replies();
    void accept_connections(int listen_fd);
    ~osd_messenger_t();

protected:
    void try_connect_peer(uint64_t osd_num);
    void try_connect_peer_addr(osd_num_t peer_osd, const char *peer_host, int peer_port);
    void handle_connect_epoll(int peer_fd);
    void on_connect_peer(osd_num_t peer_osd, int peer_fd);
    void check_peer_config(osd_client_t & cl);
    void cancel_osd_ops(osd_client_t & cl);
    void cancel_op(osd_op_t *op);

    bool try_send(osd_client_t & cl);
    void handle_send(int result, int peer_fd);

    bool handle_read(int result, int peer_fd);
    bool handle_finished_read(osd_client_t & cl);
    void handle_op_hdr(osd_client_t *cl);
    bool handle_reply_hdr(osd_client_t *cl);
    void handle_reply_ready(osd_op_t *op);
};
