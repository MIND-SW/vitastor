// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <mutex>
#include <condition_variable>
#include <thread>

#include "ringloop.h"

struct iothread_sqe_t
{
    io_uring_sqe sqe;
    ring_data_t data;
};

class msgr_iothread_t
{
protected:
    ring_loop_t ring;
    ring_loop_i *outer_loop = NULL;
    ring_data_t *outer_loop_data = NULL;
    int eventfd = -1;
    bool stopped = false;
    std::mutex mu;
    std::condition_variable cond;
    std::vector<iothread_sqe_t> queue;
    std::thread thread;

    void run();
public:

    msgr_iothread_t();
    ~msgr_iothread_t();

    void add_sqe(io_uring_sqe & sqe);
    void stop();
    void add_to_ringloop(ring_loop_i *outer_loop);
};
