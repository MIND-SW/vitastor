// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <stdexcept>
#include <sys/poll.h>

#include "messenger.h"
#include "msgr_iothread.h"

msgr_iothread_t::msgr_iothread_t():
    ring(RINGLOOP_DEFAULT_SIZE, true),
    thread(&msgr_iothread_t::run, this)
{
    eventfd = ring.register_eventfd();
    if (eventfd < 0)
    {
        throw std::runtime_error(std::string("failed to register eventfd: ") + strerror(-eventfd));
    }
}

msgr_iothread_t::~msgr_iothread_t()
{
    stop();
}

void msgr_iothread_t::add_sqe(io_uring_sqe & sqe)
{
    mu.lock();
    queue.push_back((iothread_sqe_t){ .sqe = sqe, .data = std::move(*(ring_data_t*)sqe.user_data) });
    if (queue.size() == 1)
    {
        cond.notify_all();
    }
    mu.unlock();
}

void msgr_iothread_t::stop()
{
    mu.lock();
    if (stopped)
    {
        mu.unlock();
        return;
    }
    stopped = true;
    if (outer_loop_data)
    {
        outer_loop_data->callback = [](ring_data_t*){};
    }
    cond.notify_all();
    close(eventfd);
    mu.unlock();
    thread.join();
}

void msgr_iothread_t::add_to_ringloop(ring_loop_i *outer_loop)
{
    assert(!this->outer_loop || this->outer_loop == outer_loop);
    io_uring_sqe *sqe = outer_loop->get_sqe();
    assert(sqe != NULL);
    this->outer_loop = outer_loop;
    this->outer_loop_data = ((ring_data_t*)sqe->user_data);
    io_uring_prep_poll_add(sqe, eventfd, POLLIN);
    outer_loop_data->callback = [this](ring_data_t *data)
    {
        if (data->res < 0)
        {
            throw std::runtime_error(std::string("eventfd poll failed: ") + strerror(-data->res));
        }
        outer_loop_data = NULL;
        if (stopped)
        {
            return;
        }
        add_to_ringloop(this->outer_loop);
        ring.loop();
    };
}

void msgr_iothread_t::run()
{
    while (true)
    {
        {
            std::unique_lock<std::mutex> lk(mu);
            while (!stopped && !queue.size())
                cond.wait(lk);
            if (stopped)
                return;
            int i = 0;
            for (; i < queue.size(); i++)
            {
                io_uring_sqe *sqe = ring.get_sqe();
                if (!sqe)
                    break;
                ring_data_t *data = ((ring_data_t*)sqe->user_data);
                *data = std::move(queue[i].data);
                *sqe = queue[i].sqe;
                sqe->user_data = (uint64_t)data;
            }
            queue.erase(queue.begin(), queue.begin()+i);
        }
        // We only want to offload sendmsg/recvmsg. Callbacks will be called in main thread
        ring.submit();
    }
}

void osd_messenger_t::init_iothreads()
{
    for (int i = 0; i < iothread_count; i++)
    {
        auto iot = new msgr_iothread_t();
        iothreads.push_back(iot);
        iot->add_to_ringloop(ringloop);
    }
}

void osd_messenger_t::destroy_iothreads()
{
    if (iothreads.size())
    {
        for (auto iot: iothreads)
        {
            delete iot;
        }
        iothreads.clear();
    }
}
