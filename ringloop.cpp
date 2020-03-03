#include <set>

#include "ringloop.h"

ring_loop_t::ring_loop_t(int qd)
{
    io_uring_params params = { 0 };
    params.flags = IORING_SETUP_SQPOLL;
    params.sq_thread_idle = 10;
    int ret = io_uring_queue_init_params(qd, &ring, &params);
    if (ret < 0)
    {
        throw std::runtime_error(std::string("io_uring_queue_init: ") + strerror(-ret));
    }
    ring_data_total = free_ring_data_ptr = *ring.cq.kring_entries;
    ring_datas = (ring_data_t*)malloc(sizeof(ring_data_t) * free_ring_data_ptr);
    free_ring_data = (int*)malloc(sizeof(int) * free_ring_data_ptr);
    if (!ring_datas || !free_ring_data)
    {
        throw std::bad_alloc();
    }
    for (int i = 0; i < free_ring_data_ptr; i++)
    {
        ring_datas[i] = { 0 };
        free_ring_data[i] = i;
    }
}

ring_loop_t::~ring_loop_t()
{
    free(free_ring_data);
    free(ring_datas);
    io_uring_queue_exit(&ring);
}

void ring_loop_t::drain_events(void *completions_ptr)
{
    std::set<ring_data_t*> & completions = *((std::set<ring_data_t*> *)completions_ptr);
    if (free_ring_data_ptr < ring_data_total)
    {
        // Try to cancel requests that are allowed to be canceled by the caller (epoll, timerfd and similar)
        for (int i = 0; i < ring_data_total; i++)
        {
            if (ring_datas[i].allow_cancel)
            {
                // allow_cancel may only be true while the operation is inflight
                io_uring_sqe *sqe = get_sqe();
                if (!sqe)
                {
                    throw std::runtime_error("can't get SQE to cancel operation");
                }
                ring_data_t *data = (ring_data_t*)sqe->user_data;
                data->callback = NULL;
                ring_datas[i].res = -ECANCELED;
                my_uring_prep_cancel(sqe, &ring_datas[i], 0);
                // It seems (FIXME) cancel operations don't always get completions
                completions.insert(data);
            }
        }
        if (completions.size() > 0)
        {
            submit();
        }
    }
    int inflight = ring_data_total - free_ring_data_ptr;
    while (completions.size() < inflight)
    {
        io_uring_cqe *cqe;
        while (!io_uring_peek_cqe(&ring, &cqe))
        {
            ring_data_t *d = (ring_data_t*)cqe->user_data;
            d->res = cqe->res;
            d->allow_cancel = false;
            completions.insert(d);
            io_uring_cqe_seen(&ring, cqe);
        }
        if (completions.size() < inflight)
        {
            wait();
        }
    }
}

void ring_loop_t::run_completions(void *completions_ptr)
{
    std::set<ring_data_t*> & completions = *((std::set<ring_data_t*> *)completions_ptr);
    // Call event callbacks
    for (ring_data_t *d: completions)
    {
        free_ring_data[free_ring_data_ptr++] = d - ring_datas;
        if (d->callback)
            d->callback(d);
    }
}

int ring_loop_t::register_fd(int fd)
{
    std::set<ring_data_t*> completions;
    drain_events((void*)&completions);
    // Modify registered files
    int idx = reg_fds.size();
    reg_fds.push_back(fd);
    if (registered)
    {
        io_uring_unregister_files(&ring);
    }
    int ret = io_uring_register_files(&ring, reg_fds.data(), reg_fds.size());
    if (ret != 0)
    {
        throw std::runtime_error(std::string("io_uring_register_files_update: ") + strerror(-ret));
    }
    registered = 1;
    run_completions((void*)&completions);
    return idx;
}

void ring_loop_t::unregister_fd(int fd_index)
{
    std::set<ring_data_t*> completions;
    drain_events((void*)&completions);
    // Modify registered files
    reg_fds.erase(reg_fds.begin()+fd_index, reg_fds.begin()+fd_index+1);
    if (registered)
    {
        io_uring_unregister_files(&ring);
    }
    int ret = io_uring_register_files(&ring, reg_fds.data(), reg_fds.size());
    if (ret != 0)
    {
        throw std::runtime_error(std::string("io_uring_register_files_update: ") + strerror(-ret));
    }
    run_completions((void*)&completions);
}

int ring_loop_t::register_consumer(ring_consumer_t & consumer)
{
    consumer.number = consumers.size();
    consumers.push_back(consumer);
    return consumer.number;
}

void ring_loop_t::wakeup()
{
    loop_again = true;
}

void ring_loop_t::unregister_consumer(ring_consumer_t & consumer)
{
    if (consumer.number >= 0 && consumer.number < consumers.size())
    {
        consumers[consumer.number].loop = NULL;
        consumer.number = -1;
    }
}

void ring_loop_t::loop()
{
    io_uring_cqe *cqe;
    while (!io_uring_peek_cqe(&ring, &cqe))
    {
        ring_data_t *d = (ring_data_t*)cqe->user_data;
        d->res = cqe->res;
        d->allow_cancel = false;
        io_uring_cqe_seen(&ring, cqe);
        free_ring_data[free_ring_data_ptr++] = d - ring_datas;
        if (d->callback)
            d->callback(d);
    }
    do
    {
        loop_again = false;
        for (int i = 0; i < consumers.size(); i++)
        {
            consumers[i].loop();
        }
    } while (loop_again);
}

unsigned ring_loop_t::save()
{
    return ring.sq.sqe_tail;
}

void ring_loop_t::restore(unsigned sqe_tail)
{
    assert(ring.sq.sqe_tail >= sqe_tail);
    for (unsigned i = sqe_tail; i < ring.sq.sqe_tail; i++)
    {
        free_ring_data[free_ring_data_ptr++] = ((ring_data_t*)ring.sq.sqes[i & *ring.sq.kring_mask].user_data) - ring_datas;
    }
    ring.sq.sqe_tail = sqe_tail;
}
