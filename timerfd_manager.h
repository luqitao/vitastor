#pragma once

#include <time.h>
#include "ringloop.h"

struct timerfd_timer_t
{
    int id;
    uint64_t millis;
    timespec start, next;
    bool repeat;
    std::function<void(int)> callback;
};

class timerfd_manager_t
{
    int wait_state = 0;
    int timerfd;
    int nearest = -1;
    int id = 1;
    std::vector<timerfd_timer_t> timers;
    ring_loop_t *ringloop;
    ring_consumer_t consumer;

    void inc_timer(timerfd_timer_t & t);
    void set_nearest();
    void trigger_nearest();
    void handle_readable();
    void set_wait();
    void loop();
public:
    // FIXME shouldn't be here
    std::function<void(int, std::function<void(int, int)>)> set_fd_handler;

    timerfd_manager_t(ring_loop_t *ringloop);
    ~timerfd_manager_t();
    int set_timer(uint64_t millis, bool repeat, std::function<void(int)> callback);
    void clear_timer(int timer_id);
};
