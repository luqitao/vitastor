#pragma once

#include "ringloop.h"

class timerfd_interval
{
    int wait_state;
    int timerfd, timerfd_index;
    int status;
    ring_loop_t *ringloop;
    ring_consumer_t consumer;
    std::function<void(void)> callback;
public:
    timerfd_interval(ring_loop_t *ringloop, int seconds, std::function<void(void)> cb);
    ~timerfd_interval();
    void loop();
};
