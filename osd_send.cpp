#include "osd.h"

void osd_t::outbox_push(osd_client_t & cl, osd_op_t *cur_op)
{
    assert(cur_op->peer_fd);
    if (cur_op->op_type == OSD_OP_OUT)
    {
        clock_gettime(CLOCK_REALTIME, &cur_op->tv_begin);
    }
    cl.outbox.push_back(cur_op);
    if (cl.write_op || cl.outbox.size() > 1 || !try_send(cl))
    {
        if (cl.write_state == 0)
        {
            cl.write_state = CL_WRITE_READY;
            write_ready_clients.push_back(cur_op->peer_fd);
        }
        ringloop->wakeup();
    }
}

bool osd_t::try_send(osd_client_t & cl)
{
    int peer_fd = cl.peer_fd;
    io_uring_sqe* sqe = ringloop->get_sqe();
    if (!sqe)
    {
        return false;
    }
    ring_data_t* data = ((ring_data_t*)sqe->user_data);
    if (!cl.write_op)
    {
        // pick next command
        cl.write_op = cl.outbox.front();
        cl.outbox.pop_front();
        cl.write_state = CL_WRITE_REPLY;
        clock_gettime(CLOCK_REALTIME, &cl.write_op->tv_send);
        if (cl.write_op->op_type == OSD_OP_IN)
        {
            // Measure execution latency
            timespec tv_end = cl.write_op->tv_send;
            op_stat_count[cl.write_op->req.hdr.opcode]++;
            op_stat_sum[cl.write_op->req.hdr.opcode] += (
                (tv_end.tv_sec - cl.write_op->tv_begin.tv_sec)*1000000 +
                (tv_end.tv_nsec - cl.write_op->tv_begin.tv_nsec)/1000
            );
        }
    }
    cl.write_msg.msg_iov = cl.write_op->send_list.get_iovec();
    cl.write_msg.msg_iovlen = cl.write_op->send_list.get_size();
    data->callback = [this, peer_fd](ring_data_t *data) { handle_send(data, peer_fd); };
    my_uring_prep_sendmsg(sqe, cl.peer_fd_index, &cl.write_msg, 0);
    sqe->flags |= IOSQE_FIXED_FILE;
    return true;
}

void osd_t::send_replies()
{
    for (int i = 0; i < write_ready_clients.size(); i++)
    {
        int peer_fd = write_ready_clients[i];
        if (!try_send(clients[peer_fd]))
        {
            write_ready_clients.erase(write_ready_clients.begin(), write_ready_clients.begin() + i);
            return;
        }
    }
    write_ready_clients.clear();
}

void osd_t::handle_send(ring_data_t *data, int peer_fd)
{
    int res = data->res;
    auto cl_it = clients.find(peer_fd);
    if (cl_it != clients.end())
    {
        auto & cl = cl_it->second;
        if (res < 0 && res != -EAGAIN)
        {
            // this is a client socket, so don't panic. just disconnect it
            printf("Client %d socket write error: %d (%s). Disconnecting client\n", peer_fd, -res, strerror(-res));
            stop_client(peer_fd);
            return;
        }
        if (res >= 0)
        {
            osd_op_t *cur_op = cl.write_op;
            while (res > 0 && cur_op->send_list.sent < cur_op->send_list.count)
            {
                iovec & iov = cur_op->send_list.buf[cur_op->send_list.sent];
                if (iov.iov_len <= res)
                {
                    res -= iov.iov_len;
                    cur_op->send_list.sent++;
                }
                else
                {
                    iov.iov_len -= res;
                    iov.iov_base += res;
                    break;
                }
            }
            if (cur_op->send_list.sent >= cur_op->send_list.count)
            {
                // Done
                if (cur_op->req.hdr.opcode == OSD_OP_SECONDARY_STABILIZE)
                {
                    timespec tv_end;
                    clock_gettime(CLOCK_REALTIME, &tv_end);
                    send_stat_count++;
                    send_stat_sum += (
                        (tv_end.tv_sec - cl.write_op->tv_send.tv_sec)*1000000 +
                        (tv_end.tv_nsec - cl.write_op->tv_send.tv_nsec)/1000
                    );
                }
                if (cur_op->op_type == OSD_OP_IN)
                {
                    delete cur_op;
                }
                else
                {
                    cl.sent_ops[cl.write_op->req.hdr.id] = cl.write_op;
                }
                cl.write_op = NULL;
                cl.write_state = cl.outbox.size() > 0 ? CL_WRITE_READY : 0;
            }
        }
        if (cl.write_state != 0)
        {
            write_ready_clients.push_back(peer_fd);
        }
    }
}
