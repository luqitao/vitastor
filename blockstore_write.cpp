#include "blockstore_impl.h"

bool blockstore_impl_t::enqueue_write(blockstore_op_t *op)
{
    // Check or assign version number
    bool found = false, deleted = false, is_del = (op->opcode == BS_OP_DELETE);
    uint64_t version = 1;
    if (dirty_db.size() > 0)
    {
        auto dirty_it = dirty_db.upper_bound((obj_ver_id){
            .oid = op->oid,
            .version = UINT64_MAX,
        });
        dirty_it--; // segfaults when dirty_db is empty
        if (dirty_it != dirty_db.end() && dirty_it->first.oid == op->oid)
        {
            found = true;
            version = dirty_it->first.version + 1;
            deleted = IS_DELETE(dirty_it->second.state);
        }
    }
    if (!found)
    {
        auto clean_it = clean_db.find(op->oid);
        if (clean_it != clean_db.end())
        {
            version = clean_it->second.version + 1;
        }
        else
        {
            deleted = true;
        }
    }
    if (op->version == 0)
    {
        op->version = version;
    }
    else if (op->version < version)
    {
        // Invalid version requested
        op->retval = -EINVAL;
        return false;
    }
    if (deleted && is_del)
    {
        // Already deleted
        op->retval = 0;
        return false;
    }
    // Immediately add the operation into dirty_db, so subsequent reads could see it
#ifdef BLOCKSTORE_DEBUG
    printf("%s %lu:%lu v%lu\n", is_del ? "Delete" : "Write", op->oid.inode, op->oid.stripe, op->version);
#endif
    dirty_db.emplace((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    }, (dirty_entry){
        .state = (uint32_t)(
            is_del
                ? ST_DEL_IN_FLIGHT
                : (op->len == block_size || deleted ? ST_D_IN_FLIGHT : ST_J_IN_FLIGHT)
        ),
        .flags = 0,
        .location = 0,
        .offset = is_del ? 0 : op->offset,
        .len = is_del ? 0 : op->len,
        .journal_sector = 0,
    });
    return true;
}

// First step of the write algorithm: dequeue operation and submit initial write(s)
int blockstore_impl_t::dequeue_write(blockstore_op_t *op)
{
    auto dirty_it = dirty_db.find((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    });
    if (dirty_it->second.state == ST_D_IN_FLIGHT)
    {
        blockstore_journal_check_t space_check(this);
        if (!space_check.check_available(op, unsynced_big_writes.size() + 1, sizeof(journal_entry_big_write), JOURNAL_STABILIZE_RESERVATION))
        {
            return 0;
        }
        // Big (redirect) write
        uint64_t loc = data_alloc->find_free();
        if (loc == UINT64_MAX)
        {
            // no space
            if (flusher->is_active())
            {
                // hope that some space will be available after flush
                PRIV(op)->wait_for = WAIT_FREE;
                return 0;
            }
            op->retval = -ENOSPC;
            FINISH_OP(op);
            return 1;
        }
        BS_SUBMIT_GET_SQE(sqe, data);
        dirty_it->second.location = loc << block_order;
        dirty_it->second.state = ST_D_SUBMITTED;
#ifdef BLOCKSTORE_DEBUG
        printf("Allocate block %lu\n", loc);
#endif
        data_alloc->set(loc, true);
        uint64_t stripe_offset = (op->offset % bitmap_granularity);
        uint64_t stripe_end = (op->offset + op->len) % bitmap_granularity;
        // Zero fill up to bitmap_granularity
        int vcnt = 0;
        if (stripe_offset)
        {
            PRIV(op)->iov_zerofill[vcnt++] = (struct iovec){ zero_object, stripe_offset };
        }
        PRIV(op)->iov_zerofill[vcnt++] = (struct iovec){ op->buf, op->len };
        if (stripe_end)
        {
            stripe_end = bitmap_granularity - stripe_end;
            PRIV(op)->iov_zerofill[vcnt++] = (struct iovec){ zero_object, stripe_end };
        }
        data->iov.iov_len = op->len + stripe_offset + stripe_end; // to check it in the callback
        data->callback = [this, op](ring_data_t *data) { handle_write_event(data, op); };
        my_uring_prep_writev(
            sqe, data_fd_index, PRIV(op)->iov_zerofill, vcnt, data_offset + (loc << block_order) + op->offset - stripe_offset
        );
        sqe->flags |= IOSQE_FIXED_FILE;
        PRIV(op)->pending_ops = 1;
        PRIV(op)->min_used_journal_sector = PRIV(op)->max_used_journal_sector = 0;
        // Remember big write as unsynced
        unsynced_big_writes.push_back((obj_ver_id){
            .oid = op->oid,
            .version = op->version,
        });
    }
    else
    {
        // Small (journaled) write
        // First check if the journal has sufficient space
        blockstore_journal_check_t space_check(this);
        if (unsynced_big_writes.size() && !space_check.check_available(op, unsynced_big_writes.size(), sizeof(journal_entry_big_write), 0)
            || !space_check.check_available(op, 1, sizeof(journal_entry_small_write), op->len + JOURNAL_STABILIZE_RESERVATION))
        {
            return 0;
        }
        // There is sufficient space. Get SQE(s)
        struct io_uring_sqe *sqe1 = NULL;
        if ((journal_block_size - journal.in_sector_pos) < sizeof(journal_entry_small_write) &&
            journal.sector_info[journal.cur_sector].dirty)
        {
            // Write current journal sector only if it's dirty and full
            BS_SUBMIT_GET_SQE_DECL(sqe1);
        }
        struct io_uring_sqe *sqe2 = NULL;
        if (op->len > 0)
        {
            BS_SUBMIT_GET_SQE_DECL(sqe2);
        }
        // Got SQEs. Prepare previous journal sector write if required
        auto cb = [this, op](ring_data_t *data) { handle_write_event(data, op); };
        if (sqe1)
        {
            prepare_journal_sector_write(journal, journal.cur_sector, sqe1, cb);
            // FIXME rename to min/max _flushing
            PRIV(op)->min_used_journal_sector = PRIV(op)->max_used_journal_sector = 1 + journal.cur_sector;
            PRIV(op)->pending_ops++;
        }
        else
        {
            PRIV(op)->min_used_journal_sector = PRIV(op)->max_used_journal_sector = 0;
        }
        // Then pre-fill journal entry
        journal_entry_small_write *je = (journal_entry_small_write*)
            prefill_single_journal_entry(journal, JE_SMALL_WRITE, sizeof(journal_entry_small_write));
        dirty_it->second.journal_sector = journal.sector_info[journal.cur_sector].offset;
        journal.used_sectors[journal.sector_info[journal.cur_sector].offset]++;
#ifdef BLOCKSTORE_DEBUG
        printf("journal offset %lu is used by %lu:%lu v%lu\n", dirty_it->second.journal_sector, dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version);
#endif
        // Figure out where data will be
        journal.next_free = (journal.next_free + op->len) <= journal.len ? journal.next_free : journal_block_size;
        je->oid = op->oid;
        je->version = op->version;
        je->offset = op->offset;
        je->len = op->len;
        je->data_offset = journal.next_free;
        je->crc32_data = crc32c(0, op->buf, op->len);
        je->crc32 = je_crc32((journal_entry*)je);
        journal.crc32_last = je->crc32;
        if (op->len > 0)
        {
            // Prepare journal data write
            if (journal.inmemory)
            {
                // Copy data
                memcpy(journal.buffer + journal.next_free, op->buf, op->len);
            }
            ring_data_t *data2 = ((ring_data_t*)sqe2->user_data);
            data2->iov = (struct iovec){ op->buf, op->len };
            data2->callback = cb;
            my_uring_prep_writev(
                sqe2, journal_fd_index, &data2->iov, 1, journal.offset + journal.next_free
            );
            sqe2->flags |= IOSQE_FIXED_FILE;
            PRIV(op)->pending_ops++;
        }
        else
        {
            // Zero-length overwrite. Allowed to bump object version in EC placement groups without actually writing data
        }
        dirty_it->second.location = journal.next_free;
        dirty_it->second.state = ST_J_SUBMITTED;
        journal.next_free += op->len;
        if (journal.next_free >= journal.len)
        {
            journal.next_free = journal_block_size;
        }
        // Remember small write as unsynced
        unsynced_small_writes.push_back((obj_ver_id){
            .oid = op->oid,
            .version = op->version,
        });
        if (!PRIV(op)->pending_ops)
        {
            ack_write(op);
        }
    }
    return 1;
}

void blockstore_impl_t::handle_write_event(ring_data_t *data, blockstore_op_t *op)
{
    live = true;
    if (data->res != data->iov.iov_len)
    {
        // FIXME: our state becomes corrupted after a write error. maybe do something better than just die
        throw std::runtime_error(
            "write operation failed ("+std::to_string(data->res)+" != "+std::to_string(data->iov.iov_len)+
            "). in-memory state is corrupted. AAAAAAAaaaaaaaaa!!!111"
        );
    }
    PRIV(op)->pending_ops--;
    if (PRIV(op)->pending_ops == 0)
    {
        release_journal_sectors(op);
        ack_write(op);
    }
}

void blockstore_impl_t::release_journal_sectors(blockstore_op_t *op)
{
    // Release used journal sectors
    if (PRIV(op)->min_used_journal_sector > 0 &&
        PRIV(op)->max_used_journal_sector > 0)
    {
        uint64_t s = PRIV(op)->min_used_journal_sector;
        while (1)
        {
            journal.sector_info[s-1].usage_count--;
            if (s == PRIV(op)->max_used_journal_sector)
                break;
            s = 1 + s % journal.sector_count;
        }
        PRIV(op)->min_used_journal_sector = PRIV(op)->max_used_journal_sector = 0;
    }
}

void blockstore_impl_t::ack_write(blockstore_op_t *op)
{
    // Switch object state
    auto & dirty_entry = dirty_db[(obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    }];
#ifdef BLOCKSTORE_DEBUG
    printf("Ack write %lu:%lu v%lu = %d\n", op->oid.inode, op->oid.stripe, op->version, dirty_entry.state);
#endif
    if (dirty_entry.state == ST_J_SUBMITTED)
    {
        dirty_entry.state = ST_J_WRITTEN;
    }
    else if (dirty_entry.state == ST_D_SUBMITTED)
    {
        dirty_entry.state = ST_D_WRITTEN;
    }
    else if (dirty_entry.state == ST_DEL_SUBMITTED)
    {
        dirty_entry.state = ST_DEL_WRITTEN;
    }
    // Acknowledge write without sync
    op->retval = op->len;
    FINISH_OP(op);
}

int blockstore_impl_t::dequeue_del(blockstore_op_t *op)
{
    auto dirty_it = dirty_db.find((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    });
    blockstore_journal_check_t space_check(this);
    if (!space_check.check_available(op, 1, sizeof(journal_entry_del), 0))
    {
        return 0;
    }
    BS_SUBMIT_GET_ONLY_SQE(sqe);
    // Prepare journal sector write
    journal_entry_del *je = (journal_entry_del*)
        prefill_single_journal_entry(journal, JE_DELETE, sizeof(struct journal_entry_del));
    dirty_it->second.journal_sector = journal.sector_info[journal.cur_sector].offset;
    journal.used_sectors[journal.sector_info[journal.cur_sector].offset]++;
#ifdef BLOCKSTORE_DEBUG
    printf("journal offset %lu is used by %lu:%lu v%lu\n", dirty_it->second.journal_sector, dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version);
#endif
    je->oid = op->oid;
    je->version = op->version;
    je->crc32 = je_crc32((journal_entry*)je);
    journal.crc32_last = je->crc32;
    auto cb = [this, op](ring_data_t *data) { handle_write_event(data, op); };
    prepare_journal_sector_write(journal, journal.cur_sector, sqe, cb);
    PRIV(op)->min_used_journal_sector = PRIV(op)->max_used_journal_sector = 1 + journal.cur_sector;
    PRIV(op)->pending_ops = 1;
    dirty_it->second.state = ST_DEL_SUBMITTED;
    // Remember small write as unsynced
    unsynced_small_writes.push_back((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    });
    return 1;
}
