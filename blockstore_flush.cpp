#include "blockstore_impl.h"

journal_flusher_t::journal_flusher_t(int flusher_count, blockstore_impl_t *bs)
{
    this->bs = bs;
    this->flusher_count = flusher_count;
    active_flushers = 0;
    sync_threshold = flusher_count == 1 ? 1 : flusher_count/2;
    journal_trim_interval = sync_threshold;
    journal_trim_counter = 0;
    journal_superblock = bs->journal.inmemory ? bs->journal.buffer : memalign(MEM_ALIGNMENT, bs->journal_block_size);
    co = new journal_flusher_co[flusher_count];
    for (int i = 0; i < flusher_count; i++)
    {
        co[i].bs = bs;
        co[i].flusher = this;
    }
}

journal_flusher_co::journal_flusher_co()
{
    wait_state = 0;
    simple_callback_r = [this](ring_data_t* data)
    {
        bs->live = true;
        if (data->res != data->iov.iov_len)
        {
            throw std::runtime_error(
                "data read operation failed during flush ("+std::to_string(data->res)+" != "+std::to_string(data->iov.iov_len)+
                "). can't continue, sorry :-("
            );
        }
        wait_count--;
    };
    simple_callback_w = [this](ring_data_t* data)
    {
        bs->live = true;
        if (data->res != data->iov.iov_len)
        {
            throw std::runtime_error(
                "write operation failed ("+std::to_string(data->res)+" != "+std::to_string(data->iov.iov_len)+
                "). state "+std::to_string(wait_state)+". in-memory state is corrupted. AAAAAAAaaaaaaaaa!!!111"
            );
        }
        wait_count--;
    };
}

journal_flusher_t::~journal_flusher_t()
{
    if (!bs->journal.inmemory)
        free(journal_superblock);
    delete[] co;
}

bool journal_flusher_t::is_active()
{
    return active_flushers > 0 || start_forced && flush_queue.size() > 0 || flush_queue.size() >= sync_threshold;
}

void journal_flusher_t::loop()
{
    for (int i = 0; i < flusher_count; i++)
    {
        if (!active_flushers && (start_forced ? !flush_queue.size() : (flush_queue.size() < sync_threshold)))
        {
            return;
        }
        co[i].loop();
    }
}

void journal_flusher_t::enqueue_flush(obj_ver_id ov)
{
    auto it = flush_versions.find(ov.oid);
    if (it != flush_versions.end())
    {
        if (it->second < ov.version)
            it->second = ov.version;
    }
    else
    {
        flush_versions[ov.oid] = ov.version;
        flush_queue.push_back(ov.oid);
    }
}

void journal_flusher_t::unshift_flush(obj_ver_id ov)
{
    auto it = flush_versions.find(ov.oid);
    if (it != flush_versions.end())
    {
        if (it->second < ov.version)
            it->second = ov.version;
    }
    else
    {
        flush_versions[ov.oid] = ov.version;
        flush_queue.push_front(ov.oid);
    }
}

void journal_flusher_t::force_start()
{
    start_forced = true;
    bs->ringloop->wakeup();
}

#define await_sqe(label) \
    resume_##label:\
        sqe = bs->get_sqe();\
        if (!sqe)\
        {\
            wait_state = label;\
            return false;\
        }\
        data = ((ring_data_t*)sqe->user_data);

bool journal_flusher_co::loop()
{
    // This is much better than implementing the whole function as an FSM
    // Maybe I should consider a coroutine library like https://github.com/hnes/libaco ...
    if (wait_state == 1)
        goto resume_1;
    else if (wait_state == 2)
        goto resume_2;
    else if (wait_state == 3)
        goto resume_3;
    else if (wait_state == 4)
        goto resume_4;
    else if (wait_state == 5)
        goto resume_5;
    else if (wait_state == 6)
        goto resume_6;
    else if (wait_state == 7)
        goto resume_7;
    else if (wait_state == 8)
        goto resume_8;
    else if (wait_state == 9)
        goto resume_9;
    else if (wait_state == 10)
        goto resume_10;
    else if (wait_state == 12)
        goto resume_12;
    else if (wait_state == 13)
        goto resume_13;
    else if (wait_state == 14)
        goto resume_14;
    else if (wait_state == 15)
        goto resume_15;
    else if (wait_state == 16)
        goto resume_16;
    else if (wait_state == 17)
        goto resume_17;
    else if (wait_state == 18)
        goto resume_18;
resume_0:
    if (!flusher->flush_queue.size() ||
        !flusher->start_forced && !flusher->active_flushers && flusher->flush_queue.size() < flusher->sync_threshold)
    {
        flusher->start_forced = false;
        wait_state = 0;
        return true;
    }
    cur.oid = flusher->flush_queue.front();
    cur.version = flusher->flush_versions[cur.oid];
    flusher->flush_queue.pop_front();
    flusher->flush_versions.erase(cur.oid);
    dirty_end = bs->dirty_db.find(cur);
    if (dirty_end != bs->dirty_db.end())
    {
        repeat_it = flusher->sync_to_repeat.find(cur.oid);
        if (repeat_it != flusher->sync_to_repeat.end())
        {
#ifdef BLOCKSTORE_DEBUG
            printf("Postpone %lu:%lu v%lu\n", cur.oid.inode, cur.oid.stripe, cur.version);
#endif
            // We don't flush different parts of history of the same object in parallel
            // So we check if someone is already flushing this object
            // In that case we set sync_to_repeat and pick another object
            // Another coroutine will see it and re-queue the object after it finishes
            if (repeat_it->second < cur.version)
                repeat_it->second = cur.version;
            wait_state = 0;
            goto resume_0;
        }
        else
            flusher->sync_to_repeat[cur.oid] = 0;
#ifdef BLOCKSTORE_DEBUG
        printf("Flushing %lu:%lu v%lu\n", cur.oid.inode, cur.oid.stripe, cur.version);
#endif
        flusher->active_flushers++;
resume_1:
        // Scan dirty versions of the object
        if (!scan_dirty(1))
        {
            wait_state += 1;
            return false;
        }
        if (copy_count == 0 && clean_loc == UINT64_MAX && !has_delete && !has_empty)
        {
            // Nothing to flush
            flusher->active_flushers--;
            repeat_it = flusher->sync_to_repeat.find(cur.oid);
            if (repeat_it != flusher->sync_to_repeat.end() && repeat_it->second > cur.version)
            {
                // Requeue version
                flusher->unshift_flush({ .oid = cur.oid, .version = repeat_it->second });
            }
            flusher->sync_to_repeat.erase(repeat_it);
            wait_state = 0;
            goto resume_0;
        }
        // Find it in clean_db
        clean_it = bs->clean_db.find(cur.oid);
        old_clean_loc = (clean_it != bs->clean_db.end() ? clean_it->second.location : UINT64_MAX);
        if (clean_loc == UINT64_MAX)
        {
            if (copy_count > 0 && has_delete || old_clean_loc == UINT64_MAX)
            {
                // Object not allocated. This is a bug.
                char err[1024];
                snprintf(
                    err, 1024, "BUG: Object %lu:%lu v%lu that we are trying to flush is not allocated on the data device",
                    cur.oid.inode, cur.oid.stripe, cur.version
                );
                throw std::runtime_error(err);
            }
            else
            {
                clean_loc = old_clean_loc;
            }
        }
        // Also we need to submit metadata read(s). We do read-modify-write cycle(s) for every operation.
    resume_2:
        if (!modify_meta_read(clean_loc, meta_new, 2))
        {
            wait_state += 2;
            return false;
        }
        if (old_clean_loc != UINT64_MAX && old_clean_loc != clean_loc)
        {
        resume_14:
            if (!modify_meta_read(old_clean_loc, meta_old, 14))
            {
                wait_state += 14;
                return false;
            }
        }
        else
            meta_old.submitted = false;
    resume_3:
        if (wait_count > 0)
        {
            wait_state = 3;
            return false;
        }
        if (meta_new.submitted)
        {
            meta_new.it->second.state = 1;
            bs->ringloop->wakeup();
        }
        if (meta_old.submitted)
        {
            meta_old.it->second.state = 1;
            bs->ringloop->wakeup();
        }
        // Reads completed, submit writes and set bitmap bits
        if (bs->clean_entry_bitmap_size)
        {
            new_clean_bitmap = (bs->inmemory_meta
                ? meta_new.buf + meta_new.pos*bs->clean_entry_size + sizeof(clean_disk_entry)
                : bs->clean_bitmap + (clean_loc >> bs->block_order)*bs->clean_entry_bitmap_size);
            if (clean_init_bitmap)
            {
                memset(new_clean_bitmap, 0, bs->clean_entry_bitmap_size);
                bitmap_set(new_clean_bitmap, clean_bitmap_offset, clean_bitmap_len);
            }
        }
        for (it = v.begin(); it != v.end(); it++)
        {
            if (new_clean_bitmap)
            {
                bitmap_set(new_clean_bitmap, it->offset, it->len);
            }
            await_sqe(4);
            data->iov = (struct iovec){ it->buf, (size_t)it->len };
            data->callback = simple_callback_w;
            my_uring_prep_writev(
                sqe, bs->data_fd_index, &data->iov, 1, bs->data_offset + clean_loc + it->offset
            );
            sqe->flags |= IOSQE_FIXED_FILE;
            wait_count++;
        }
        // Sync data before writing metadata
    resume_16:
    resume_17:
    resume_18:
        if (copy_count && !fsync_batch(false, 16))
        {
            wait_state += 16;
            return false;
        }
    resume_5:
        // And metadata writes, but only after data writes complete
        if (!bs->inmemory_meta && meta_new.it->second.state == 0 || wait_count > 0)
        {
            // metadata sector is still being read or data is still being written, wait for it
            wait_state = 5;
            return false;
        }
        if (old_clean_loc != UINT64_MAX && old_clean_loc != clean_loc)
        {
            if (!bs->inmemory_meta && meta_old.it->second.state == 0)
            {
                wait_state = 5;
                return false;
            }
            memset(meta_old.buf + meta_old.pos*bs->clean_entry_size, 0, bs->clean_entry_size);
            await_sqe(15);
            data->iov = (struct iovec){ meta_old.buf, bs->meta_block_size };
            data->callback = simple_callback_w;
            my_uring_prep_writev(
                sqe, bs->meta_fd_index, &data->iov, 1, bs->meta_offset + meta_old.sector
            );
            sqe->flags |= IOSQE_FIXED_FILE;
            wait_count++;
        }
        if (has_delete)
        {
            memset(meta_new.buf + meta_new.pos*bs->clean_entry_size, 0, bs->clean_entry_size);
        }
        else
        {
            clean_disk_entry *new_entry = (clean_disk_entry*)(meta_new.buf + meta_new.pos*bs->clean_entry_size);
            new_entry->oid = cur.oid;
            new_entry->version = cur.version;
            if (!bs->inmemory_meta)
            {
                memcpy(&new_entry->bitmap, new_clean_bitmap, bs->clean_entry_bitmap_size);
            }
        }
        await_sqe(6);
        data->iov = (struct iovec){ meta_new.buf, bs->meta_block_size };
        data->callback = simple_callback_w;
        my_uring_prep_writev(
            sqe, bs->meta_fd_index, &data->iov, 1, bs->meta_offset + meta_new.sector
        );
        sqe->flags |= IOSQE_FIXED_FILE;
        wait_count++;
    resume_7:
        if (wait_count > 0)
        {
            wait_state = 7;
            return false;
        }
        // Done, free all buffers
        if (!bs->inmemory_meta)
        {
            meta_new.it->second.usage_count--;
            if (meta_new.it->second.usage_count == 0)
            {
                free(meta_new.it->second.buf);
                flusher->meta_sectors.erase(meta_new.it);
            }
            if (old_clean_loc != UINT64_MAX && old_clean_loc != clean_loc)
            {
                meta_old.it->second.usage_count--;
                if (meta_old.it->second.usage_count == 0)
                {
                    free(meta_old.it->second.buf);
                    flusher->meta_sectors.erase(meta_old.it);
                }
            }
        }
        for (it = v.begin(); it != v.end(); it++)
        {
            free(it->buf);
        }
        v.clear();
        // And sync metadata (in batches - not per each operation!)
    resume_8:
    resume_9:
    resume_10:
        if (!fsync_batch(true, 8))
        {
            wait_state += 8;
            return false;
        }
        // Update clean_db and dirty_db, free old data locations
        update_clean_db();
        // Clear unused part of the journal every <journal_trim_interval> flushes
        if (!((++flusher->journal_trim_counter) % flusher->journal_trim_interval))
        {
            flusher->journal_trim_counter = 0;
            if (bs->journal.trim())
            {
                // Update journal "superblock"
                await_sqe(12);
                *((journal_entry_start*)flusher->journal_superblock) = {
                    .crc32 = 0,
                    .magic = JOURNAL_MAGIC,
                    .type = JE_START,
                    .size = sizeof(journal_entry_start),
                    .reserved = 0,
                    .journal_start = bs->journal.used_start,
                };
                ((journal_entry_start*)flusher->journal_superblock)->crc32 = je_crc32((journal_entry*)flusher->journal_superblock);
                data->iov = (struct iovec){ flusher->journal_superblock, bs->journal_block_size };
                data->callback = simple_callback_w;
                my_uring_prep_writev(sqe, bs->journal_fd_index, &data->iov, 1, bs->journal.offset);
                sqe->flags |= IOSQE_FIXED_FILE;
                wait_count++;
            resume_13:
                if (wait_count > 0)
                {
                    wait_state = 13;
                    return false;
                }
            }
        }
        // All done
#ifdef BLOCKSTORE_DEBUG
        printf("Flushed %lu:%lu v%lu\n", cur.oid.inode, cur.oid.stripe, cur.version);
#endif
        flusher->active_flushers--;
        repeat_it = flusher->sync_to_repeat.find(cur.oid);
        if (repeat_it != flusher->sync_to_repeat.end() && repeat_it->second > cur.version)
        {
            // Requeue version
            flusher->unshift_flush({ .oid = cur.oid, .version = repeat_it->second });
        }
        flusher->sync_to_repeat.erase(repeat_it);
        wait_state = 0;
        goto resume_0;
    }
    return true;
}

bool journal_flusher_co::scan_dirty(int wait_base)
{
    if (wait_state == wait_base)
    {
        goto resume_0;
    }
    dirty_it = dirty_start = dirty_end;
    v.clear();
    wait_count = 0;
    copy_count = 0;
    clean_loc = UINT64_MAX;
    has_delete = false;
    has_empty = false;
    skip_copy = false;
    clean_init_bitmap = false;
    while (1)
    {
        if (dirty_it->second.state == ST_J_STABLE && !skip_copy)
        {
            // First we submit all reads
            if (dirty_it->second.len == 0)
            {
                has_empty = true;
            }
            else
            {
                offset = dirty_it->second.offset;
                end_offset = dirty_it->second.offset + dirty_it->second.len;
                it = v.begin();
                while (1)
                {
                    for (; it != v.end(); it++)
                        if (it->offset >= offset)
                            break;
                    if (it == v.end() || it->offset > offset && it->len > 0)
                    {
                        submit_offset = dirty_it->second.location + offset - dirty_it->second.offset;
                        submit_len = it == v.end() || it->offset >= end_offset ? end_offset-offset : it->offset-offset;
                        it = v.insert(it, (copy_buffer_t){ .offset = offset, .len = submit_len, .buf = memalign(MEM_ALIGNMENT, submit_len) });
                        copy_count++;
                        if (bs->journal.inmemory)
                        {
                            // Take it from memory
                            memcpy(v.back().buf, bs->journal.buffer + submit_offset, submit_len);
                        }
                        else
                        {
                            // Read it from disk
                            await_sqe(0);
                            data->iov = (struct iovec){ v.back().buf, (size_t)submit_len };
                            data->callback = simple_callback_r;
                            my_uring_prep_readv(
                                sqe, bs->journal_fd_index, &data->iov, 1, bs->journal.offset + submit_offset
                            );
                            sqe->flags |= IOSQE_FIXED_FILE;
                            wait_count++;
                        }
                    }
                    offset = it->offset+it->len;
                    if (it == v.end() || offset >= end_offset)
                        break;
                }
            }
        }
        else if (dirty_it->second.state == ST_D_STABLE && !skip_copy)
        {
            // There is an unflushed big write. Copy small writes in its position
            clean_loc = dirty_it->second.location;
            clean_init_bitmap = true;
            clean_bitmap_offset = dirty_it->second.offset;
            clean_bitmap_len = dirty_it->second.len;
            skip_copy = true;
        }
        else if (dirty_it->second.state == ST_DEL_STABLE && !skip_copy)
        {
            // There is an unflushed delete
            has_delete = true;
            skip_copy = true;
        }
        else if (!IS_STABLE(dirty_it->second.state))
        {
            char err[1024];
            snprintf(
                err, 1024, "BUG: Unexpected dirty_entry %lu:%lu v%lu state during flush: %d",
                dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version, dirty_it->second.state
            );
            throw std::runtime_error(err);
        }
        dirty_start = dirty_it;
        if (dirty_it == bs->dirty_db.begin())
        {
            break;
        }
        dirty_it--;
        if (dirty_it->first.oid != cur.oid)
        {
            break;
        }
    }
    return true;
}

bool journal_flusher_co::modify_meta_read(uint64_t meta_loc, flusher_meta_write_t &wr, int wait_base)
{
    if (wait_state == wait_base)
    {
        goto resume_0;
    }
    // We must check if the same sector is already in memory if we don't keep all metadata in memory all the time.
    // And yet another option is to use LSM trees for metadata, but it sophisticates everything a lot,
    // so I'll avoid it as long as I can.
    wr.submitted = false;
    wr.sector = ((meta_loc >> bs->block_order) / (bs->meta_block_size / bs->clean_entry_size)) * bs->meta_block_size;
    wr.pos = ((meta_loc >> bs->block_order) % (bs->meta_block_size / bs->clean_entry_size));
    if (bs->inmemory_meta)
    {
        wr.buf = bs->metadata_buffer + wr.sector;
        return true;
    }
    wr.it = flusher->meta_sectors.find(wr.sector);
    if (wr.it == flusher->meta_sectors.end())
    {
        // Not in memory yet, read it
        wr.buf = memalign(MEM_ALIGNMENT, bs->meta_block_size);
        wr.it = flusher->meta_sectors.emplace(wr.sector, (meta_sector_t){
            .offset = wr.sector,
            .len = bs->meta_block_size,
            .state = 0, // 0 = not read yet
            .buf = wr.buf,
            .usage_count = 1,
        }).first;
        await_sqe(0);
        data->iov = (struct iovec){ wr.it->second.buf, bs->meta_block_size };
        data->callback = simple_callback_r;
        wr.submitted = true;
        my_uring_prep_readv(
            sqe, bs->meta_fd_index, &data->iov, 1, bs->meta_offset + wr.sector
        );
        sqe->flags |= IOSQE_FIXED_FILE;
        wait_count++;
    }
    else
    {
        wr.buf = wr.it->second.buf;
        wr.it->second.usage_count++;
    }
    return true;
}

void journal_flusher_co::update_clean_db()
{
    if (old_clean_loc != UINT64_MAX && old_clean_loc != clean_loc)
    {
#ifdef BLOCKSTORE_DEBUG
        printf("Free block %lu\n", old_clean_loc >> bs->block_order);
#endif
        bs->data_alloc->set(old_clean_loc >> bs->block_order, false);
    }
    if (has_delete)
    {
        auto clean_it = bs->clean_db.find(cur.oid);
        bs->clean_db.erase(clean_it);
        bs->data_alloc->set(clean_loc >> bs->block_order, false);
        clean_loc = UINT64_MAX;
    }
    else
    {
        bs->clean_db[cur.oid] = {
            .version = cur.version,
            .location = clean_loc,
        };
    }
    bs->erase_dirty(dirty_start, std::next(dirty_end), clean_loc);
}

bool journal_flusher_co::fsync_batch(bool fsync_meta, int wait_base)
{
    if (wait_state == wait_base)
        goto resume_0;
    else if (wait_state == wait_base+1)
        goto resume_1;
    else if (wait_state == wait_base+2)
        goto resume_2;
    if (!(fsync_meta ? bs->disable_meta_fsync : bs->disable_journal_fsync))
    {
        cur_sync = flusher->syncs.end();
        while (cur_sync != flusher->syncs.begin())
        {
            cur_sync--;
            if (cur_sync->fsync_meta == fsync_meta && cur_sync->state == 0)
            {
                goto sync_found;
            }
        }
        cur_sync = flusher->syncs.emplace(flusher->syncs.end(), (flusher_sync_t){
            .fsync_meta = fsync_meta,
            .ready_count = 0,
            .state = 0,
        });
    sync_found:
        cur_sync->ready_count++;
        if (cur_sync->ready_count >= flusher->sync_threshold || !flusher->flush_queue.size())
        {
            // Sync batch is ready. Do it.
            await_sqe(0);
            data->iov = { 0 };
            data->callback = simple_callback_w;
            my_uring_prep_fsync(sqe, fsync_meta ? bs->meta_fd_index : bs->data_fd_index, IORING_FSYNC_DATASYNC);
            sqe->flags |= IOSQE_FIXED_FILE;
            cur_sync->state = 1;
            wait_count++;
        resume_1:
            if (wait_count > 0)
            {
                wait_state = 1;
                return false;
            }
            // Sync completed. All previous coroutines waiting for it must be resumed
            cur_sync->state = 2;
            bs->ringloop->wakeup();
        }
        // Wait until someone else sends and completes a sync.
    resume_2:
        if (!cur_sync->state)
        {
            wait_state = 2;
            return false;
        }
        cur_sync->ready_count--;
        if (cur_sync->ready_count == 0)
        {
            flusher->syncs.erase(cur_sync);
        }
    }
    return true;
}

void journal_flusher_co::bitmap_set(void *bitmap, uint64_t start, uint64_t len)
{
    if (start == 0)
    {
        if (len == 32*bs->bitmap_granularity)
        {
            *((uint32_t*)bitmap) = UINT32_MAX;
            return;
        }
        else if (len == 64*bs->bitmap_granularity)
        {
            *((uint64_t*)bitmap) = UINT64_MAX;
            return;
        }
    }
    unsigned bit_start = start / bs->bitmap_granularity;
    unsigned bit_end = ((start + len) + bs->bitmap_granularity - 1) / bs->bitmap_granularity;
    while (bit_start < bit_end)
    {
        if (!(bit_start & 7) && bit_end >= bit_start+8)
        {
            ((uint8_t*)bitmap)[bit_start / 8] = UINT8_MAX;
            bit_start += 8;
        }
        else
        {
            ((uint8_t*)bitmap)[bit_start / 8] |= 1 << (bit_start % 8);
            bit_start++;
        }
    }
}
