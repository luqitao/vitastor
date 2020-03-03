#include "blockstore_impl.h"

blockstore_journal_check_t::blockstore_journal_check_t(blockstore_impl_t *bs)
{
    this->bs = bs;
    sectors_required = 0;
    next_pos = bs->journal.next_free;
    next_sector = bs->journal.cur_sector;
    next_in_pos = bs->journal.in_sector_pos;
    right_dir = next_pos >= bs->journal.used_start;
}

// Check if we can write <required> entries of <size> bytes and <data_after> data bytes after them to the journal
int blockstore_journal_check_t::check_available(blockstore_op_t *op, int required, int size, int data_after)
{
    while (1)
    {
        int fits = (bs->journal.block_size - next_in_pos) / size;
        if (fits > 0)
        {
            required -= fits;
            next_in_pos += fits * size;
            sectors_required++;
        }
        else if (bs->journal.sector_info[next_sector].dirty)
        {
            // sectors_required is more like "sectors to write"
            sectors_required++;
        }
        if (required <= 0)
        {
            break;
        }
        next_pos = next_pos + bs->journal.block_size;
        if (next_pos >= bs->journal.len)
        {
            next_pos = bs->journal.block_size;
            right_dir = false;
        }
        next_in_pos = 0;
        if (bs->journal.sector_info[next_sector].usage_count > 0 ||
            bs->journal.sector_info[next_sector].dirty)
        {
            next_sector = ((next_sector + 1) % bs->journal.sector_count);
        }
        if (bs->journal.sector_info[next_sector].usage_count > 0 ||
            bs->journal.sector_info[next_sector].dirty)
        {
            // No memory buffer available. Wait for it.
#ifdef BLOCKSTORE_DEBUG
            printf("next journal buffer %d is still dirty=%d used=%d\n", next_sector,
                bs->journal.sector_info[next_sector].dirty, bs->journal.sector_info[next_sector].usage_count);
#endif
            PRIV(op)->wait_for = WAIT_JOURNAL_BUFFER;
            return 0;
        }
    }
    if (data_after > 0)
    {
        next_pos = next_pos + data_after;
        if (next_pos > bs->journal.len)
        {
            next_pos = bs->journal.block_size + data_after;
            right_dir = false;
        }
    }
    if (!right_dir && next_pos >= bs->journal.used_start-bs->journal.block_size)
    {
        // No space in the journal. Wait until used_start changes.
        printf(
            "Ran out of journal space (free space: %lu bytes)\n",
            (bs->journal.next_free >= bs->journal.used_start
                ? bs->journal.len-bs->journal.block_size - (bs->journal.next_free-bs->journal.used_start)
                : bs->journal.used_start - bs->journal.next_free)
        );
        PRIV(op)->wait_for = WAIT_JOURNAL;
        bs->flusher->force_start();
        PRIV(op)->wait_detail = bs->journal.used_start;
        return 0;
    }
    return 1;
}

journal_entry* prefill_single_journal_entry(journal_t & journal, uint16_t type, uint32_t size)
{
    if (journal.block_size - journal.in_sector_pos < size)
    {
        assert(!journal.sector_info[journal.cur_sector].dirty);
        // Move to the next journal sector
        if (journal.sector_info[journal.cur_sector].usage_count > 0)
        {
            // Also select next sector buffer in memory
            journal.cur_sector = ((journal.cur_sector + 1) % journal.sector_count);
        }
        journal.sector_info[journal.cur_sector].offset = journal.next_free;
        journal.in_sector_pos = 0;
        journal.next_free = (journal.next_free+journal.block_size) < journal.len ? journal.next_free + journal.block_size : journal.block_size;
        memset(journal.inmemory
            ? journal.buffer + journal.sector_info[journal.cur_sector].offset
            : journal.sector_buf + journal.block_size*journal.cur_sector, 0, journal.block_size);
    }
    journal_entry *je = (struct journal_entry*)(
        (journal.inmemory
            ? journal.buffer + journal.sector_info[journal.cur_sector].offset
            : journal.sector_buf + journal.block_size*journal.cur_sector) + journal.in_sector_pos
    );
    journal.in_sector_pos += size;
    je->magic = JOURNAL_MAGIC;
    je->type = type;
    je->size = size;
    je->crc32_prev = journal.crc32_last;
    journal.sector_info[journal.cur_sector].dirty = true;
    return je;
}

void prepare_journal_sector_write(journal_t & journal, int cur_sector, io_uring_sqe *sqe, std::function<void(ring_data_t*)> cb)
{
    journal.sector_info[cur_sector].dirty = false;
    journal.sector_info[cur_sector].usage_count++;
    ring_data_t *data = ((ring_data_t*)sqe->user_data);
    data->iov = (struct iovec){
        (journal.inmemory
            ? journal.buffer + journal.sector_info[cur_sector].offset
            : journal.sector_buf + journal.block_size*cur_sector),
        journal.block_size
    };
    data->callback = cb;
    my_uring_prep_writev(
        sqe, journal.fd_index, &data->iov, 1, journal.offset + journal.sector_info[cur_sector].offset
    );
    sqe->flags |= IOSQE_FIXED_FILE;
}

journal_t::~journal_t()
{
    if (sector_buf)
        free(sector_buf);
    if (sector_info)
        free(sector_info);
    if (buffer)
        free(buffer);
    sector_buf = NULL;
    sector_info = NULL;
    buffer = NULL;
}

bool journal_t::trim()
{
    auto journal_used_it = used_sectors.lower_bound(used_start);
#ifdef BLOCKSTORE_DEBUG
    printf(
        "Trimming journal (used_start=%lu, next_free=%lu, first_used=%lu, usage_count=%lu)\n",
        used_start, next_free,
        journal_used_it == used_sectors.end() ? 0 : journal_used_it->first,
        journal_used_it == used_sectors.end() ? 0 : journal_used_it->second
    );
#endif
    if (journal_used_it == used_sectors.end())
    {
        // Journal is cleared to its end, restart from the beginning
        journal_used_it = used_sectors.begin();
        if (journal_used_it == used_sectors.end())
        {
            // Journal is empty
            used_start = next_free;
        }
        else
        {
            used_start = journal_used_it->first;
            // next_free does not need updating here
        }
    }
    else if (journal_used_it->first > used_start)
    {
        // Journal is cleared up to <journal_used_it>
        used_start = journal_used_it->first;
    }
    else
    {
        // Can't trim journal
        return false;
    }
#ifdef BLOCKSTORE_DEBUG
    printf("Journal trimmed to %lu (next_free=%lu)\n", used_start, next_free);
#endif
    return true;
}
