#pragma once

#include "blockstore.h"

#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <malloc.h>
#include <linux/fs.h>

#include <vector>
#include <list>
#include <deque>
#include <new>

#include "cpp-btree/btree_map.h"

#include "allocator.h"

//#define BLOCKSTORE_DEBUG

// States are not stored on disk. Instead, they're deduced from the journal
// FIXME: Rename to BS_ST_*

#define ST_J_WAIT_BIG 1
#define ST_J_IN_FLIGHT 2
#define ST_J_SUBMITTED 3
#define ST_J_WRITTEN 4
#define ST_J_SYNCED 5
#define ST_J_STABLE 6

#define ST_D_IN_FLIGHT 15
#define ST_D_SUBMITTED 16
#define ST_D_WRITTEN 17
#define ST_D_SYNCED 20
#define ST_D_STABLE 21

#define ST_DEL_IN_FLIGHT 31
#define ST_DEL_SUBMITTED 32
#define ST_DEL_WRITTEN 33
#define ST_DEL_SYNCED 34
#define ST_DEL_STABLE 35

#define ST_CURRENT 48

#define IMMEDIATE_NONE 0
#define IMMEDIATE_SMALL 1
#define IMMEDIATE_ALL 2

#define IS_IN_FLIGHT(st) (st == ST_J_WAIT_BIG || st == ST_J_IN_FLIGHT || st == ST_D_IN_FLIGHT || st == ST_DEL_IN_FLIGHT || st == ST_J_SUBMITTED || st == ST_D_SUBMITTED || st == ST_DEL_SUBMITTED)
#define IS_STABLE(st) (st == ST_J_STABLE || st == ST_D_STABLE || st == ST_DEL_STABLE || st == ST_CURRENT)
#define IS_SYNCED(st) (IS_STABLE(st) || st == ST_J_SYNCED || st == ST_D_SYNCED || st == ST_DEL_SYNCED)
#define IS_JOURNAL(st) (st >= ST_J_WAIT_BIG && st <= ST_J_STABLE)
#define IS_BIG_WRITE(st) (st >= ST_D_IN_FLIGHT && st <= ST_D_STABLE)
#define IS_DELETE(st) (st >= ST_DEL_IN_FLIGHT && st <= ST_DEL_STABLE)
#define IS_UNSYNCED(st) (st >= ST_J_WAIT_BIG && st <= ST_J_WRITTEN || st >= ST_D_IN_FLIGHT && st <= ST_D_WRITTEN|| st >= ST_DEL_IN_FLIGHT && st <= ST_DEL_WRITTEN)

#define BS_SUBMIT_GET_SQE(sqe, data) \
    BS_SUBMIT_GET_ONLY_SQE(sqe); \
    struct ring_data_t *data = ((ring_data_t*)sqe->user_data)

#define BS_SUBMIT_GET_ONLY_SQE(sqe) \
        {\
            timespec now;\
            clock_gettime(CLOCK_REALTIME, &now);\
            printf("get_sqe %s %d %ld.%06ld\n", __FILE__, __LINE__, now.tv_sec, now.tv_nsec/1000);\
        }\
    struct io_uring_sqe *sqe = get_sqe();\
    if (!sqe)\
    {\
        /* Pause until there are more requests available */\
        PRIV(op)->wait_for = WAIT_SQE;\
        return 0;\
    }

#define BS_SUBMIT_GET_SQE_DECL(sqe) \
        {\
            timespec now;\
            clock_gettime(CLOCK_REALTIME, &now);\
            printf("get_sqe %s %d %ld.%06ld\n", __FILE__, __LINE__, now.tv_sec, now.tv_nsec/1000);\
        }\
    sqe = get_sqe();\
    if (!sqe)\
    {\
        /* Pause until there are more requests available */\
        PRIV(op)->wait_for = WAIT_SQE;\
        return 0;\
    }

#include "blockstore_journal.h"

// 24 bytes + block bitmap per "clean" entry on disk with fixed metadata tables
// FIXME: maybe add crc32's to metadata
struct __attribute__((__packed__)) clean_disk_entry
{
    object_id oid;
    uint64_t version;
    uint8_t bitmap[];
};

// 32 = 16 + 16 bytes per "clean" entry in memory (object_id => clean_entry)
struct __attribute__((__packed__)) clean_entry
{
    uint64_t version;
    uint64_t location;
};

// 56 = 24 + 32 bytes per dirty entry in memory (obj_ver_id => dirty_entry)
struct __attribute__((__packed__)) dirty_entry
{
    uint32_t state;
    uint32_t flags;    // unneeded, but present for alignment
    uint64_t location; // location in either journal or data -> in BYTES
    uint32_t offset;   // data offset within object (stripe)
    uint32_t len;      // data length
    uint64_t journal_sector; // journal sector used for this entry
};

// - Sync must be submitted after previous writes/deletes (not before!)
// - Reads to the same object must be submitted after previous writes/deletes
//   are written (not necessarily synced) in their location. This is because we
//   rely on read-modify-write for erasure coding and we must return new data
//   to calculate parity for subsequent writes
// - Writes may be submitted in any order, because they don't overlap. Each write
//   goes into a new location - either on the journal device or on the data device
// - Stable (stabilize) must be submitted after sync of that object is completed
//   It's even OK to return an error to the caller if that object is not synced yet
// - Journal trim may be processed only after all versions are moved to
//   the main storage AND after all read operations for older versions complete
// - If an operation can not be submitted because the ring is full
//   we should stop submission of other operations. Otherwise some "scatter" reads
//   may end up blocked for a long time.
// Otherwise, the submit order is free, that is all operations may be submitted immediately
// In fact, adding a write operation must immediately result in dirty_db being populated

// Suspend operation until there are more free SQEs
#define WAIT_SQE 1
// Suspend operation until there are <wait_detail> bytes of free space in the journal on disk
#define WAIT_JOURNAL 3
// Suspend operation until the next journal sector buffer is free
#define WAIT_JOURNAL_BUFFER 4
// Suspend operation until there is some free space on the data device
#define WAIT_FREE 5

struct fulfill_read_t
{
    uint64_t offset, len;
};

#define PRIV(op) ((blockstore_op_private_t*)(op)->private_data)
#define FINISH_OP(op) PRIV(op)->~blockstore_op_private_t(); std::function<void (blockstore_op_t*)>(op->callback)(op)

struct blockstore_op_private_t
{
    // Wait status
    int wait_for;
    uint64_t wait_detail;
    int pending_ops;
    int op_state;

    // Read
    std::vector<fulfill_read_t> read_vec;

    // Sync, write
    uint64_t min_flushed_journal_sector, max_flushed_journal_sector;

    // Write
    struct iovec iov_zerofill[3];

    // Sync
    std::vector<obj_ver_id> sync_big_writes, sync_small_writes;
    int sync_small_checked, sync_big_checked;
    std::list<blockstore_op_t*>::iterator in_progress_ptr;
    int prev_sync_count;
};

// https://github.com/algorithm-ninja/cpp-btree
// https://github.com/greg7mdp/sparsepp/ was used previously, but it was TERRIBLY slow after resizing
// with sparsepp, random reads dropped to ~700 iops very fast with just as much as ~32k objects in the DB
typedef btree::btree_map<object_id, clean_entry> blockstore_clean_db_t;
typedef std::map<obj_ver_id, dirty_entry> blockstore_dirty_db_t;

#include "blockstore_init.h"

#include "blockstore_flush.h"

class blockstore_impl_t
{
    /******* OPTIONS *******/
    std::string data_device, meta_device, journal_device;
    uint32_t block_size;
    uint64_t meta_offset;
    uint64_t data_offset;
    uint64_t cfg_journal_size, cfg_data_size;
    // Required write alignment and journal/metadata/data areas' location alignment
    uint32_t disk_alignment = 4096;
    // Journal block size - minimum_io_size of the journal device is the best choice
    uint64_t journal_block_size = 4096;
    // Metadata block size - minimum_io_size of the metadata device is the best choice
    uint64_t meta_block_size = 4096;
    // Sparse write tracking granularity. 4 KB is a good choice. Must be a multiple of disk_alignment
    uint64_t bitmap_granularity = 4096;
    bool readonly = false;
    // By default, Blockstore locks all opened devices exclusively. This option can be used to disable locking
    bool disable_flock = false;
    // It is safe to disable fsync() if drive write cache is writethrough
    bool disable_data_fsync = false, disable_meta_fsync = false, disable_journal_fsync = false;
    // Enable if you want every operation to be executed with an "implicit fsync"
    // Suitable only for server SSDs with capacitors, requires disabled data and journal fsyncs
    int immediate_commit = IMMEDIATE_NONE;
    bool inmemory_meta = false;
    int flusher_count;
    /******* END OF OPTIONS *******/

    struct ring_consumer_t ring_consumer;

    blockstore_clean_db_t clean_db;
    uint8_t *clean_bitmap = NULL;
    blockstore_dirty_db_t dirty_db;
    std::list<blockstore_op_t*> submit_queue; // FIXME: funny thing is that vector is better here
    std::vector<obj_ver_id> unsynced_big_writes, unsynced_small_writes;
    std::list<blockstore_op_t*> in_progress_syncs; // ...and probably here, too
    allocator *data_alloc = NULL;
    uint8_t *zero_object;

    uint32_t block_order;
    uint64_t block_count;
    uint32_t clean_entry_bitmap_size = 0, clean_entry_size = 0;

    int meta_fd;
    int data_fd;
    uint64_t meta_size, meta_area, meta_len;
    uint64_t data_size, data_len;

    void *metadata_buffer = NULL;

    struct journal_t journal;
    journal_flusher_t *flusher;

    bool live = false, queue_stall = false;
    ring_loop_t *ringloop;
    int inflight_writes = 0;

    bool stop_sync_submitted;

    inline struct io_uring_sqe* get_sqe()
    {
        return ringloop->get_sqe();
    }

    friend class blockstore_init_meta;
    friend class blockstore_init_journal;
    friend class blockstore_journal_check_t;
    friend class journal_flusher_t;
    friend class journal_flusher_co;

    void parse_config(blockstore_config_t & config);
    void calc_lengths();
    void open_data();
    void open_meta();
    void open_journal();

    // Asynchronous init
    int initialized;
    int metadata_buf_size;
    blockstore_init_meta* metadata_init_reader;
    blockstore_init_journal* journal_init_reader;

    void check_wait(blockstore_op_t *op);

    // Read
    int dequeue_read(blockstore_op_t *read_op);
    int fulfill_read(blockstore_op_t *read_op, uint64_t &fulfilled, uint32_t item_start, uint32_t item_end,
        uint32_t item_state, uint64_t item_version, uint64_t item_location);
    int fulfill_read_push(blockstore_op_t *op, void *buf, uint64_t offset, uint64_t len,
        uint32_t item_state, uint64_t item_version);
    void handle_read_event(ring_data_t *data, blockstore_op_t *op);

    // Write
    bool enqueue_write(blockstore_op_t *op);
    int dequeue_write(blockstore_op_t *op);
    int dequeue_del(blockstore_op_t *op);
    int continue_write(blockstore_op_t *op);
    void release_journal_sectors(blockstore_op_t *op);
    void handle_write_event(ring_data_t *data, blockstore_op_t *op);

    // Sync
    int dequeue_sync(blockstore_op_t *op);
    void handle_sync_event(ring_data_t *data, blockstore_op_t *op);
    int continue_sync(blockstore_op_t *op);
    void ack_one_sync(blockstore_op_t *op);
    int ack_sync(blockstore_op_t *op);

    // Stabilize
    int dequeue_stable(blockstore_op_t *op);
    int continue_stable(blockstore_op_t *op);
    void mark_stable(const obj_ver_id & ov);
    void handle_stable_event(ring_data_t *data, blockstore_op_t *op);
    void stabilize_object(object_id oid, uint64_t max_ver);

    // Rollback
    int dequeue_rollback(blockstore_op_t *op);
    int continue_rollback(blockstore_op_t *op);
    void mark_rolled_back(const obj_ver_id & ov);
    void handle_rollback_event(ring_data_t *data, blockstore_op_t *op);
    void erase_dirty(blockstore_dirty_db_t::iterator dirty_start, blockstore_dirty_db_t::iterator dirty_end, uint64_t clean_loc);

    // List
    void process_list(blockstore_op_t *op);

public:

    blockstore_impl_t(blockstore_config_t & config, ring_loop_t *ringloop);
    ~blockstore_impl_t();

    // Event loop
    void loop();

    // Returns true when blockstore is ready to process operations
    // (Although you're free to enqueue them before that)
    bool is_started();

    // Returns true when it's safe to destroy the instance. If destroying the instance
    // requires to purge some queues, starts that process. Should be called in the event
    // loop until it returns true.
    bool is_safe_to_stop();

    // Returns true if stalled
    bool is_stalled();

    // Submission
    void enqueue_op(blockstore_op_t *op, bool first = false);

    // Unstable writes are added here (map of object_id -> version)
    std::unordered_map<object_id, uint64_t> unstable_writes;

    inline uint32_t get_block_size() { return block_size; }
    inline uint64_t get_block_count() { return block_count; }
    inline uint64_t get_free_block_count() { return data_alloc->get_free_count(); }
    inline uint32_t get_disk_alignment() { return disk_alignment; }
};
