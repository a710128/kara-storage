#pragma once

#include <stdint.h>
#include "exceptions.h"
#include "trunk_view.h"
#include "storage/storage.h"
#include <thread>
#include <mutex>
#include "queue/readerwriterqueue.h"

const uint32_t IORequest_WRITE = 0;
const uint32_t IORequest_PREPARE = 1;

struct IORequest {
    uint32_t op;
    uint32_t trunk_id;
    uint32_t length;
    uint8_t* data;
    TrunkLinkInfo *info;
};

class TrunkController {
private:
    Storage *storage;
    std::thread *load_thread;
    TrunkView last_trunk;

    uint32_t max_trunk_size, last_buffer_pos;
    moodycamel::ReaderWriterQueue<IORequest> io_queue;
    uint32_t num_trunks;

    KaraStorageException *err;

    TrunkLinkInfo* trunk_link_head;

    std::mutex prepare_lock, unlink_lock;


    volatile int current_prepare_trunk_id;
    volatile bool prepare_done;
    volatile bool stop;
    uint8_t *current_prepare_buffer;
    bool write_since_last_flush;
private:
    void load_process();
    void submit_write_buffer();
    void prepare_trunk(uint32_t trunk_id);
    void unlink_info(TrunkLinkInfo*);
public:
    TrunkController(Storage *storage, uint32_t io_queue_size = 16);
    ~TrunkController();

    uint64_t append(const void* data, const uint32_t length);
    TrunkView link(const uint32_t trunk_id);
    void relink(const uint32_t trunk_id, TrunkView&);
    void unlink(TrunkView);
    void flush();
    void pread(void* dest, uint32_t trunk_id, uint32_t offset, uint32_t length);
    uint32_t get_num_trunks() const;
    uint32_t last_trunk_size() const;
    
};