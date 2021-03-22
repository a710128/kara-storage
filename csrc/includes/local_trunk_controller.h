#pragma once
#include "trunk_controller.h"
const uint32_t FLUSH_INTERVAL = 4 * 1024 * 1024; // force flush for every 4MB writes
class LocalTrunkController: public TrunkController {
private:
    int base_dir_fd, meta_fd;
    uint32_t max_trunk_size, max_file_size, trunks_per_file, trunk_size_pow;
    uint32_t num_trunks;
    uint32_t last_file_size, bytes_since_last_flush;
    int current_fd;
    // void* write_mem_map;
    char buffer[FLUSH_INTERVAL];
    bool writable;
private:
    void read_last_file_size();
    void write_last_file_size();
    int init_file(uint32_t);
    int open_file(uint32_t, bool writable = false);
    void quick_flush();
public:
    // default max 128 MB for each file
    LocalTrunkController(int base_dir_fd, bool writable = false, uint32_t max_trunk_size = 32 * 1024 * 1024, uint32_t trunks_per_file = 4);
    ~LocalTrunkController();
    uint64_t append(const void* data, const uint32_t length);
    TrunkView link(const uint32_t trunk_id);
    void relink(const uint32_t trunk_id, TrunkView&);
    void unlink(TrunkView);
    void flush();
    
    void pread(void* dest, uint32_t trunk_id, uint32_t offset, uint32_t length);
    uint32_t get_num_trunks() const;
    uint32_t last_trunk_size() const;
};
