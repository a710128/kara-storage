#pragma once
#include "storage/storage.h"

struct LocalMetaInfo {
    uint32_t num_trunks;
    uint32_t last_trunk_size;
    uint32_t max_trunk_size;
    uint32_t trunks_per_file;
};


class LocalStorage : public Storage {
private:
    int base_dir_fd, meta_fd;
    LocalMetaInfo meta;
    uint32_t trunks_per_file;
    void write_meta();
    void read_meta();
public:
    LocalStorage(int base_dir_fd, uint32_t max_trunk_size = DEFAULT_TRUNK_SIZE, uint32_t trunks_per_file = 4);
    ~LocalStorage();
    void read(uint32_t trunk_id, void* dest);
    uint32_t get_trunk_size() const;
    uint32_t get_trunk_num() const;
    uint32_t last_trunk_size() const;
    void write(uint32_t trunk_id, const void * src, uint32_t length);
    void pread(uint32_t trunk_id, uint32_t offset, uint32_t length, void* dest);
    void flush();
};