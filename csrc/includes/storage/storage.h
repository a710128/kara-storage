#pragma once
#include <cstdint>

const uint32_t DEFAULT_TRUNK_SIZE = 32 * 1024 * 1024;

class Storage {
public:
    virtual ~Storage()  {}
    virtual void read(uint32_t trunk_id, void* dest) = 0;
    virtual uint32_t get_trunk_size() const = 0;
    virtual uint32_t get_trunk_num() const = 0;
    virtual uint32_t last_trunk_size() const = 0;
    virtual void write(uint32_t trunk_id, const void * src, uint32_t length) = 0;
    virtual void pread(uint32_t trunk_id, uint32_t offset, uint32_t length, void* dest) = 0;
    virtual void flush() = 0;
};