#pragma once
#include <stdint.h>
#include "trunk_view.h"

class TrunkController {
public:
    TrunkController(){}
    virtual uint64_t append(const void* data, const uint32_t length) = 0;
    virtual TrunkView link(const uint32_t trunk_id) = 0;
    virtual void unlink(TrunkView) = 0;
    virtual void flush() = 0;
    virtual void pread(void* dest, uint32_t trunk_id, uint32_t offset, uint32_t length) = 0;
    virtual uint32_t get_num_trunks() const = 0;
    virtual uint32_t last_trunk_size() const = 0;
    virtual ~TrunkController() {};
};
