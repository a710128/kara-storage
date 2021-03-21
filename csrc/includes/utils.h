#pragma once
#include <stdint.h>

inline uint32_t low32(uint64_t x) {
    return x & ((uint64_t(1) << uint64_t(32)) - 1);
}
inline uint32_t high32(uint64_t x) {
    return x >> uint64_t(32);
}