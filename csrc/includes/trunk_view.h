#pragma once

class TrunkView {
public:
    int fd;
    uint32_t trunk_id;
    void* data;
    uint32_t length;
    TrunkView(int fd, uint32_t trunk_id, void* data, uint32_t length) : 
        fd(fd), trunk_id(trunk_id), data(data), length(length) {}
    TrunkView() : fd(0), trunk_id(0), data(nullptr), length(0) {}
};
