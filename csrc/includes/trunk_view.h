#pragma once

class TrunkView {
public:
    int fd;
    void* data;
    uint32_t length;
    TrunkView(int fd, void* data, uint32_t length) : fd(fd), data(data), length(length) {}
    TrunkView() : fd(0), data(nullptr), length(0) {}
};
