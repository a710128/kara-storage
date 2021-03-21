#pragma once
#include "trunk_controller.h"
#include <dirent.h>
#include <stdint.h>

const int INDEX_FILE_SIZE_POW = 25; // 32MB
const int INDEX_FILE_SIZE = 1 << INDEX_FILE_SIZE_POW;

const int INDEX_PER_FILE_POW = INDEX_FILE_SIZE_POW - 3;
const int INDEX_PER_FILE = INDEX_FILE_SIZE / 8;
const int IN_TRUNK_INDEX_MASK = INDEX_PER_FILE - 1;

class DataView {
private:
    bool need_free;
public:
    const char* data;
    uint32_t length;
    DataView(const char* data, uint32_t length, bool need_free = false) :  need_free(need_free), data(data), length(length) {};
    void free() {
        if (this->need_free) {
            this->need_free = false;
            delete[] data;
        }
    }
};


class Dataset {
private:
    TrunkController *index;
    TrunkController *data;
    bool writable;

    TrunkView view_data;
    TrunkView view_index;

    uint32_t _tell, _total;
    uint32_t curr_data_trunk, curr_data_offset;
public:
    Dataset(TrunkController *index, TrunkController *data, bool writable = false, int max_trunk_size = 32 * 1024 * 1024, int trunks_per_file = 4);
    ~Dataset();
    void write(const DataView&);
    void flush();
    DataView read();
    void seek(uint32_t offset, uint32_t whence);
    DataView pread(uint32_t offset);
    uint32_t size() const;
    uint32_t tell() const;
};