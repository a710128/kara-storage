#include "dataset.h"
#include "exceptions.h"
#include <cstring>
#include <cstdio>
#include <unistd.h>
#include "utils.h"


Dataset::Dataset(TrunkController *index, TrunkController *data, bool writable, int max_trunk_size, int trunks_per_file) {
    this->writable = writable;

    this->index = index;
    this->data = data;
   
    int num_trunks = this->index->get_num_trunks();
    int f_size = this->index->last_trunk_size();
    this->_total = ((num_trunks - 1) * INDEX_FILE_SIZE + f_size) / sizeof(uint64_t);

    this->curr_data_trunk = 0;
    this->curr_data_offset = 0;
    this->view_data = this->data->link(0);
    this->view_index = this->index->link(0);
}

Dataset::~Dataset() {
    this->data->unlink(this->view_data);
    this->index->unlink(this->view_index);
}

void Dataset::write(const DataView &data) {
    if (!this->writable) throw KaraStorageException("Dataset not writable");
    uint64_t pos = this->data->append(data.data, data.length);
    this->index->append(&pos, sizeof(pos));
    this->_total ++;
}

void Dataset::flush() {
    if (!this->writable) return;
    this->index->flush();
    this->data->flush();
}


DataView Dataset::read() {
    uint64_t data_info = *((uint64_t *)(this->view_index.data) + (this->_tell & IN_TRUNK_INDEX_MASK));
    uint32_t data_trunk = high32(data_info);
    uint32_t data_offset = low32(data_info);
    if (this->curr_data_trunk != data_trunk) {
        // data starts from new trunk
        this->data->unlink(this->view_data);
        this->view_data = this->data->link(data_trunk);
        this->curr_data_trunk = data_trunk;
        this->curr_data_offset = 0;
    }
    int start = this->curr_data_offset;
    uint32_t length = data_offset - this->curr_data_offset;

    // move pointer to next index    
    this->_tell ++;
    if ((this->_tell & IN_TRUNK_INDEX_MASK) == 0) {
        // index in new trunk
        this->index->unlink(this->view_index);
        this->view_index = this->index->link(this->_tell >> INDEX_PER_FILE_POW);
    }

    return DataView((char*)(this->view_data.data) + start, length);
}


void Dataset::seek(uint32_t offset, uint32_t whence) {
    uint32_t nwtell = 0;
    if (whence == 0) {
        nwtell = offset;
    }
    else if (whence == 1) {
        nwtell = this->_tell;
    }
    else if (whence == 2) {
        nwtell = this->_total - offset;
    }

    int curr_index_trunk = this->_tell >> INDEX_PER_FILE_POW;
    int nw_index_trunk = nwtell >> INDEX_PER_FILE_POW;
    uint32_t old_data_trunk = this->curr_data_trunk;

    if (nw_index_trunk != curr_index_trunk) {
        // switch index view
        this->index->unlink(this->view_index);
        this->view_index = this->index->link(nw_index_trunk);
    }
    if (nwtell > 0) {
        // prev index block
        int nw_index_prev_trunk = (nwtell - 1) / INDEX_PER_FILE;

        uint64_t prev_info;
        if (nw_index_prev_trunk != nw_index_trunk) {
            // not in the same trunk
            this->index->pread(&prev_info, nw_index_prev_trunk, ((nwtell - 1) & IN_TRUNK_INDEX_MASK) << 3, sizeof(prev_info));
        } else {
            // in the same trunk
            prev_info = *((uint64_t *)(this->view_index.data) + ((nwtell - 1) & IN_TRUNK_INDEX_MASK));
        }
        this->curr_data_trunk = high32(prev_info);
        this->curr_data_offset = low32(prev_info);
    } else {
        this->curr_data_trunk = 0;
        this->curr_data_offset = 0;
    }

    uint64_t next_data_trunk = high32(*((uint64_t *)(this->view_index.data) + (nwtell & IN_TRUNK_INDEX_MASK)));
    
    if (this->curr_data_trunk != next_data_trunk) {
        this->curr_data_trunk = next_data_trunk;
        this->curr_data_offset = 0;
    }
    if (this->curr_data_trunk != old_data_trunk) {
        this->data->unlink(this->view_data);
        this->view_data = this->data->link(this->curr_data_trunk);
    }
    this->_tell = nwtell;
}


DataView Dataset::pread(uint32_t offset) {
    uint64_t pos[2];
    
    uint32_t trunk = offset >> INDEX_PER_FILE_POW;
    uint32_t trunk_prev = 0;
    if (offset > 0) {
        trunk_prev = (offset - 1) >> INDEX_PER_FILE_POW;
    }

    uint32_t intrunk_off = offset & IN_TRUNK_INDEX_MASK;
    uint32_t intrunk_off_prev = 0;
    if (offset > 0) {
        intrunk_off_prev = (offset - 1) & IN_TRUNK_INDEX_MASK;
    }

    uint32_t current_index_trunk = this->_tell >> INDEX_PER_FILE_POW;

    if (trunk == trunk_prev) {
        if (trunk == current_index_trunk) {
            // no pread needed
            pos[0] = *((uint64_t*)(this->view_index.data) + intrunk_off_prev);
            pos[1] = *((uint64_t*)(this->view_index.data) + intrunk_off);
        } else {
            this->index->pread(pos, trunk, intrunk_off_prev << 3, sizeof(uint64_t) * 2);
        }
    } else {
        // two index not in the same trunk
        if (trunk_prev == current_index_trunk) {
            pos[0] = *((uint64_t*)(this->view_index.data) + intrunk_off_prev);
        } else {
            this->index->pread(pos, trunk_prev, intrunk_off_prev << 3, sizeof(uint64_t));
        }

        if (trunk_prev == current_index_trunk) {
            pos[1] = *((uint64_t*)(this->view_index.data) + intrunk_off);
        } else {
            this->index->pread(pos + 1, trunk_prev, intrunk_off << 3, sizeof(uint64_t));
        }
    }

    trunk_prev = high32(pos[0]);
    trunk = high32(pos[1]);
    if (offset == 0) {
        trunk_prev = 0;
        intrunk_off_prev = 0;
        intrunk_off = low32(pos[0]);
    } else {
        intrunk_off_prev = low32(pos[0]);
        intrunk_off = low32(pos[1]);
    }
    if (trunk != trunk_prev) {
        intrunk_off_prev = 0;
    }
    uint32_t length = intrunk_off - intrunk_off_prev;
    if (trunk == this->curr_data_trunk) {
        return DataView((char*)(this->view_data.data) + intrunk_off_prev, length);
    } else {
        char* v = new char[length];
        this->data->pread(v, trunk, intrunk_off_prev, length);
        return DataView( v, length, true);
    }
}


uint32_t Dataset::tell() const { return this->_tell; }
uint32_t Dataset::size() const { return this->_total; }