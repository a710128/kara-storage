#include "trunk_controller.h"
#include "exceptions.h"
#include <cstring>
#include <cstdio>


TrunkController::TrunkController(Storage *storage, uint32_t io_queue_size) : storage(storage), io_queue(io_queue_size) {
    
    this->max_trunk_size = this->storage->get_trunk_size();
    this->last_buffer_pos = this->storage->last_trunk_size();
    this->num_trunks = this->storage->get_trunk_num();

    this->err = NULL;
    this->trunk_link_head = NULL;
    
    this->current_prepare_trunk_id = -1;
    this->prepare_done = false;
    this->stop = false;
    this->current_prepare_buffer = new uint8_t[this->max_trunk_size];
    this->load_thread = new std::thread(&TrunkController::load_process, this);

    this->last_trunk = this->link(this->num_trunks - 1);
    this->write_since_last_flush = false;
    

}

TrunkController::~TrunkController() {
    try {
        if (this->write_since_last_flush) this->flush();
        this->unlink(this->last_trunk);
        if (this->trunk_link_head != nullptr) {
            fprintf(stderr, "Warning : trunk controller is not empty!\n");
            TrunkLinkInfo *p = this->trunk_link_head;
            while (p != NULL) {
                fprintf(stderr, "Trunk id %d, ref: %d\n", p->trunk_id, p->ref);
                p = p->next;
            }
        }
        
    } catch (KaraStorageException e) {
        fprintf(stderr, "Catched exception while shutting down:\n%s\n", e.msg.c_str());
    }
    this->stop = true;
    this->load_thread->join();
    delete this->load_thread;
    delete[] this->current_prepare_buffer;
}

void TrunkController::load_process() {
    IORequest r;
    while (!this->stop) {
        if (this->io_queue.try_dequeue(r)) {
            
        }
        else {
            std::this_thread::yield();
            continue;
        }

        try {
            if (r.op == IORequest_WRITE) {
                this->storage->write(r.trunk_id, r.data, r.length);
                this->unlink_info(r.info);
            } else if (r.op == IORequest_PREPARE) {
                if (r.trunk_id != this->current_prepare_trunk_id || this->prepare_done) {
                    // prepare changed
                    continue;
                }
                this->storage->read(r.trunk_id, r.data);
                prepare_lock.lock();
                if (r.trunk_id == this->current_prepare_trunk_id) this->prepare_done = true;
                prepare_lock.unlock();
            }
        } catch (KaraStorageException e) {
            this->err = new KaraStorageException(e);
        }
    }
}

void TrunkController::submit_write_buffer() {
    if (this->err != NULL) {
        throw *(this->err);
    }
    this->io_queue.enqueue(IORequest{
        IORequest_WRITE,
        this->num_trunks - 1,
        this->max_trunk_size,
        this->last_trunk.data,
        this->last_trunk.link_info
    });
    this->num_trunks ++;
    this->last_buffer_pos = 0;
    this->last_trunk.link_info->ref ++;

    this->relink(this->num_trunks - 1, this->last_trunk);
}

uint64_t TrunkController::append(const void* data, const uint32_t length) {
    if (length > this->max_trunk_size) throw KaraStorageException("Data is too large");
    if (length + this->last_buffer_pos > this->max_trunk_size) {
        // new trunk
        this->submit_write_buffer();
    }
    memcpy((uint8_t*)(this->last_trunk.data) + this->last_buffer_pos, data, length );
    this->last_buffer_pos += length;

    uint64_t ret = ((uint64_t(this->num_trunks - 1)) << uint64_t(32)) | uint64_t(this->last_buffer_pos);
    this->write_since_last_flush = true;
    if (this->last_buffer_pos == this->max_trunk_size) {
        this->submit_write_buffer();
    }
    return ret;
}


void TrunkController::prepare_trunk(uint32_t trunk_id) {
    if (this->current_prepare_trunk_id != trunk_id) {
        prepare_lock.lock();
        this->current_prepare_trunk_id = trunk_id;
        this->prepare_done = false;
        prepare_lock.unlock();
        this->io_queue.enqueue(IORequest{
            IORequest_PREPARE,
            trunk_id,
            this->max_trunk_size,
            this->current_prepare_buffer,
            NULL
        });
    }
}

TrunkView TrunkController::link(const uint32_t trunk_id) {
    TrunkLinkInfo *p = this->trunk_link_head;
    while (p != NULL) {
        if (p->trunk_id == trunk_id) break;
        p = p->next;
    }
    if (p != NULL) {
        // found exist link
        p->ref ++;
        return TrunkView(trunk_id, p->data, this->max_trunk_size, p);
    }
    
    if (trunk_id + 1 == this->num_trunks) {
        // special case:
        // 1. first init
        // 2. create new trunk
        TrunkLinkInfo *info = new TrunkLinkInfo;
        info->data = new uint8_t[ this->max_trunk_size ];
        info->next = this->trunk_link_head;
        info->ref = 1;
        info->trunk_id = trunk_id;

        this->trunk_link_head = info;

        memset(info->data, 0, this->max_trunk_size * sizeof(uint8_t));
        if (this->last_buffer_pos != 0) {
            this->storage->pread(trunk_id, 0, this->last_buffer_pos, info->data);
        }
        return TrunkView(trunk_id, info->data, this->max_trunk_size, info);
    }
    else if (trunk_id >= 0 && trunk_id + 1 < this->num_trunks) {
        TrunkLinkInfo *info = new TrunkLinkInfo;
        info->data = new uint8_t[ this->max_trunk_size ];
        info->next = this->trunk_link_head;
        info->ref = 1;
        info->trunk_id = trunk_id;

        this->trunk_link_head = info;

        // lock to check here
        if (this->current_prepare_trunk_id == trunk_id) {
            while (!this->prepare_done) {
                std::this_thread::yield(); // wait to be done
            }
            memcpy(info->data, this->current_prepare_buffer, this->max_trunk_size);
        } else {
            this->storage->read(trunk_id, info->data);
        }

        if (trunk_id + 2 < this->num_trunks) {
            // prepare next trunk
            this->prepare_trunk( trunk_id + 1 );
        }
        return TrunkView(trunk_id, info->data, this->max_trunk_size, info);
    } else {
        throw KaraStorageException("Invalid trunk id %d (should in range 0 ~ %d)", trunk_id, this->num_trunks - 1);
    }
}

void TrunkController::unlink_info(TrunkLinkInfo* link_info) {
    this->unlink_lock.lock();
    link_info->ref --;

    if (link_info->ref == 0) {
        // remove link info
        if (this->trunk_link_head == NULL) {
            this->unlink_lock.unlock();
            throw KaraStorageException("Unknown error : decr ref");
        }
        if (link_info == this->trunk_link_head) {
            this->trunk_link_head = link_info->next;
        } else {
            TrunkLinkInfo *p = this->trunk_link_head;
            while (p != NULL && p->next != link_info) {
                p = p->next;
            }
            if (p == NULL) {
                this->unlink_lock.unlock();
                throw KaraStorageException("Unknown error : decr ref NULL ptr");
            }
            else p->next = link_info->next;
        }
        this->unlink_lock.unlock();
        delete[] link_info->data;
    }
    else this->unlink_lock.unlock();
}

void TrunkController::unlink(TrunkView v) {
    this->unlink_info(v.link_info);
}


void  TrunkController::relink(const uint32_t trunk_id, TrunkView& v) {
    this->unlink(v);
    TrunkView vv = this->link(trunk_id);
    v.data = vv.data;
    v.length = vv.length;
    v.link_info = vv.link_info;
    v.trunk_id = trunk_id;
}

void TrunkController::flush() {
    while (this->io_queue.size_approx() > 0) {
        std::this_thread::yield();
    }
    this->storage->write(this->num_trunks - 1, this->last_trunk.data, this->last_buffer_pos);
    this->storage->flush();
    this->write_since_last_flush = false;
}


void TrunkController::pread(void* dest, uint32_t trunk_id, uint32_t offset, uint32_t length) {
    this->storage->pread(trunk_id, offset, length, dest);
}


uint32_t TrunkController::get_num_trunks() const { return this->num_trunks; };
uint32_t TrunkController::last_trunk_size() const { return this->last_buffer_pos; };