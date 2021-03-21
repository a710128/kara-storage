#define _POSIX_C_SOURCE  200112L
#include "local_trunk_controller.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "exceptions.h"
#include <cstring>
#include <sys/mman.h>


/* constrcutor & destructor */
LocalTrunkController::LocalTrunkController(int base_dir_fd, bool writable, uint32_t max_trunk_size, uint32_t trunks_per_file) : 
    base_dir_fd(base_dir_fd), max_trunk_size(max_trunk_size), trunks_per_file(trunks_per_file), writable(writable) {

    this->trunk_size_pow = 0;
    while ( (1u << this->trunk_size_pow) < this->max_trunk_size ) this->trunk_size_pow ++;
    if (max_trunk_size != (1u << this->trunk_size_pow)  ) {
        throw KaraStorageException("max_trunk_size is not an exponential of 2");
    }
    
    
    if (faccessat(this->base_dir_fd, "meta", F_OK, 0) == -1) {
        // init dataset
        this->meta_fd = openat(this->base_dir_fd, "meta", (this->writable ? O_RDWR : O_RDONLY) | O_CREAT);
        if (this->meta_fd == -1) throw KaraStorageException("Failed to intialize database");

        this->last_file_size = 0;
        this->write_last_file_size();

        close( this->init_file(0) );
    } else {
        // open meta
        this->meta_fd = openat(this->base_dir_fd, "meta", this->writable ? O_RDWR : O_RDONLY);
        if (this->meta_fd == -1) throw KaraStorageException("Failed to open meta file");
    }

    struct flock f_lock;
    f_lock.l_len = 0;
    f_lock.l_pid = getpid();
    f_lock.l_start = 0;
    f_lock.l_whence = 0;
    f_lock.l_type = this->writable ? F_WRLCK : F_RDLCK;
    if (fcntl(this->meta_fd, F_SETLK, &f_lock) == -1) {
        throw KaraStorageException("Failed to lock dataset");
    }

    this->read_last_file_size();


    char tmp_buffer[24];
    
    int num_files = 0;
    while (true) {
        sprintf(tmp_buffer, "%d.block", num_files ++);

        if (faccessat(this->base_dir_fd, tmp_buffer, F_OK, 0) == -1) break;
    }
    if (num_files <= 0) throw KaraStorageException("Broken dataset");
    this->num_trunks = (num_files - 1) * this->trunks_per_file + ( this->last_file_size >> this->trunk_size_pow ) + 1;
    
    if (this->writable) {
        this->current_fd = this->open_file((this->num_trunks - 1) / this->trunks_per_file, true);
        this->write_mem_map = mmap(NULL, this->max_trunk_size, PROT_WRITE, MAP_SHARED | MAP_LOCKED, this->current_fd,  ((this->num_trunks - 1) % this->trunks_per_file) << this->trunk_size_pow);
        if (this->write_mem_map == MAP_FAILED) throw KaraStorageException("Failed to map data");

        this->bytes_since_last_flush = 0;
    }
}

LocalTrunkController::~LocalTrunkController() {
    this->flush();

    if (this->writable) {
        munmap(this->write_mem_map, this->max_trunk_size);
        close(this->current_fd);
    }

    struct flock f_lock;
    f_lock.l_len = 0;
    f_lock.l_pid = getpid();
    f_lock.l_start = 0;
    f_lock.l_whence = 0;
    f_lock.l_type = F_UNLCK;
    fcntl(this->meta_fd, F_SETLK, &f_lock);
    close(this->meta_fd);
}

/* private */

void LocalTrunkController::read_last_file_size() {
    lseek(this->meta_fd, 0, SEEK_SET);
    if (read(this->meta_fd, &(this->last_file_size), sizeof(this->last_file_size)) == -1) {
        throw KaraStorageException("Failed to read meta file");
    }
}

void LocalTrunkController::write_last_file_size() {
    if (!this->writable) throw KaraStorageException("Not writable");
    lseek(this->meta_fd, 0, SEEK_SET);
    if (write(this->meta_fd, &(this->last_file_size), sizeof(this->last_file_size)) == -1) {
        throw KaraStorageException("Failed to write meta file");
    }
}

int LocalTrunkController::init_file(uint32_t file_id) {
    if (!this->writable) throw KaraStorageException("Not writable");
    char tmp_buffer[24];
    sprintf(tmp_buffer, "%d.block", file_id);
    int fd = openat(this->base_dir_fd, tmp_buffer, O_RDWR | O_CREAT, 0755);
    if (fd == -1) throw KaraStorageException("Failed to create local storage file");
    posix_fallocate(fd, 0, this->max_trunk_size * this->trunks_per_file);
    return fd;
}

int LocalTrunkController::open_file(uint32_t file_id, bool writable) {
    char tmp_buffer[24];
    sprintf(tmp_buffer, "%d.block", file_id);
    int fd = openat(this->base_dir_fd, tmp_buffer, writable ? O_RDWR : O_RDONLY );
    if (fd == -1) throw KaraStorageException("Failed to open local storage file");
    return fd;
}

inline uint32_t  LocalTrunkController::last_trunk_size() const {
    return this->last_file_size & ((1u << this->trunk_size_pow) - 1);
}

/* public */

uint64_t LocalTrunkController::append(const void* data, const uint32_t length) {
    if (!this->writable) throw KaraStorageException("Not writable");
    uint32_t last_position = this->last_trunk_size();
    if (last_position + length > this->max_trunk_size) {
        // create new trunk here
        this->flush();

        // unmap old file
        if (munmap(this->write_mem_map, this->max_trunk_size) == -1) {
            throw KaraStorageException("Failed to unmap old data");
        }

        if (this->num_trunks % this->trunks_per_file == 0) {
            // create new file here

            // close old files
            close(this->current_fd);

            // init new file
            this->current_fd = this->init_file(this->num_trunks / this->trunks_per_file);
        }

        this->write_mem_map = mmap(NULL, this->max_trunk_size, PROT_WRITE, MAP_SHARED | MAP_LOCKED, this->current_fd,  (this->num_trunks % this->trunks_per_file) << this->trunk_size_pow);
        if (this->write_mem_map == MAP_FAILED) throw KaraStorageException("Failed to map data");
        this->last_file_size = (this->num_trunks % this->trunks_per_file) << this->trunk_size_pow;
        this->num_trunks ++;
        this->write_last_file_size();
        last_position = 0;
    }
    memcpy((char*)this->write_mem_map + last_position, data, length);
    this->last_file_size += length;
    last_position += length;
    this->bytes_since_last_flush += length;
    if (this->bytes_since_last_flush >= FLUSH_INTERVAL) this->flush();
    return ((uint64_t(this->num_trunks - 1)) << uint64_t(32)) | uint64_t(last_position);
}

TrunkView LocalTrunkController::link(const uint32_t trunk_id) {
    if ( trunk_id + 1 == this->num_trunks && this->bytes_since_last_flush > 0) {
        this->flush();
    }
    int fd = this->open_file( trunk_id / this->trunks_per_file );
    void* mem = mmap(NULL, this->max_trunk_size, PROT_READ, MAP_SHARED | MAP_LOCKED | MAP_POPULATE, fd, (trunk_id % this->trunks_per_file) << this->trunk_size_pow );
    if (mem == MAP_FAILED) throw KaraStorageException("Failed to map local storage data");
    return TrunkView(fd, mem, this->max_trunk_size);
}

void LocalTrunkController::unlink(TrunkView v) {
    if (munmap(v.data, this->max_trunk_size) == -1) {
        throw KaraStorageException("Failed to unmap local storage data");
    }
    close(v.fd);
}

void LocalTrunkController::flush() {
    if (!this->writable) return;
    this->write_last_file_size();
    if (msync(this->write_mem_map, this->max_trunk_size, MS_ASYNC) == -1) {
        throw KaraStorageException("Failed to flush data");
    }
    this->bytes_since_last_flush = 0;
}


void LocalTrunkController::pread(void* dest, uint32_t trunk_id, uint32_t offset, uint32_t length) {
    if ( trunk_id + 1 == this->num_trunks && this->bytes_since_last_flush > 0) {
        this->flush();
    }
    int fd = this->open_file( trunk_id / this->trunks_per_file );
    offset += (trunk_id % this->trunks_per_file) << this->trunk_size_pow;
    lseek(fd, offset, SEEK_SET);
    read(fd, dest, length);
    close(fd);
}

uint32_t LocalTrunkController::get_num_trunks() const {
    return this->num_trunks;
}