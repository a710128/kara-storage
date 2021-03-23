#define _POSIX_C_SOURCE  200112L
#include "local_trunk_controller.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "exceptions.h"
#include <cstring>
#include <sys/mman.h>

#define USE_HUGE_TLB 0 // (MAP_HUGETLB | (22 << MAP_HUGE_SHIFT))


/* constrcutor & destructor */
LocalTrunkController::LocalTrunkController(int base_dir_fd, bool writable, uint32_t max_trunk_size, uint32_t trunks_per_file) : 
    base_dir_fd(base_dir_fd), max_trunk_size(max_trunk_size), trunks_per_file(trunks_per_file), writable(writable) {

    this->trunk_size_pow = 0;
    while ( (1u << this->trunk_size_pow) < this->max_trunk_size ) this->trunk_size_pow ++;
    if (max_trunk_size != (1u << this->trunk_size_pow)  ) {
        throw KaraStorageException("max_trunk_size is not an exponential of 2");
    }
    this->max_file_size = this->max_trunk_size * this->trunks_per_file;
    
    if (faccessat(this->base_dir_fd, "meta", F_OK, 0) == -1) {
        // init dataset
        this->meta_fd = openat(this->base_dir_fd, "meta", (this->writable ? O_RDWR : O_RDONLY) | O_CREAT, 0755);
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
        sprintf(tmp_buffer, "%d.block", num_files);

        if (faccessat(this->base_dir_fd, tmp_buffer, F_OK, 0) == -1) break;
        num_files ++;
    }
    if (num_files <= 0) throw KaraStorageException("Broken dataset");
    this->num_trunks = (num_files - 1) * this->trunks_per_file + ( this->last_file_size >> this->trunk_size_pow ) + 1;
    
    if (this->writable) {
        this->current_fd = this->open_file((this->num_trunks - 1) / this->trunks_per_file, true);
        off_t v = lseek(this->current_fd, this->last_file_size, SEEK_SET);
        
#ifdef __linux__
        posix_fadvise(this->current_fd, v, this->max_file_size - v, POSIX_FADV_DONTNEED);
#endif
    }
    this->bytes_since_last_flush = 0;
}

LocalTrunkController::~LocalTrunkController() {
    this->flush();

    if (this->writable) {
        // munmap(this->write_mem_map, this->max_trunk_size);
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
#ifdef __linux__
    posix_fallocate(fd, 0, this->max_file_size);
#else
    fallocate(fd, 0, 0, this->max_file_size);
#endif
    return fd;
}

int LocalTrunkController::open_file(uint32_t file_id, bool writable) {
    char tmp_buffer[24];
    sprintf(tmp_buffer, "%d.block", file_id);
    int fd = openat(this->base_dir_fd, tmp_buffer, writable ? O_RDWR : O_RDONLY );
    if (fd == -1) throw KaraStorageException("Failed to open local storage file id: %d (errno: %d)", file_id, errno);
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
        this->quick_flush();

        if (this->num_trunks % this->trunks_per_file == 0) {
            // create new file here

            // close old files
            close(this->current_fd);

            // init new file
            this->current_fd = this->init_file(this->num_trunks / this->trunks_per_file);
#ifdef __linux__
            posix_fadvise(this->current_fd, 0, this->max_file_size, POSIX_FADV_DONTNEED);
#endif
        }

        lseek(this->current_fd, (this->num_trunks % this->trunks_per_file) << this->trunk_size_pow, SEEK_SET);

        this->last_file_size = (this->num_trunks % this->trunks_per_file) << this->trunk_size_pow;
        this->num_trunks ++;
        this->write_last_file_size();
        last_position = 0;
    }
    
    //memcpy((char*)this->write_mem_map + last_position, data, length);
    // write(this->current_fd, data, length);
    if (this->bytes_since_last_flush + length >= FLUSH_INTERVAL) {
        this->quick_flush();
    }
    memcpy(this->buffer + this->bytes_since_last_flush, data, length);
    this->bytes_since_last_flush += length;
    this->last_file_size += length;
    last_position += length;

    // exactly at the end
    if (last_position == this->max_trunk_size) {
        // new page here
        // create new trunk here
        this->quick_flush();

        if (this->num_trunks % this->trunks_per_file == 0) {
            // create new file here

            // close old files
            close(this->current_fd);

            // init new file
            this->current_fd = this->init_file(this->num_trunks / this->trunks_per_file);
#ifdef __linux__
            posix_fadvise(this->current_fd, 0, this->max_file_size, POSIX_FADV_DONTNEED);
#endif
        }

        lseek(this->current_fd, (this->num_trunks % this->trunks_per_file) << this->trunk_size_pow, SEEK_SET);

        this->last_file_size = (this->num_trunks % this->trunks_per_file) << this->trunk_size_pow;
        this->num_trunks ++;
        this->write_last_file_size();
        last_position = 0;
    }
    
    return ((uint64_t(this->num_trunks - 1)) << uint64_t(32)) | uint64_t(last_position);
}

TrunkView LocalTrunkController::link(const uint32_t trunk_id) {
    if (trunk_id >= this->num_trunks) throw KaraStorageException("Trunk id out of range");
    if ( trunk_id + 1 == this->num_trunks && this->bytes_since_last_flush > 0) {
        this->quick_flush();
    }
    int fd = this->open_file( trunk_id / this->trunks_per_file );
    void* mem = malloc( this->max_trunk_size );
    uint32_t offset = (trunk_id % this->trunks_per_file) << this->trunk_size_pow;
    lseek(fd, offset, SEEK_SET);
    if(read(fd, mem, this->max_trunk_size) == -1) throw KaraStorageException("Failed to read data trunk");
#ifdef __linux__
    uint32_t next_offset = offset + this->max_trunk_size;
    if ( next_offset < this->max_file_size ) {
        posix_fadvise(fd, next_offset, this->max_file_size - next_offset, POSIX_FADV_WILLNEED);
    }
#endif
    return TrunkView(fd, trunk_id, mem, this->max_trunk_size);
}

void LocalTrunkController::relink(const uint32_t trunk_id, TrunkView &v) {
    if (trunk_id >= this->num_trunks) throw KaraStorageException("Trunk id out of range");
    if ( trunk_id + 1 == this->num_trunks && this->bytes_since_last_flush > 0) {
        this->quick_flush();
    }
    uint32_t old_trunk_file = v.trunk_id / this->trunks_per_file ;
    uint32_t nw_trunk_file = trunk_id / this->trunks_per_file;
    if (old_trunk_file != nw_trunk_file) {
        // open new fd
        close(v.fd);
        v.fd = this->open_file( nw_trunk_file );
    } else {
        // reuse fd
    }
    v.trunk_id = trunk_id;
    uint32_t offset = (trunk_id % this->trunks_per_file) << this->trunk_size_pow;
    lseek(v.fd, offset, SEEK_SET);
    if (read(v.fd, v.data, this->max_trunk_size) == -1) throw KaraStorageException("Failed to read data trunk");
#ifdef __linux__
    uint32_t next_offset = offset + this->max_trunk_size;
    if ( next_offset < this->max_file_size ) {
        posix_fadvise(v.fd, next_offset, this->max_file_size - next_offset, POSIX_FADV_WILLNEED);
    }
#endif
}

void LocalTrunkController::unlink(TrunkView v) {
    if (v.data != NULL) {
        free(v.data);
        v.data = NULL;
    }
    close(v.fd);
}
void LocalTrunkController::flush() {
    if (!this->writable) return;
    this->quick_flush();
    fsync(this->current_fd);
    fsync(this->meta_fd);
}

void LocalTrunkController::quick_flush() {
    if (!this->writable) return;
    this->write_last_file_size();
    /*
    if (msync(this->write_mem_map, this->max_trunk_size, MS_ASYNC) == -1) {
        throw KaraStorageException("Failed to flush data");
    }
    */
    if(write(this->current_fd, this->buffer, this->bytes_since_last_flush) == -1) throw KaraStorageException("Failed to flush write buffer");
    this->bytes_since_last_flush = 0;
}


void LocalTrunkController::pread(void* dest, uint32_t trunk_id, uint32_t offset, uint32_t length) {
    if ( trunk_id + 1 == this->num_trunks && this->bytes_since_last_flush > 0) {
        this->quick_flush();
    }
    int fd = this->open_file( trunk_id / this->trunks_per_file );
    offset += (trunk_id % this->trunks_per_file) << this->trunk_size_pow;
    lseek(fd, offset, SEEK_SET);
    if (read(fd, dest, length) == -1) throw KaraStorageException("Failed to read data in trunk %d", trunk_id);
    close(fd);
}

uint32_t LocalTrunkController::get_num_trunks() const {
    return this->num_trunks;
}