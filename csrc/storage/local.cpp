#include "storage/local_storage.h"
#include "exceptions.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

void LocalStorage::write_meta() {
    lseek(this->meta_fd, 0, SEEK_SET);
    if (::write(this->meta_fd, &(this->meta), sizeof(LocalMetaInfo)) == -1) {
        throw KaraStorageException("Failed to write meta info");
    }
}

void LocalStorage::read_meta() {
    lseek(this->meta_fd, 0, SEEK_SET);
    if (::read(this->meta_fd, &(this->meta), sizeof(LocalMetaInfo)) == -1) {
        throw KaraStorageException("Failed to read meta info");
    }
}

LocalStorage::LocalStorage(int base_dir_fd, uint32_t max_trunk_size, uint32_t trunks_per_file) : base_dir_fd(base_dir_fd) {
    if (faccessat(this->base_dir_fd, "meta", F_OK, 0) == -1) {
        this->meta_fd = openat(this->base_dir_fd, "meta", O_RDWR | O_CREAT, 0755);
        if (this->meta_fd == -1) throw KaraStorageException("Failed to intialize database");
        this->meta = {1, 0, max_trunk_size, trunks_per_file};
        this->write_meta();
    } else {
        this->meta_fd = openat(this->base_dir_fd, "meta", O_RDWR, 0755);
        if (this->meta_fd == -1) throw KaraStorageException("Failed to intialize database");
    }
    this->read_meta();
}

LocalStorage::~LocalStorage() {
    fsync(this->meta_fd);
    close(this->meta_fd);
}


void LocalStorage::read(uint32_t trunk_id, void* dest) {
    if (trunk_id >= this->meta.num_trunks) throw KaraStorageException("Trunk_id error : %d (max: %d)", trunk_id, this->meta.num_trunks - 1);
    uint32_t file_id = trunk_id / this->meta.trunks_per_file;
    uint32_t in_trunk_offset = (trunk_id % this->meta.trunks_per_file) * this->meta.max_trunk_size;
    char buffer[32];
    sprintf(buffer, "%d.block", file_id);
    int fd = openat(this->base_dir_fd, buffer, O_RDONLY);
    if (fd == -1) throw KaraStorageException("Failed to open trunk %d", trunk_id);
    lseek(fd, in_trunk_offset, SEEK_SET);
    if (::read(fd, dest, this->meta.max_trunk_size) == -1) {
        throw KaraStorageException("Failed to read trunk %d", trunk_id);
    }
    close(fd);
}

uint32_t LocalStorage::get_trunk_size() const {
    return this->meta.max_trunk_size;
}
uint32_t LocalStorage::get_trunk_num() const {
    return this->meta.num_trunks;
}
uint32_t LocalStorage::last_trunk_size() const {
    return this->meta.last_trunk_size;
}


void LocalStorage::write(uint32_t trunk_id, const void * src, uint32_t length) {
    if (this->meta.num_trunks != trunk_id && this->meta.num_trunks != trunk_id + 1) {
        // need trunk_id == next_trunk or trunk_id == last_trunk
        throw KaraStorageException("Trunk_id error : %d (need: %d)", trunk_id, this->meta.num_trunks);
    }
    uint32_t file_id = trunk_id / this->meta.trunks_per_file;
    uint32_t in_trunk_offset = (trunk_id % this->meta.trunks_per_file) * this->meta.max_trunk_size;

    char buffer[32];
    sprintf(buffer, "%d.block", file_id);
    int fd = -1;
    if (faccessat(this->base_dir_fd, buffer, F_OK, 0) == -1) {
        fd = openat(this->base_dir_fd, buffer, O_RDWR | O_CREAT , 0755);
        if (fallocate(fd, 0, 0, this->meta.max_trunk_size * this->meta.trunks_per_file) == -1) {
            throw KaraStorageException("Failed to create trunk %d", trunk_id);
        }
    } else {
        fd = openat(this->base_dir_fd, buffer, O_RDWR);
    }
    if (fd == -1) throw KaraStorageException("Failed to open trunk %d", trunk_id);
    lseek(fd, in_trunk_offset, SEEK_SET);
    if (::write(fd, src, length) == -1) {
        throw KaraStorageException("Failed to write trunk %d (errno: %d)", trunk_id, errno);
    }
    close(fd);
    if (this->meta.num_trunks == trunk_id) {
        // write next trunk
        this->meta.num_trunks ++;
        this->meta.last_trunk_size = length;
    } else {
        // overwrite last trunk
        this->meta.last_trunk_size = length;
    }
    this->write_meta();
}

void LocalStorage::pread(uint32_t trunk_id, uint32_t offset, uint32_t length, void* dest) {
    if (trunk_id >= this->meta.num_trunks) throw KaraStorageException("Trunk_id error : %d (max: %d)", trunk_id, this->meta.num_trunks - 1);
    if (offset < 0 || offset >= this->meta.max_trunk_size) throw KaraStorageException("Invalid offset %d ( should in range %d ~ %d )", offset, 0, this->meta.max_trunk_size);
    uint32_t file_id = trunk_id / this->meta.trunks_per_file;
    uint32_t in_trunk_offset = (trunk_id % this->meta.trunks_per_file) * this->meta.max_trunk_size + offset;
    char buffer[32];
    sprintf(buffer, "%d.block", file_id);
    int fd = openat(this->base_dir_fd, buffer, O_RDONLY);
    if (fd == -1) throw KaraStorageException("Failed to open trunk %d", trunk_id);
    lseek(fd, in_trunk_offset, SEEK_SET);
    if (::read(fd, dest, length) == -1) {
        throw KaraStorageException("Failed to read trunk %d", trunk_id);
    }
    close(fd);
}

void LocalStorage::flush() {
    this->write_meta();
}