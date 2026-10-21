/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#define _GNU_SOURCE
#define FUSE_USE_VERSION 31
#include <fuse3/fuse.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/statvfs.h>
#include <time.h>
#include <unistd.h>

/* Original, deliberately small test filesystem; not a general-purpose passthrough.
 * No caller can select a backing directory or mountpoint, or create links/devices.
 * Every pathname walk is relative to the isolated backing root, without symlinks.
 */
static int backing_fd;
static int control_fd;
static pthread_mutex_t event_lock = PTHREAD_MUTEX_INITIALIZER;

static int result(int value) {
    return value == -1 ? -errno : value;
}

/* Return an owned parent fd and a single leaf, never following a symlink. */
static int parent_fd(const char *path, char leaf[NAME_MAX + 1]) {
    if (path == NULL || path[0] != '/' || strlen(path) >= PATH_MAX) {
        return -EINVAL;
    }
    int fd = dup(backing_fd);
    if (fd == -1) {
        return -errno;
    }
    const char *part = path + 1;
    if (*part == '\0') {
        strcpy(leaf, ".");
        return fd;
    }
    for (;;) {
        const char *end = strchr(part, '/');
        size_t size = end == NULL ? strlen(part) : (size_t) (end - part);
        if (size == 0 || size > NAME_MAX || (size == 1 && part[0] == '.')
                || (size == 2 && part[0] == '.' && part[1] == '.')) {
            close(fd);
            return -EINVAL;
        }
        memcpy(leaf, part, size);
        leaf[size] = '\0';
        if (end == NULL) {
            return fd;
        }
        int next = openat(fd, leaf, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        int saved = errno;
        close(fd);
        if (next == -1) {
            return -saved;
        }
        fd = next;
        part = end + 1;
    }
}

static int open_path(const char *path, int flags, mode_t mode) {
    char leaf[NAME_MAX + 1];
    int parent = parent_fd(path, leaf);
    if (parent < 0) {
        return parent;
    }
    int fd = openat(parent, leaf, flags | O_NOFOLLOW | O_CLOEXEC, mode);
    int saved = errno;
    close(parent);
    return fd == -1 ? -saved : fd;
}

static int journal_path(const char *path) {
    size_t length = strlen(path);
    return length > 4 && strcmp(path + length - 4, ".txn") == 0;
}

static int read_mode(char mode[16]) {
    int fd = openat(control_fd, "mode", O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd == -1) {
        return -errno;
    }
    ssize_t count = read(fd, mode, 15);
    int saved = errno;
    close(fd);
    if (count < 0) {
        return -saved;
    }
    mode[count] = '\0';
    while (count > 0 && (mode[count - 1] == '\n' || mode[count - 1] == '\r')) {
        mode[--count] = '\0';
    }
    if (strcmp(mode, "healthy") != 0 && strcmp(mode, "readonly") != 0
            && strcmp(mode, "eio") != 0 && strcmp(mode, "stall") != 0) {
        return -EINVAL;
    }
    return 0;
}

/* Evidence lives outside the faulty filesystem, with a completed fsync before
 * returning an error or beginning a stall. Mutex is never held during a stall.
 */
static int event(const char *op, const char *path, const char *mode, const char *state) {
    char line[PATH_MAX + 128];
    int length = snprintf(line, sizeof(line), "op=%s path=%s mode=%s state=%s\n", op, path, mode, state);
    if (length < 0 || (size_t) length >= sizeof(line)) {
        return -EOVERFLOW;
    }
    pthread_mutex_lock(&event_lock);
    int fd = openat(control_fd, "events", O_WRONLY | O_APPEND | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0644);
    int error = fd == -1 ? -errno : 0;
    size_t written = 0;
    while (error == 0 && written < (size_t) length) {
        ssize_t count = write(fd, line + written, (size_t) length - written);
        if (count == -1 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            error = count == -1 ? -errno : -EIO;
        } else {
            written += (size_t) count;
        }
    }
    if (error == 0) {
        error = result(fsync(fd));
    }
    if (fd != -1) {
        close(fd);
    }
    pthread_mutex_unlock(&event_lock);
    return error;
}

/* A read-only transition rejects mutations on the entire journal filesystem,
 * including already-open descriptors. It is EROFS, not a chmod approximation;
 * it does not claim to emulate a block device or a physical filesystem remount.
 */
static int readonly_fault(const char *op, const char *path) {
    char mode[16];
    int error = read_mode(mode);
    if (error != 0 || strcmp(mode, "readonly") != 0) {
        return error;
    }
    if (journal_path(path)) {
        error = event(op, path, mode, "INJECTED");
    }
    return error != 0 ? error : -EROFS;
}

static int fault(const char *op, const char *path) {
    /* Disk-check probes and metadata operations must never count as evidence. */
    if (!journal_path(path)) {
        return readonly_fault(op, path);
    }
    char mode[16];
    int error = read_mode(mode);
    if (error != 0 || strcmp(mode, "healthy") == 0) {
        return error;
    }
    error = event(op, path, mode, "INJECTED");
    if (error != 0) {
        return error;
    }
    if (strcmp(mode, "eio") == 0) {
        return -EIO;
    }
    if (strcmp(mode, "readonly") == 0) {
        return -EROFS;
    }
    struct timespec start;
    if (clock_gettime(CLOCK_MONOTONIC, &start) == -1) {
        return -errno;
    }
    for (;;) {
        struct timespec now;
        if (clock_gettime(CLOCK_MONOTONIC, &now) == -1) {
            return -errno;
        }
        if (now.tv_sec - start.tv_sec > 60
                || (now.tv_sec - start.tv_sec == 60 && now.tv_nsec >= start.tv_nsec)) {
            /* Expiration is failure evidence, never an implicit healthy recovery. */
            error = event(op, path, "stall", "EXPIRED");
            fprintf(stderr, "EXPIRED op=%s path=%s mode=stall\n", op, path);
            fflush(stderr);
            return error != 0 ? error : -ETIMEDOUT;
        }
        error = read_mode(mode);
        if (error != 0) {
            return error;
        }
        if (strcmp(mode, "healthy") == 0) {
            return 0;
        }
        if (strcmp(mode, "eio") == 0 || strcmp(mode, "readonly") == 0) {
            error = event(op, path, mode, "INJECTED");
            return error != 0 ? error : (strcmp(mode, "eio") == 0 ? -EIO : -EROFS);
        }
        const struct timespec pause = { .tv_sec = 0, .tv_nsec = 100000000 };
        nanosleep(&pause, NULL);
    }
}

static void *fs_init(struct fuse_conn_info *connection, struct fuse_config *config) {
    connection->want &= ~FUSE_CAP_WRITEBACK_CACHE;
    config->kernel_cache = 0;
    config->auto_cache = 0;
    config->direct_io = 1;
    config->entry_timeout = 0;
    config->attr_timeout = 0;
    config->negative_timeout = 0;
    return NULL;
}

static int fs_getattr(const char *path, struct stat *status, struct fuse_file_info *info) {
    if (info != NULL) {
        return result(fstat((int) info->fh, status));
    }
    char leaf[NAME_MAX + 1];
    int parent = parent_fd(path, leaf);
    if (parent < 0) {
        return parent;
    }
    int value = result(fstatat(parent, leaf, status, AT_SYMLINK_NOFOLLOW));
    close(parent);
    return value == 0 && S_ISLNK(status->st_mode) ? -ELOOP : value;
}

static int fs_open(const char *path, struct fuse_file_info *info) {
    if ((info->flags & O_ACCMODE) != O_RDONLY || (info->flags & O_TRUNC) != 0) {
        int error = readonly_fault("open", path);
        if (error != 0) {
            return error;
        }
    }
    int fd = open_path(path, info->flags, 0);
    if (fd < 0) {
        return fd;
    }
    info->fh = (uint64_t) fd;
    info->direct_io = 1;
    info->keep_cache = 0;
    return 0;
}

static int fs_create(const char *path, mode_t mode, struct fuse_file_info *info) {
    int error = readonly_fault("create", path);
    if (error != 0) {
        return error;
    }
    int fd = open_path(path, info->flags | O_CREAT, mode);
    if (fd < 0) {
        return fd;
    }
    const struct fuse_context *context = fuse_get_context();
    if (fchown(fd, context->uid, context->gid) == -1) {
        int saved = errno;
        close(fd);
        return -saved;
    }
    info->fh = (uint64_t) fd;
    info->direct_io = 1;
    info->keep_cache = 0;
    return 0;
}

static int fs_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *info) {
    (void) path;
    ssize_t count = pread((int) info->fh, buffer, size, offset);
    return count == -1 ? -errno : (int) count;
}

static int fs_write(const char *path, const char *buffer, size_t size, off_t offset,
                    struct fuse_file_info *info) {
    int error = fault("write", path);
    if (error != 0) {
        return error;
    }
    ssize_t count = pwrite((int) info->fh, buffer, size, offset);
    return count == -1 ? -errno : (int) count;
}

static int fs_fsync(const char *path, int data_only, struct fuse_file_info *info) {
    int error = fault("fsync", path);
    return error != 0 ? error : result(data_only ? fdatasync((int) info->fh) : fsync((int) info->fh));
}

static int fs_flush(const char *path, struct fuse_file_info *info) {
    (void) path;
    int fd = dup((int) info->fh);
    return fd == -1 ? -errno : result(close(fd));
}

static int fs_release(const char *path, struct fuse_file_info *info) {
    (void) path;
    return result(close((int) info->fh));
}

static int fs_readdir(const char *path, void *buffer, fuse_fill_dir_t fill, off_t offset,
                      struct fuse_file_info *info, enum fuse_readdir_flags flags) {
    (void) info;
    (void) flags;
    int fd = open_path(path, O_RDONLY | O_DIRECTORY, 0);
    if (fd < 0) {
        return fd;
    }
    DIR *directory = fdopendir(fd);
    if (directory == NULL) {
        int saved = errno;
        close(fd);
        return -saved;
    }
    seekdir(directory, offset);
    struct dirent *entry;
    int error = 0;
    for (;;) {
        errno = 0;
        entry = readdir(directory);
        if (entry == NULL) {
            error = -errno;
            break;
        }
        if (fill(buffer, entry->d_name, NULL, telldir(directory), 0) != 0) {
            break;
        }
    }
    closedir(directory);
    return error;
}

static int fs_mkdir(const char *path, mode_t mode) {
    int error = readonly_fault("mkdir", path);
    if (error != 0) {
        return error;
    }
    char leaf[NAME_MAX + 1];
    int parent = parent_fd(path, leaf);
    if (parent < 0) {
        return parent;
    }
    int value = result(mkdirat(parent, leaf, mode));
    if (value == 0) {
        const struct fuse_context *context = fuse_get_context();
        value = result(fchownat(parent, leaf, context->uid, context->gid, AT_SYMLINK_NOFOLLOW));
    }
    close(parent);
    return value;
}

static int fs_unlink(const char *path) {
    int error = readonly_fault("unlink", path);
    if (error != 0) {
        return error;
    }
    char leaf[NAME_MAX + 1];
    int parent = parent_fd(path, leaf);
    if (parent < 0) {
        return parent;
    }
    int value = result(unlinkat(parent, leaf, 0));
    close(parent);
    return value;
}

static int fs_rmdir(const char *path) {
    int error = readonly_fault("rmdir", path);
    if (error != 0) {
        return error;
    }
    char leaf[NAME_MAX + 1];
    int parent = parent_fd(path, leaf);
    if (parent < 0) {
        return parent;
    }
    int value = result(unlinkat(parent, leaf, AT_REMOVEDIR));
    close(parent);
    return value;
}

static int fs_rename(const char *from, const char *to, unsigned int flags) {
    int error = readonly_fault("rename", journal_path(from) ? from : to);
    if (error != 0) {
        return error;
    }
    if (flags != 0) {
        return -EINVAL;
    }
    char source[NAME_MAX + 1];
    char target[NAME_MAX + 1];
    int source_parent = parent_fd(from, source);
    if (source_parent < 0) {
        return source_parent;
    }
    int target_parent = parent_fd(to, target);
    if (target_parent < 0) {
        close(source_parent);
        return target_parent;
    }
    int value = result(renameat(source_parent, source, target_parent, target));
    close(source_parent);
    close(target_parent);
    return value;
}

static int fs_truncate(const char *path, off_t length, struct fuse_file_info *info) {
    int error = readonly_fault("truncate", path);
    if (error != 0) {
        return error;
    }
    if (info != NULL) {
        return result(ftruncate((int) info->fh, length));
    }
    int fd = open_path(path, O_WRONLY, 0);
    if (fd < 0) {
        return fd;
    }
    int value = result(ftruncate(fd, length));
    close(fd);
    return value;
}

static int fs_statfs(const char *path, struct statvfs *status) {
    (void) path;
    return result(fstatvfs(backing_fd, status));
}

static int fs_access(const char *path, int mode) {
    char leaf[NAME_MAX + 1];
    int parent = parent_fd(path, leaf);
    if (parent < 0) {
        return parent;
    }
    int value = result(faccessat(parent, leaf, mode, AT_SYMLINK_NOFOLLOW));
    close(parent);
    return value;
}

static int fs_chmod(const char *path, mode_t mode, struct fuse_file_info *info) {
    int error = readonly_fault("chmod", path);
    if (error != 0) {
        return error;
    }
    int fd = info == NULL ? open_path(path, O_RDONLY, 0) : (int) info->fh;
    if (fd < 0) {
        return fd;
    }
    int value = result(fchmod(fd, mode));
    if (info == NULL) {
        close(fd);
    }
    return value;
}

static int fs_chown(const char *path, uid_t user, gid_t group, struct fuse_file_info *info) {
    int error = readonly_fault("chown", path);
    if (error != 0) {
        return error;
    }
    int fd = info == NULL ? open_path(path, O_RDONLY, 0) : (int) info->fh;
    if (fd < 0) {
        return fd;
    }
    int value = result(fchown(fd, user, group));
    if (info == NULL) {
        close(fd);
    }
    return value;
}

static int fs_utimens(const char *path, const struct timespec times[2], struct fuse_file_info *info) {
    int error = readonly_fault("utimens", path);
    if (error != 0) {
        return error;
    }
    int fd = info == NULL ? open_path(path, O_RDONLY, 0) : (int) info->fh;
    if (fd < 0) {
        return fd;
    }
    int value = result(futimens(fd, times));
    if (info == NULL) {
        close(fd);
    }
    return value;
}

int main(int argc, char **argv) {
    (void) argv;
    if (argc != 1) {
        fprintf(stderr, "journal-faultfs takes no arguments; all paths are fixed\n");
        return 2;
    }
    int storage_fd = open("/storage", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (storage_fd == -1) {
        perror("open fixed storage root");
        return 1;
    }
    backing_fd = openat(storage_fd, "backing", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    control_fd = openat(storage_fd, "control", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    close(storage_fd);
    if (backing_fd == -1 || control_fd == -1) {
        perror("open isolated storage directories");
        return 1;
    }
    static const struct fuse_operations operations = {
        .init = fs_init,
        .getattr = fs_getattr,
        .open = fs_open,
        .create = fs_create,
        .read = fs_read,
        .write = fs_write,
        .fsync = fs_fsync,
        .flush = fs_flush,
        .release = fs_release,
        .readdir = fs_readdir,
        .mkdir = fs_mkdir,
        .unlink = fs_unlink,
        .rmdir = fs_rmdir,
        .rename = fs_rename,
        .truncate = fs_truncate,
        .statfs = fs_statfs,
        .access = fs_access,
        .chmod = fs_chmod,
        .chown = fs_chown,
        .utimens = fs_utimens,
    };
    /* Foreground inside the wrapper's background process; no -s: multithreaded
     * dispatch leaves metadata/read/control operations live during a stalled IO.
     */
    char *arguments[] = { "journal-faultfs", "-f", "-o",
        "allow_other,default_permissions,fsname=journal-faultfs", "/storage/journal" };
    int value = fuse_main(5, arguments, &operations, NULL);
    close(backing_fd);
    close(control_fd);
    return value;
}
