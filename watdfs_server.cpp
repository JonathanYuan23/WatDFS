#include "debug.h"
#include "rpc.h"
#include "rw_lock.h"
#include <ctime>
#include <fcntl.h>
#include <fuse/fuse.h>
INIT_LOG

#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <mutex>
#include <string>
#include <unordered_map>

// Global state server_persist_dir.
char *server_persist_dir = nullptr;

// Important: the server needs to handle multiple concurrent client requests.
// You have to be careful in handling global variables, especially for updating
// them. Hint: use locks before you update any global variable.

// ----------------------
// Global synchronization
// ----------------------

// Track writer exclusivity per pathname, count of open writers is either 0 or 1
static std::unordered_map<std::string, int> g_open_writers;

// Track whether a given server fd corresponds to a writer handle.
static std::unordered_map<int, bool> g_fd_is_writer;

// Map pathname to per-file reader-writer lock used for transfer atomicity.
static std::unordered_map<std::string, rw_lock_t *> g_locks;

// Protect access to the above maps.
static std::mutex g_state_mu;

// Helper to get or create a per-path rw_lock_t. Must be called with g_state_mu
// unlocked.
static rw_lock_t *get_or_create_lock_for_path(const std::string &path) {
  std::lock_guard<std::mutex> lk(g_state_mu);

  auto it = g_locks.find(path);
  if (it != g_locks.end())
    return it->second;

  rw_lock_t *lkp = (rw_lock_t *)malloc(sizeof(rw_lock_t));

  if (!lkp)
    return nullptr;

  if (rw_lock_init(lkp) != 0) {
    free(lkp);
    return nullptr;
  }

  g_locks.emplace(path, lkp);
  return lkp;
}

// We need to operate on the path relative to the server_persist_dir.
// This function returns a path that appends the given short path to the
// server_persist_dir. The character array is allocated on the heap, therefore
// it should be freed after use.
// Tip: update this function to return a unique_ptr for automatic memory
// management.
char *get_full_path(char *short_path) {
  int short_path_len = strlen(short_path);
  int dir_len = strlen(server_persist_dir);
  int full_len = dir_len + short_path_len + 1;

  char *full_path = (char *)malloc(full_len);

  // First fill in the directory.
  strcpy(full_path, server_persist_dir);
  // Then append the path.
  strcat(full_path, short_path);
  DLOG("Full path: %s\n", full_path);

  return full_path;
}

// The server implementation of getattr.
int watdfs_getattr(int *argTypes, void **args) {
  // Get the arguments.
  // The first argument is the path relative to the mountpoint.
  char *short_path = (char *)args[0];
  // The second argument is the stat structure, which should be filled in
  // by this function.
  struct stat *statbuf = (struct stat *)args[1];
  // The third argument is the return code, which should be set be 0 or -errno.
  int *ret = (int *)args[2];

  // Get the local file name, so we call our helper function which appends
  // the server_persist_dir to the given path.
  char *full_path = get_full_path(short_path);

  // Initially we set the return code to be 0.
  *ret = 0;

  // Let sys_ret be the return code from the stat system call.
  int sys_ret = stat(full_path, statbuf);

  if (sys_ret < 0) {
    // If there is an error on the system call, then the return code should
    // be -errno.
    *ret = -errno;
  }

  DLOG("watdfs_getattr, path: %s, ret: %d", full_path, *ret);

  // Clean up the full path, it was allocated on the heap.
  free(full_path);

  DLOG("Returning code: %d", *ret);
  // The RPC call succeeded, so return 0.
  return 0;
}

int watdfs_mknod(int *argTypes, void **args) {
  // Get the arguments.
  // The first argument is the path relative to the mountpoint.
  char *short_path = (char *)args[0];

  // The second argument is the mode argument
  mode_t *mode = (mode_t *)args[1];

  // The third argument is the dev argument
  dev_t *dev = (dev_t *)args[2];

  // Get the local file name, so we call our helper function which appends
  // the server_persist_dir to the given path.
  char *full_path = get_full_path(short_path);

  // The fourth argument is the return code, which should be set be 0 or -errno.
  int *ret = (int *)args[3];

  // Initially we set the return code to be 0.
  *ret = 0;

  // Let sys_ret be the return code from the mknod system call.
  int sys_ret = mknod(full_path, *mode, *dev);

  // Debug the parameters to help diagnose issues
  DLOG("watdfs_mknod called with path: %s, mode: %o, dev: %ld", full_path,
       *mode, *dev);

  if (sys_ret < 0) {
    // If there is an error on the system call, then the return code should
    // be -errno.
    *ret = -errno;
  }

  DLOG("watdfs_mknod, path: %s, ret: %d", full_path, *ret);

  // Clean up the full path, it was allocated on the heap.
  free(full_path);

  DLOG("Returning code: %d", *ret);
  // The RPC call succeeded, so return 0.
  return 0;
}

int watdfs_open(int *argTypes, void **args) {
  // Get the arguments.
  // The first argument is the path relative to the mountpoint.
  char *short_path = (char *)args[0];

  // The second argument is the fi
  struct fuse_file_info *fi = (struct fuse_file_info *)args[1];

  // Get the local file name, so we call our helper function which appends
  // the server_persist_dir to the given path.
  char *full_path = get_full_path(short_path);

  // The third argument is the return code, which should be set be 0 or -errno.
  int *ret = (int *)args[2];

  // Initially we set the return code to be 0.
  *ret = 0;

  bool is_writer = ((fi->flags & O_ACCMODE) == O_WRONLY) ||
                   ((fi->flags & O_ACCMODE) == O_RDWR);

  // Enforce single-writer per file policy.
  if (is_writer) {
    std::lock_guard<std::mutex> lk(g_state_mu);
    int &cnt = g_open_writers[short_path];

    if (cnt > 0) {
      *ret = -EACCES;
      fi->fh = 0;
      free(full_path);
      return 0;
    }
    // Reserve writer slot optimistically; will roll back on failure.
    cnt += 1;
  }

  // Let sys_ret be the return code from the open system call.
  int fh_ret = open(full_path, fi->flags);

  if (fh_ret < 0) {
    // If there is an error on the system call, then the return code should
    // be -errno.
    *ret = -errno;
    fi->fh = 0;
    // Roll back writer reservation on failure
    if (is_writer) {
      std::lock_guard<std::mutex> lk(g_state_mu);
      auto it = g_open_writers.find(short_path);

      if (it != g_open_writers.end() && it->second > 0) {
        it->second -= 1;
        if (it->second == 0)
          g_open_writers.erase(it);
      }
    }
  } else {
    fi->fh = fh_ret;
    if (is_writer) {
      std::lock_guard<std::mutex> lk(g_state_mu);
      g_fd_is_writer[fh_ret] = true;
    } else {
      std::lock_guard<std::mutex> lk(g_state_mu);
      g_fd_is_writer[fh_ret] = false;
    }
  }

  DLOG("watdfs_open, path: %s, ret: %d", full_path, *ret);

  // Clean up the full path, it was allocated on the heap.
  free(full_path);

  DLOG("Returning code: %d", *ret);
  // The RPC call succeeded, so return 0.
  return 0;
}

int watdfs_release(int *argTypes, void **args) {
  // Get the arguments.
  // The first argument is the path relative to the mountpoint.
  char *short_path = (char *)args[0];

  // The second argument is the fi
  struct fuse_file_info *fi = (struct fuse_file_info *)args[1];

  // Get the local file name, so we call our helper function which appends
  // the server_persist_dir to the given path.
  char *full_path = get_full_path(short_path);

  // The third argument is the return code, which should be set be 0 or -errno.
  int *ret = (int *)args[2];

  // Initially we set the return code to be 0.
  *ret = 0;

  // Let sys_ret be the return code from the close system call.
  int sys_ret = close(fi->fh);

  if (sys_ret < 0) {
    // If there is an error on the system call, then the return code should
    // be -errno.
    *ret = -errno;
  }

  // Update writer exclusivity tracking on close.
  {
    std::lock_guard<std::mutex> lk(g_state_mu);
    auto itfd = g_fd_is_writer.find((int)fi->fh);
    bool was_writer = false;

    if (itfd != g_fd_is_writer.end()) {
      was_writer = itfd->second;
      g_fd_is_writer.erase(itfd);
    }

    if (was_writer) {
      auto it = g_open_writers.find(short_path);
      if (it != g_open_writers.end() && it->second > 0) {
        it->second -= 1;
        if (it->second == 0)
          g_open_writers.erase(it);
      }
    }
  }

  DLOG("watdfs_release, path: %s, ret: %d", full_path, *ret);

  // Clean up the full path, it was allocated on the heap.
  free(full_path);

  DLOG("Returning code: %d", *ret);
  // The RPC call succeeded, so return 0.
  return 0;
}

int watdfs_write(int *argTypes, void **args) {
  // Get the arguments.
  // The first argument is the path relative to the mountpoint.
  char *short_path = (char *)args[0];

  // Get the local file name, so we call our helper function which appends
  // the server_persist_dir to the given path.
  char *full_path = get_full_path(short_path);

  char *buf = (char *)args[1];

  size_t *size = (size_t *)args[2];

  off_t *offset = (off_t *)args[3];

  struct fuse_file_info *fi = (struct fuse_file_info *)args[4];

  // The sixth argument is the return code, which should be set be the number of
  // bytes written or -errno.
  int *ret = (int *)args[5];

  // Initially we set the return code to be 0.
  *ret = 0;

  int bytes_written = pwrite(fi->fh, buf, *size, *offset);

  if (bytes_written < 0) {
    // If there is an error on the system call, then the return code should
    // be -errno.
    *ret = -errno;
  } else {
    *ret = bytes_written;
  }

  DLOG("watdfs_write, path: %s, ret: %d", full_path, *ret);

  // Clean up the full path, it was allocated on the heap.
  free(full_path);

  DLOG("Returning code: %d", *ret);
  // The RPC call succeeded, so return 0.
  return 0;
}

int watdfs_read(int *argTypes, void **args) {
  // Get the arguments.
  // The first argument is the path relative to the mountpoint.
  char *short_path = (char *)args[0];

  // Get the local file name, so we call our helper function which appends
  // the server_persist_dir to the given path.
  char *full_path = get_full_path(short_path);

  char *buf = (char *)args[1];

  size_t *size = (size_t *)args[2];

  off_t *offset = (off_t *)args[3];

  struct fuse_file_info *fi = (struct fuse_file_info *)args[4];

  // The sixth argument is the return code, which should be set be the number of
  // bytes read or -errno.
  int *ret = (int *)args[5];

  // Initially we set the return code to be 0.
  *ret = 0;

  int bytes_read = pread(fi->fh, buf, *size, *offset);

  if (bytes_read < 0) {
    // If there is an error on the system call, then the return code should
    // be -errno.
    *ret = -errno;
  } else {
    *ret = bytes_read;
  }

  DLOG("watdfs_read, path: %s, ret: %d", full_path, *ret);

  // Clean up the full path, it was allocated on the heap.
  free(full_path);

  DLOG("Returning code: %d", *ret);
  // The RPC call succeeded, so return 0.
  return 0;
}

int watdfs_truncate(int *argTypes, void **args) {
  // Get the arguments.
  // The first argument is the path relative to the mountpoint.
  char *short_path = (char *)args[0];

  // The second argument is the newsize
  off_t *newsize = (off_t *)args[1];

  // The third argument is the return code, which should be set be 0 or -errno.
  int *ret = (int *)args[2];

  // Get the local file name, so we call our helper function which appends
  // the server_persist_dir to the given path.
  char *full_path = get_full_path(short_path);

  // Initially we set the return code to be 0.
  *ret = 0;

  int sys_ret = truncate(full_path, *newsize);

  if (sys_ret < 0) {
    // If there is an error on the system call, then the return code should
    // be -errno.
    *ret = -errno;
  }

  DLOG("watdfs_truncate, path: %s, ret: %d", full_path, *ret);

  // Clean up the full path, it was allocated on the heap.
  free(full_path);

  DLOG("Returning code: %d", *ret);
  // The RPC call succeeded, so return 0.
  return 0;
}

int watdfs_utimensat(int *argTypes, void **args) {
  // Get the arguments.
  // The first argument is the path relative to the mountpoint.
  char *short_path = (char *)args[0];

  // The second argument is the ts
  struct timespec *ts = (struct timespec *)args[1];

  // The third argument is the return code, which should be set be 0 or -errno.
  int *ret = (int *)args[2];

  // Get the local file name, so we call our helper function which appends
  // the server_persist_dir to the given path.
  char *full_path = get_full_path(short_path);

  // Initially we set the return code to be 0.
  *ret = 0;

  int sys_ret = utimensat(AT_FDCWD, full_path, ts, 0);

  if (sys_ret < 0) {
    // If there is an error on the system call, then the return code should
    // be -errno.
    *ret = -errno;
  }

  DLOG("watdfs_utimensat, path: %s, ret: %d", full_path, *ret);

  // Clean up the full path, it was allocated on the heap.
  free(full_path);

  DLOG("Returning code: %d", *ret);
  // The RPC call succeeded, so return 0.
  return 0;
}

int watdfs_fsync(int *argTypes, void **args) {
  // Get the arguments.
  // The first argument is the path relative to the mountpoint.
  char *short_path = (char *)args[0];

  // The second argument is the fi
  struct fuse_file_info *fi = (struct fuse_file_info *)args[1];

  // The third argument is the return code, which should be set be 0 or -errno.
  int *ret = (int *)args[2];

  // Get the local file name, so we call our helper function which appends
  // the server_persist_dir to the given path.
  char *full_path = get_full_path(short_path);

  // Initially we set the return code to be 0.
  *ret = 0;

  int sys_ret = fsync(fi->fh);

  if (sys_ret < 0) {
    // If there is an error on the system call, then the return code should
    // be -errno.
    *ret = -errno;
  }

  DLOG("watdfs_fsync, path: %s, ret: %d", full_path, *ret);

  // Clean up the full path, it was allocated on the heap.
  free(full_path);

  DLOG("Returning code: %d", *ret);
  // The RPC call succeeded, so return 0.
  return 0;
}

int watdfs_lock(int *argTypes, void **args) {
  const char *short_path = (char *)args[0];
  int *mode = (int *)args[1];
  int *ret = (int *)args[2];
  *ret = 0;

  std::string p(short_path);
  rw_lock_t *lk = get_or_create_lock_for_path(p);

  if (!lk) {
    *ret = -ENOMEM;
    return 0;
  }

  int rc = rw_lock_lock(lk, (rw_lock_mode_t)(*mode));

  if (rc != 0) {
    *ret = -abs(rc);
  }

  return 0;
}

int watdfs_unlock(int *argTypes, void **args) {
  char *short_path = (char *)args[0];
  int *mode = (int *)args[1];
  int *ret = (int *)args[2];
  *ret = 0;

  std::string p(short_path);
  rw_lock_t *lk = get_or_create_lock_for_path(p);

  if (!lk) {
    *ret = -EINVAL;
    return 0;
  }

  int rc = rw_lock_unlock(lk, (rw_lock_mode_t)(*mode));

  if (rc != 0) {
    *ret = -abs(rc);
  }
  return 0;
}

// The main function of the server.
int main(int argc, char *argv[]) {
  // argv[1] should contain the directory where you should store data on the
  // server. If it is not present it is an error, that we cannot recover from.
  if (argc != 2) {
    // In general, you shouldn't print to stderr or stdout, but it may be
    // helpful here for debugging. Important: Make sure you turn off logging
    // prior to submission!
    // See watdfs_client.cpp for more details
    // # ifdef PRINT_ERR
    // std::cerr << "Usage:" << argv[0] << " server_persist_dir";
    // #endif
    return -1;
  }
  // Store the directory in a global variable.
  server_persist_dir = argv[1];

  // Important: `rpcServerInit` prints the 'export SERVER_ADDRESS' and
  // 'export SERVER_PORT' lines. Make sure you *do not* print anything
  // to *stdout* before calling `rpcServerInit`.
  DLOG("Initializing server...");
  int init_status = rpcServerInit();

  int ret = 0;
  if (init_status < 0) {
    return init_status;
  }

  // Note: The braces are used to limit the scope of `argTypes`, so that you can
  // reuse the variable for multiple registrations. Another way could be to
  // remove the braces and use `argTypes0`, `argTypes1`, etc.
  {
    // There are 3 args for the function (see watdfs_client.cpp for more
    // detail).
    int argTypes[4];
    // First is the path.
    argTypes[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
    // The second argument is the statbuf.
    argTypes[1] =
        (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
    // The third argument is the retcode.
    argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    // Finally we fill in the null terminator.
    argTypes[3] = 0;

    ret = rpcRegister((char *)"getattr", argTypes, watdfs_getattr);
    if (ret < 0) {
      return ret;
    }
  }

  {
    int argTypes[5];

    argTypes[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[1] = (1u << ARG_INPUT) | (ARG_INT << 16u);

    argTypes[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);

    argTypes[3] = (1 << ARG_OUTPUT) | (ARG_INT << 16u);

    // Finally we fill in the null terminator.
    argTypes[4] = 0;

    ret = rpcRegister((char *)"mknod", argTypes, watdfs_mknod);
    if (ret < 0) {
      return ret;
    }
  }

  {
    int argTypes[4];

    argTypes[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[1] = (1u << ARG_INPUT) | (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) |
                  (ARG_CHAR << 16u) | 1u;

    argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);

    argTypes[3] = 0;

    ret = rpcRegister((char *)"open", argTypes, watdfs_open);
    if (ret < 0) {
      return ret;
    }
  }

  {
    int argTypes[4];

    argTypes[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[1] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);

    argTypes[3] = 0;

    ret = rpcRegister((char *)"release", argTypes, watdfs_release);
    if (ret < 0) {
      return ret;
    }
  }

  {
    int argTypes[7];
    argTypes[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[1] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);

    argTypes[3] = (1u << ARG_INPUT) | (ARG_LONG << 16u);

    argTypes[4] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[5] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);

    argTypes[6] = 0;

    ret = rpcRegister((char *)"write", argTypes, watdfs_write);
    if (ret < 0) {
      return ret;
    }
  }

  {
    int argTypes[7];
    argTypes[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[1] =
        (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);

    argTypes[3] = (1u << ARG_INPUT) | (ARG_LONG << 16u);

    argTypes[4] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[5] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);

    argTypes[6] = 0;

    ret = rpcRegister((char *)"read", argTypes, watdfs_read);
    if (ret < 0) {
      return ret;
    }
  }

  {
    int argTypes[4];

    argTypes[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[1] = (1u << ARG_INPUT) | (ARG_LONG << 16u);

    argTypes[2] = (1 << ARG_OUTPUT) | (ARG_INT << 16u);

    argTypes[3] = 0;

    ret = rpcRegister((char *)"truncate", argTypes, watdfs_truncate);
    if (ret < 0) {
      return ret;
    }
  }

  {
    int argTypes[4];

    argTypes[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[1] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[2] = (1 << ARG_OUTPUT) | (ARG_INT << 16u);

    argTypes[3] = 0;

    ret = rpcRegister((char *)"utimensat", argTypes, watdfs_utimensat);
    if (ret < 0) {
      return ret;
    }
  }

  {
    int argTypes[4];

    argTypes[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[1] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[2] = (1 << ARG_OUTPUT) | (ARG_INT << 16u);

    argTypes[3] = 0;

    ret = rpcRegister((char *)"fsync", argTypes, watdfs_fsync);
    if (ret < 0) {
      return ret;
    }
  }

  {
    int argTypes[4];

    argTypes[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[1] = (1u << ARG_INPUT) | (ARG_INT << 16u);

    argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);

    argTypes[3] = 0;

    ret = rpcRegister((char *)"lock", argTypes, watdfs_lock);
    if (ret < 0) {
      return ret;
    }
  }

  {
    int argTypes[4];

    argTypes[0] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;

    argTypes[1] = (1u << ARG_INPUT) | (ARG_INT << 16u);

    argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);

    argTypes[3] = 0;

    ret = rpcRegister((char *)"unlock", argTypes, watdfs_unlock);

    if (ret < 0) {
      return ret;
    }
  }

  int execute_status = rpcExecute();

  if (execute_status < 0) {
    DLOG("rpcExecute failed, ret code: %d", execute_status);
  }

  // rpcExecute could fail, so you may want to have debug-printing here, and
  // then you should return.
  return ret;
}
