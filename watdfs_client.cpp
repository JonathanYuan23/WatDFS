#include "watdfs_client.h"
#include "debug.h"
#include <cerrno>
INIT_LOG

#include "rpc.h"
#include "rw_lock.h"

// Local client state (userdata) stored as opaque void* back to FUSE.
// We do NOT modify the public header (forbidden by assignment instructions),
// so we keep this private here.
#include <algorithm>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <string>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

struct WatdfsClientState {
  std::string cache_path; // absolute or relative path to local cache dir
  time_t cache_interval;  // cache freshness interval (seconds)
};

// Per-open handle stored in fi->fh (cast to/from uint64_t)
struct watdfs_handle {
  int cache_fd;           // local cached file fd
  int flags;              // O_RDONLY/O_WRONLY/O_RDWR from original open
  time_t Tc;              // last validation time
  time_t T_client;        // client-side mtime
  time_t T_server;        // server-side mtime
  bool dirty;             // true after local modification until upload
  bool has_remote_writer; // true if we reserved server-side writer
  struct fuse_file_info remote_fi; // reserved remote write handle
};

// Build absolute cache file path from st->cache_path
static std::string cache_full_path(const WatdfsClientState *st,
                                   const char *path) {
  if (!st) {
    return std::string();
  }

  if (!path) {
    return st->cache_path;
  }

  const char *rel = path;
  if (*rel == '/') {
    rel++; // skip leading slash from FUSE path
  }

  std::string result = st->cache_path;
  if (!result.empty()) {
    const char last = result.c_str()[result.size() - 1];
    if (last != '/') {
      result.push_back('/');
    }
  }

  if (rel && *rel) {
    result.append(rel);
  }

  return result;
}

// Ensure parent directories exist for a given file path
static int ensure_parent_dirs(const std::string &filepath) {
  if (filepath.empty()) {
    return 0;
  }
  const char *p = filepath.c_str();
  const char *s = p;

  for (; *s; ++s) {
    if (*s == '/') {
      if (s == p) {
        continue; // leading slash
      }

      std::string dir(p, s - p);
      if (dir.empty()) {
        continue;
      }

      struct stat st;
      if (stat(dir.c_str(), &st) == 0) {
        if (!S_ISDIR(st.st_mode)) {
          return -ENOTDIR;
        }
      } else {
        if (mkdir(dir.c_str(), 0755) < 0 && errno != EEXIST) {
          return -errno;
        }
      }
    }
  }
  return 0;
}

// Forward declarations for new helpers
static int download_to_cache(const char *path, const std::string &local_path,
                             struct stat *out_server_stat);

static int upload_from_cache(const char *path, int cache_fd,
                             struct fuse_file_info *remote_fi);

// Push local atime/mtime to server via utimensat RPC
static int rpc_utimensat_from_stat(const char *path, const struct stat *st);

// Common RPC helpers
static int rpc_release_remote(const char *path,
                              const struct fuse_file_info *fi);
static int rpc_open_remote(const char *path, struct fuse_file_info *fi,
                           int flags);

static int rpc_getattr_remote(const char *path, struct stat *st) {
  if (!path || !st)
    return -EINVAL;
  int pathlen = (int)strlen(path) + 1;
  int ARG_COUNT = 3;
  void **args = new void *[ARG_COUNT];
  int arg_types[ARG_COUNT + 1];

  int ret_code = 0;
  // path input
  arg_types[0] =
      (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint)pathlen;
  args[0] = (void *)path;

  // stat output as raw char array
  arg_types[1] = (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                 (uint)sizeof(struct stat);
  args[1] = (void *)st;

  // return code output
  arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
  args[2] = (int *)&ret_code;
  arg_types[3] = 0;

  int rpc_ret = rpcCall((char *)"getattr", arg_types, args);
  delete[] args;
  if (rpc_ret < 0)
    return -EINVAL;
  return ret_code;
}

// Lock helpers for transfer atomicity
static int rpc_lock_remote(const char *path, rw_lock_mode_t mode) {
  if (!path)
    return -EINVAL;
  int ret_code = 0;
  int pathlen = strlen(path) + 1;
  int ARG_COUNT = 3;

  void **args = new void *[ARG_COUNT];
  int arg_types[ARG_COUNT + 1];

  arg_types[0] =
      (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint)pathlen;
  args[0] = (void *)path;

  int mode_i = (int)mode;
  arg_types[1] = (1u << ARG_INPUT) | (ARG_INT << 16u);
  args[1] = (int *)&mode_i;

  arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
  args[2] = (int *)&ret_code;

  arg_types[3] = 0;
  int rpc_ret = rpcCall((char *)"lock", arg_types, args);
  delete[] args;

  if (rpc_ret < 0)
    return -EINVAL;
  return ret_code;
}

static int rpc_unlock_remote(const char *path, rw_lock_mode_t mode) {
  if (!path)
    return -EINVAL;
  int ret_code = 0;
  int pathlen = strlen(path) + 1;
  int ARG_COUNT = 3;

  void **args = new void *[ARG_COUNT];
  int arg_types[ARG_COUNT + 1];

  arg_types[0] =
      (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint)pathlen;
  args[0] = (void *)path;

  int mode_i = (int)mode;
  arg_types[1] = (1u << ARG_INPUT) | (ARG_INT << 16u);
  args[1] = (int *)&mode_i;

  arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
  args[2] = (int *)&ret_code;

  arg_types[3] = 0;
  int rpc_ret = rpcCall((char *)"unlock", arg_types, args);
  delete[] args;

  if (rpc_ret < 0)
    return -EINVAL;
  return ret_code;
}

// Track the single open handle per path
static std::unordered_map<std::string, watdfs_handle *> open_handles;

static void register_open_handle(const char *path, watdfs_handle *h) {
  if (!path || !h) {
    return;
  }
  std::string key(path);
  open_handles[key] = h;
}

static void unregister_open_handle(const char *path, watdfs_handle *h) {
  if (!path || !h) {
    return;
  }
  std::string key(path);

  auto it = open_handles.find(key);
  if (it == open_handles.end()) {
    return;
  }

  if (it->second == h) {
    open_handles.erase(it);
  }
}

static watdfs_handle *find_handle(const char *path) {
  if (!path) {
    return nullptr;
  }

  std::string key(path);

  auto it = open_handles.find(key);
  if (it == open_handles.end()) {
    return nullptr;
  }

  return it->second;
}

static watdfs_handle *find_read_only_handle(const char *path) {
  if (!path) {
    return nullptr;
  }

  std::string key;
  key.assign(path);
  auto it = open_handles.find(key);

  if (it == open_handles.end()) {
    return nullptr;
  }

  watdfs_handle *h = it->second;
  if (!h) {
    return nullptr;
  }

  int acc = (h->flags & O_ACCMODE);
  if (acc == O_RDONLY) {
    return h;
  }

  return nullptr;
}

// If cache is stale, revalidate against server and refresh local file
// Only performs refresh for non-dirty handles
// Write-open dirty handles are left unchanged
static int refresh_cache_if_stale(void *userdata, const char *path,
                                  watdfs_handle *h) {
  if (!userdata || !path || !h)
    return -EINVAL;
  WatdfsClientState *stp = reinterpret_cast<WatdfsClientState *>(userdata);
  time_t now = time(nullptr);
  // Freshness rule for reads: if (T - Tc < t) OR (T_client == T_server),
  // we can serve from local cache. The first check is purely local
  if ((now - h->Tc) < stp->cache_interval) {
    return 0; // fresh by time bound
  }

  // If we have local dirty changes, do not overwrite from server
  int acc = (h->flags & O_ACCMODE);
  // When opened for write, skip freshness checks entirely
  if (acc == O_WRONLY || acc == O_RDWR) {
    h->Tc = now; // mark checked without contacting server
    return 0;
  }

  // Fetch server stat to evaluate T_client == T_server and potential refresh
  struct stat s_stat{};
  int ret_code = rpc_getattr_remote(path, &s_stat);
  if (ret_code < 0)
    return ret_code; // file missing or error / transport fail

  // Compare server metadata; refresh if server is newer or size changed
  struct stat lst_cur{};
  fstat(h->cache_fd, &lst_cur);

  // If T_client == T_server, update Tc and continue; else refresh
  if (h->T_client == s_stat.st_mtime) {
    h->T_server = s_stat.st_mtime;
    h->Tc = now;
    return 0;
  }

  bool server_newer = (s_stat.st_mtime > h->T_server);
  bool size_changed = (lst_cur.st_size != s_stat.st_size);
  if (server_newer || size_changed) {
    // Refresh: download to cache path
    WatdfsClientState *cst = reinterpret_cast<WatdfsClientState *>(userdata);
    std::string local_path = cache_full_path(cst, path);

    int dl_rc = download_to_cache(path, local_path, &s_stat);
    if (dl_rc < 0) {
      return dl_rc;
    }

    // Update local and server times
    // After refreshing content, align T_client with T_server
    h->T_client = s_stat.st_mtime;
    h->T_server = s_stat.st_mtime;
  }

  h->Tc = now;
  return 0;
}

// Helper implementations
static int download_to_cache(const char *path, const std::string &local_path,
                             struct stat *out_server_stat) {
  // Acquire read transfer lock on server for atomic download
  int lk_rc = rpc_lock_remote(path, RW_READ_LOCK);
  if (lk_rc < 0) {
    return lk_rc;
  }
  // First query server to confirm existence and metadata
  struct stat s_stat{};
  int ret_code = rpc_getattr_remote(path, &s_stat);
  if (ret_code < 0) {
    // No local changes; release lock before returning
    rpc_unlock_remote(path, RW_READ_LOCK);
    return ret_code;
  }

  // Ensure parent directories then open local file for writing/truncation
  int dir_rc = ensure_parent_dirs(local_path);
  if (dir_rc < 0) {
    rpc_unlock_remote(path, RW_READ_LOCK);
    return dir_rc;
  }

  int fd = ::open(local_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    int e = -errno;
    rpc_unlock_remote(path, RW_READ_LOCK);
    return e;
  }

  // Open remote read-only
  struct fuse_file_info remote_fi = {};
  {
    int open_ret = rpc_open_remote(path, &remote_fi, O_RDONLY);
    if (open_ret < 0) {
      ::close(fd);
      rpc_unlock_remote(path, RW_READ_LOCK);
      return open_ret;
    }
  }

  // Transfer loop
  int pathlen = strlen(path) + 1;
  off_t offset = 0;
  std::vector<char> buf(MAX_ARRAY_LEN);
  while (true) {
    int read_ret_code = 0;
    size_t chunk = MAX_ARRAY_LEN;

    int ARG_COUNT = 6;
    void **args = new void *[ARG_COUNT];

    int arg_types[ARG_COUNT + 1];

    arg_types[0] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)pathlen;
    args[0] = (void *)path;

    arg_types[1] = (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)chunk;
    args[1] = (void *)buf.data();

    arg_types[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
    args[2] = (size_t *)&chunk;

    arg_types[3] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
    args[3] = (off_t *)&offset;

    arg_types[4] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)sizeof(struct fuse_file_info);
    args[4] = (void *)&remote_fi;

    arg_types[5] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    args[5] = (int *)&read_ret_code;

    arg_types[6] = 0;

    int rpc_ret = rpcCall((char *)"read", arg_types, args);
    delete[] args;

    if (rpc_ret < 0) {
      read_ret_code = -EINVAL;
    }

    if (read_ret_code < 0) {
      rpc_release_remote(path, &remote_fi);
      ::close(fd);
      rpc_unlock_remote(path, RW_READ_LOCK);
      return read_ret_code;
    }

    if (read_ret_code == 0) {
      break; // EOF
    }

    ssize_t wr = ::write(fd, buf.data(), (size_t)read_ret_code);

    if (wr < 0) {
      int e = -errno;
      rpc_release_remote(path, &remote_fi);
      ::close(fd);
      rpc_unlock_remote(path, RW_READ_LOCK);
      return e;
    }
    offset += wr;
  }

  // close remote
  rpc_release_remote(path, &remote_fi);

  ::fsync(fd);

  if (out_server_stat) {
    *out_server_stat = s_stat;
  }

  ::close(fd);
  // Release read transfer lock on server
  rpc_unlock_remote(path, RW_READ_LOCK);
  return 0;
}

static int upload_from_cache(const char *path, int cache_fd,
                             struct fuse_file_info *remote_fi) {
  if (!path) {
    return -EINVAL;
  }

  // Acquire write transfer lock on server for atomic upload
  int lk_rc = rpc_lock_remote(path, RW_WRITE_LOCK);
  if (lk_rc < 0) {
    return lk_rc;
  }

  // Determine local size
  struct stat st{};
  if (fstat(cache_fd, &st) < 0) {
    int e = -errno;
    rpc_unlock_remote(path, RW_WRITE_LOCK);
    return e;
  }
  int pathlen = strlen(path) + 1;

  // Determine remote handle: reuse provided one if valid, else open new
  struct fuse_file_info rfi = {};
  bool reused_remote = false;
  if (remote_fi && remote_fi->fh != 0) {
    rfi = *remote_fi;
    reused_remote = true;
  } else {
    int open_ret = rpc_open_remote(path, &rfi, O_WRONLY);
    if (open_ret < 0) {
      rpc_unlock_remote(path, RW_WRITE_LOCK);
      return open_ret;
    }
  }

  // Remote truncate to handle shrink
  {
    int ARG_COUNT = 3;

    void **args = new void *[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int ret_code = 0;

    arg_types[0] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)pathlen;
    args[0] = (void *)path;

    arg_types[1] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
    args[1] = (off_t *)&st.st_size;

    arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    args[2] = (int *)&ret_code;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"truncate", arg_types, args);
    delete[] args;

    if (rpc_ret < 0) { // best-effort release
      if (!reused_remote)
        rpc_release_remote(path, &rfi);
      rpc_unlock_remote(path, RW_WRITE_LOCK);
      return -EINVAL;
    }

    if (ret_code < 0) {
      if (!reused_remote)
        rpc_release_remote(path, &rfi);
      rpc_unlock_remote(path, RW_WRITE_LOCK);
      return ret_code;
    }
  }

  // Write loop
  off_t off = 0;
  std::vector<char> buf(MAX_ARRAY_LEN);
  while (off < st.st_size) {
    size_t chunk = (size_t)std::min<off_t>(MAX_ARRAY_LEN, st.st_size - off);
    ssize_t rd = ::pread(cache_fd, buf.data(), chunk, off);

    if (rd < 0) {
      int e = -errno;
      if (!reused_remote)
        rpc_release_remote(path, &rfi);
      rpc_unlock_remote(path, RW_WRITE_LOCK);
      return e;
    }
    if (rd == 0) {
      break;
    }

    size_t sz = (size_t)rd;
    int wr_ret_code = 0;
    int ARG_COUNT = 6;

    void **args = new void *[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    arg_types[0] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)pathlen;
    args[0] = (void *)path;

    arg_types[1] =
        (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint)sz;
    args[1] = (void *)buf.data();

    arg_types[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
    args[2] = (size_t *)&sz;

    arg_types[3] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
    args[3] = (off_t *)&off;

    arg_types[4] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)sizeof(struct fuse_file_info);
    args[4] = (void *)&rfi;

    arg_types[5] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    args[5] = (int *)&wr_ret_code;

    arg_types[6] = 0;

    int rpc_ret = rpcCall((char *)"write", arg_types, args);

    delete[] args;

    if (rpc_ret < 0) {
      wr_ret_code = -EINVAL;
    }

    if (wr_ret_code < 0) {
      if (!reused_remote)
        rpc_release_remote(path, &rfi);
      rpc_unlock_remote(path, RW_WRITE_LOCK);
      return wr_ret_code;
    }

    off += wr_ret_code;
    if (wr_ret_code == 0) {
      break;
    }
  }

  // Close remote if we opened it; otherwise leave reserved handle open
  if (!reused_remote) {
    rpc_release_remote(path, &rfi);
    if (remote_fi)
      *remote_fi = rfi;
  }
  // Release write transfer lock on server
  rpc_unlock_remote(path, RW_WRITE_LOCK);
  return 0;
}

// Helper to invoke utimensat RPC on server with times from a local stat
static int rpc_utimensat_from_stat(const char *path, const struct stat *st) {
  if (!path || !st) {
    return -EINVAL;
  }

  struct timespec ts[2];
  // Use second-level resolution for portability
  ts[0].tv_sec = st->st_atime;
  ts[0].tv_nsec = 0;
  ts[1].tv_sec = st->st_mtime;
  ts[1].tv_nsec = 0;

  int ret_code = 0;
  int pathlen = strlen(path) + 1;

  int ARG_COUNT = 3;
  void **args = new void *[ARG_COUNT];
  int arg_types[ARG_COUNT + 1];

  arg_types[0] =
      (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint)pathlen;
  args[0] = (void *)path;

  arg_types[1] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                 (uint)sizeof(ts);
  args[1] = (void *)ts;

  arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
  args[2] = (int *)&ret_code;

  arg_types[3] = 0;

  int rpc_ret = rpcCall((char *)"utimensat", arg_types, args);
  delete[] args;
  if (rpc_ret < 0) {
    return -EINVAL;
  }

  return ret_code; // 0 or -errno from server
}

// Helper to call server 'release' RPC for an opened remote file
static int rpc_release_remote(const char *path,
                              const struct fuse_file_info *fi) {
  if (!path || !fi)
    return -EINVAL;

  int ret_code = 0;
  int pathlen = strlen(path) + 1;

  int ARG_COUNT = 3;
  void **args = new void *[ARG_COUNT];
  int arg_types[ARG_COUNT + 1];

  arg_types[0] =
      (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint)pathlen;
  args[0] = (void *)path;

  arg_types[1] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                 (uint)sizeof(struct fuse_file_info);
  args[1] = (void *)fi;

  arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
  args[2] = (int *)&ret_code;

  arg_types[3] = 0;

  int rpc_ret = rpcCall((char *)"release", arg_types, args);
  delete[] args;
  if (rpc_ret < 0) {
    return -EINVAL;
  }
  return ret_code;
}

// Helper to call server 'open' RPC with desired flags
static int rpc_open_remote(const char *path, struct fuse_file_info *fi,
                           int flags) {
  if (!path || !fi)
    return -EINVAL;
  memset(fi, 0, sizeof(*fi));
  fi->flags = flags;

  int ret_code = 0;
  int pathlen = strlen(path) + 1;

  int ARG_COUNT = 3;
  void **args = new void *[ARG_COUNT];
  int arg_types[ARG_COUNT + 1];

  arg_types[0] =
      (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint)pathlen;
  args[0] = (void *)path;

  arg_types[1] = (1u << ARG_INPUT) | (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) |
                 (ARG_CHAR << 16u) | (uint)sizeof(struct fuse_file_info);
  args[1] = (void *)fi;

  arg_types[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
  args[2] = (int *)&ret_code;

  arg_types[3] = 0;

  int rpc_ret = rpcCall((char *)"open", arg_types, args);
  delete[] args;
  if (rpc_ret < 0) {
    return -EINVAL;
  }
  return ret_code;
}

void *watdfs_cli_init(struct fuse_conn_info *conn, const char *path_to_cache,
                      time_t cache_interval, int *ret_code) {
  int init_status = rpcClientInit();

  // Initialize global/connection state and return it as void*.
  // We allocate even before confirming RPC init so we can later extend it;
  // if RPC init fails we free it before returning nullptr.
  WatdfsClientState *state = new (std::nothrow) WatdfsClientState();
  if (!state) {
    // Allocation failure: treat as fatal for init.
    *ret_code = -ENOMEM;
    return nullptr;
  }

  // Save `path_to_cache` and `cache_interval`. We store a copy of the path
  // string to avoid lifetime issues.
  if (path_to_cache) {
    state->cache_path = path_to_cache;
  } else {
    state->cache_path.clear();
  }
  state->cache_interval = cache_interval;
  DLOG("Client init: cache_path='%s' interval=%ld", state->cache_path.c_str(),
       (long)state->cache_interval);

  // Set appropriate return code: 0 on success, else propagated error.
  if (init_status < 0) {
    DLOG("rpcClientInit failed, ret code: %d", init_status);
    *ret_code = init_status;
    delete state; // avoid leak
    return nullptr;
  } else {
    *ret_code = 0;
  }

  return (void *)state;
}

void watdfs_cli_destroy(void *userdata) {
  if (userdata) {
    WatdfsClientState *state = reinterpret_cast<WatdfsClientState *>(userdata);
    DLOG("Client destroy: cache_path='%s' interval=%ld",
         state->cache_path.c_str(), (long)state->cache_interval);
    delete state; // free allocated state
  }
  rpcClientDestroy();
}

int watdfs_cli_getattr(void *userdata, const char *path, struct stat *statbuf) {
  DLOG("watdfs_cli_getattr called for '%s'", path);
  if (!path || !statbuf)
    return -EINVAL;

  // If already open
  watdfs_handle *h = find_handle(path);
  if (h) {
    int acc = (h->flags & O_ACCMODE);
    if (acc == O_WRONLY || acc == O_RDWR) {
      // Writer open: no freshness checks. Use local metadata.
      struct stat lst{};
      if (fstat(h->cache_fd, &lst) == 0) {
        *statbuf = lst;
        return 0;
      }
      return -errno;
    }

    // Read-only open: perform read freshness check and then return local
    // metadata
    refresh_cache_if_stale(userdata, path, h);
    struct stat lst{};
    if (fstat(h->cache_fd, &lst) == 0) {
      *statbuf = lst;
      return 0;
    }
    return -errno;
  }

  // Not open: consult server for existence/type, then download only regular
  // files
  struct stat sstat{};
  int s_rc = rpc_getattr_remote(path, &sstat);
  if (s_rc < 0) {
    memset(statbuf, 0, sizeof(struct stat));
    return s_rc;
  }
  if (!S_ISREG(sstat.st_mode)) {
    // Non-regular; just return server metadata
    *statbuf = sstat;
    return 0;
  }
  WatdfsClientState *st = reinterpret_cast<WatdfsClientState *>(userdata);
  std::string local_path = cache_full_path(st, path);
  int rc = download_to_cache(path, local_path, &sstat);
  if (rc < 0) {
    memset(statbuf, 0, sizeof(struct stat));
    return rc;
  }
  struct stat cst{};
  if (::stat(local_path.c_str(), &cst) == 0) {
    *statbuf = cst;
    return 0;
  }
  return -errno;
}

int watdfs_cli_mknod(void *userdata, const char *path, mode_t mode, dev_t dev) {
  DLOG("watdfs_cli_mknod called for '%s'", path);

  int ARG_COUNT = 4;

  void **args = new void *[ARG_COUNT];

  int arg_types[ARG_COUNT + 1];

  int pathlen = strlen(path) + 1;

  arg_types[0] =
      (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint)pathlen;
  args[0] = (void *)path;

  arg_types[1] = (1u << ARG_INPUT) | (ARG_INT << 16u);
  args[1] = (mode_t *)&mode;

  arg_types[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
  args[2] = (dev_t *)&dev;

  arg_types[3] = (1 << ARG_OUTPUT) | (ARG_INT << 16u);

  int ret_code;
  args[3] = (int *)&ret_code;

  arg_types[4] = 0;

  int rpc_ret = rpcCall((char *)"mknod", arg_types, args);

  int fxn_ret = 0;
  if (rpc_ret < 0) {
    DLOG("mknod rpc failed with error '%d'", rpc_ret);
    fxn_ret = -EINVAL;
  } else {
    fxn_ret = ret_code;
    DLOG("mknod server returned code: %d", ret_code);
  }

  // Clean up the memory we have allocated.
  delete[] args;

  // Finally return the value we got from the server.
  return fxn_ret;
}

int watdfs_cli_open(void *userdata, const char *path,
                    struct fuse_file_info *fi) {
  // Called during open.
  // You should fill in fi->fh.
  DLOG("watdfs_cli_open called for '%s'", path);

  // Enforce single client-side open per path
  if (find_handle(path)) {
    return -EMFILE;
  }

  WatdfsClientState *st = reinterpret_cast<WatdfsClientState *>(userdata);
  std::string local_path = cache_full_path(st, path);

  // Ensure parent directories exist
  int dir_rc = ensure_parent_dirs(local_path);
  if (dir_rc < 0) {
    return dir_rc;
  }

  // Existence check via getattr
  struct stat s_stat{};
  int ret_code = rpc_getattr_remote(path, &s_stat);
  DLOG("open getattr: ret_code=%d path='%s' flags=%#x", ret_code, path,
       fi ? fi->flags : 0);

  if (ret_code < 0 && ret_code != -ENOENT) {
    DLOG("open getattr error: ret_code=%d (no create flag)", ret_code);
    return ret_code; // propagate other error
  }

  if (ret_code == -ENOENT && (fi->flags & O_CREAT)) {
    DLOG("O_CREAT set and file missing; mknod on server for '%s'", path);
    // create on server
    mode_t mode = S_IFREG | 0644;
    dev_t dev = 0;
    int mk_ret_code = 0;
    int ARG_COUNT = 4;

    void **args = new void *[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;

    arg_types[0] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint)pathlen;
    args[0] = (void *)path;

    arg_types[1] = (1u << ARG_INPUT) | (ARG_INT << 16u);
    args[1] = (mode_t *)&mode;

    arg_types[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
    args[2] = (dev_t *)&dev;

    arg_types[3] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
    args[3] = (int *)&mk_ret_code;

    arg_types[4] = 0;

    int rpc_ret = rpcCall((char *)"mknod", arg_types, args);
    DLOG("open mknod: rpc_ret=%d mk_ret_code=%d path='%s'", rpc_ret,
         mk_ret_code, path);
    delete[] args;

    if (rpc_ret < 0) {
      return -EINVAL;
    }

    if (mk_ret_code < 0) {
      return mk_ret_code;
    }

    // empty file; update server stat
    s_stat.st_size = 0;
    s_stat.st_mtime = time(nullptr);
    ret_code = 0;
  } else if (ret_code < 0) {
    return ret_code;
  }

  // Download content to cache (may be zero-length)
  int dl_rc = download_to_cache(path, local_path, &s_stat);
  if (dl_rc < 0) {
    return dl_rc;
  }

  // Mask flags when opening local file
  int acc = fi->flags & O_ACCMODE;
  // We will need to read from the cache file during upload (fsync/release).
  // If the remote open was write-only, promote the local cache fd to O_RDWR
  // so pread succeeds later.
  if (acc == O_WRONLY) {
    acc = O_RDWR;
  }
  int extra = 0;

  if (fi->flags & O_APPEND) {
    extra |= O_APPEND;
  }

  // (O_DSYNC optional)
  int local_fd = ::open(local_path.c_str(), acc | extra, 0644);

  if (local_fd < 0) {
    return -errno;
  }

  if (fi->flags & O_TRUNC) {
    if (ftruncate(local_fd, 0) < 0) {
      int e = -errno;
      ::close(local_fd);
      return e;
    }
  }

  struct stat c_stat;
  if (fstat(local_fd, &c_stat) < 0) {
    int e = -errno;
    ::close(local_fd);
    return e;
  }

  watdfs_handle *h = new (std::nothrow) watdfs_handle();

  if (!h) {
    ::close(local_fd);
    return -ENOMEM;
  }

  h->cache_fd = local_fd;
  h->flags = (fi->flags & (O_ACCMODE | O_APPEND));

  // Track server and client modification times separately
  // Use server stat mtime captured during download for T_server, and local
  // cache file stat for T_client.
  h->T_server = s_stat.st_mtime;
  h->T_client = s_stat.st_mtime;
  h->Tc = time(nullptr);
  h->dirty = (fi->flags & O_TRUNC) ? true : false;
  h->has_remote_writer = false;
  memset(&h->remote_fi, 0, sizeof(h->remote_fi));

  // If opening for write, reserve writer slot on server (single-writer)
  int acc_open = (fi->flags & O_ACCMODE);
  if (acc_open == O_WRONLY || acc_open == O_RDWR) {
    struct fuse_file_info wfi = {};
    int wrc = rpc_open_remote(path, &wfi, O_WRONLY);
    if (wrc < 0) {
      ::close(local_fd);
      delete h;
      return wrc;
    }
    h->has_remote_writer = true;
    h->remote_fi = wfi;
  }

  fi->fh = reinterpret_cast<uint64_t>(h);
  register_open_handle(path, h);
  return 0;
}

int watdfs_cli_release(void *userdata, const char *path,
                       struct fuse_file_info *fi) {
  DLOG("watdfs_cli_release called for '%s'", path);

  watdfs_handle *h = reinterpret_cast<watdfs_handle *>(fi->fh);

  int ret = 0;
  int acc = (h->flags & O_ACCMODE);

  if (h && (acc == O_WRONLY || acc == O_RDWR) && h->dirty) {
    struct fuse_file_info remote_fi = {};
    remote_fi.flags = O_WRONLY;

    // Reuse reserved remote writer if available
    struct fuse_file_info use_fi =
        h->has_remote_writer ? h->remote_fi : remote_fi;
    int up_rc = upload_from_cache(path, h->cache_fd, &use_fi);

    DLOG("upload_from_cache rc=%d", up_rc);
    if (up_rc < 0) {
      ret = up_rc;
    } else {
      struct stat lst;
      if (fstat(h->cache_fd, &lst) == 0) {
        DLOG("fstat successful");
        // Push metadata (atime/mtime) to server
        int ut_rc = rpc_utimensat_from_stat(path, &lst);
        if (ut_rc < 0) {
          ret = ut_rc;
        }
        h->T_client = lst.st_mtime;
        if (ret == 0) {
          h->T_server = h->T_client;
          h->dirty = false;
        }
        h->Tc = time(nullptr);
      }
    }
  }

  // Close reserved remote writer handle if held
  if (h && h->has_remote_writer) {
    rpc_release_remote(path, &h->remote_fi);
    h->has_remote_writer = false;
    memset(&h->remote_fi, 0, sizeof(h->remote_fi));
  }

  // Close local fd and free handle
  if (h) {
    unregister_open_handle(path, h);
    ::close(h->cache_fd);
    delete h;
    fi->fh = 0;
  }
  return ret;
}

int watdfs_cli_read(void *userdata, const char *path, char *buf, size_t size,
                    off_t offset, struct fuse_file_info *fi) {
  // Read size amount of data at offset of file from local cache.
  DLOG("watdfs_cli_read called for '%s', size: %zu, offset: %ld", path, size,
       offset);
  watdfs_handle *h = reinterpret_cast<watdfs_handle *>(fi->fh);
  if (!h) {
    return -EINVAL;
  }
  // Validate cache if stale
  int rv = refresh_cache_if_stale(userdata, path, h);
  if (rv < 0) {
    DLOG("refresh_cache_if_stale failed rc=%d", rv);
    // Non-fatal: continue to read local cache to avoid surprising errors
  }
  ssize_t rd = ::pread(h->cache_fd, buf, size, offset);
  if (rd < 0) {
    return -errno;
  }
  return (int)rd;
}

int watdfs_cli_write(void *userdata, const char *path, const char *buf,
                     size_t size, off_t offset, struct fuse_file_info *fi) {
  // Write size amount of data at offset of file to local cache.
  DLOG("watdfs_cli_write called for '%s', size: %zu, offset: %ld", path, size,
       offset);
  watdfs_handle *h = reinterpret_cast<watdfs_handle *>(fi->fh);

  if (!h) {
    return -EINVAL;
  }

  // Respect O_APPEND semantics: when opened with O_APPEND, ignore the
  // provided offset and append atomically via write.
  ssize_t wr;
  if (h->flags & O_APPEND) {
    wr = ::write(h->cache_fd, buf, size);
  } else {
    wr = ::pwrite(h->cache_fd, buf, size, offset);
  }

  if (wr < 0) {
    return -errno;
  }

  // Update client time based on local stat
  struct stat st;
  if (fstat(h->cache_fd, &st) == 0) {
    h->T_client = st.st_mtime;
    h->dirty = true;
  }

  // Freshness rule for writes at end of write: if (T - Tc < t) OR
  // (T_client == T_server) we can return, else write back synchronously.
  WatdfsClientState *stp = reinterpret_cast<WatdfsClientState *>(userdata);

  time_t now = time(nullptr);
  bool fresh_time = (stp && stp->cache_interval > 0)
                        ? ((now - h->Tc) < stp->cache_interval)
                        : true;

  // If freshness by time expired, consult server for T_server before comparing
  if (!fresh_time) {
    struct stat s_stat{};
    int s_rc = rpc_getattr_remote(path, &s_stat);
    if (s_rc == 0) {
      h->T_server = s_stat.st_mtime;
    }
  }

  if (!fresh_time && h->T_client != h->T_server) {
    struct fuse_file_info rfi = {};
    rfi.flags = O_WRONLY;

    struct fuse_file_info use_fi = h->has_remote_writer ? h->remote_fi : rfi;

    int rc = upload_from_cache(path, h->cache_fd, &use_fi);
    if (rc == 0) {
      // Align server times with client and update bookkeeping
      int ut_rc = rpc_utimensat_from_stat(path, &st);
      if (ut_rc < 0) {
        DLOG("rpc_utimensat_from_stat not successful");
      }
      h->T_server = h->T_client;
      h->Tc = now;
      h->dirty = false;
    } else {
      DLOG("upload_from_cache not successful");
    }
  } else if (!fresh_time) {
    h->Tc = now;
  }

  return (int)wr;
}

int watdfs_cli_truncate(void *userdata, const char *path, off_t newsize) {
  DLOG("watdfs_cli_truncate called for '%s'", path);

  watdfs_handle *h = find_handle(path);
  if (h) {
    int acc = (h->flags & O_ACCMODE);
    // return -EMFILE when read only
    if (acc == O_RDONLY)
      return -EMFILE;

    if (ftruncate(h->cache_fd, newsize) < 0) {
      return -errno;
    }

    struct stat st{};
    if (fstat(h->cache_fd, &st) == 0) {
      h->T_client = st.st_mtime;
      h->dirty = true;
    }

    // Apply freshness rule for writes at end of truncate
    WatdfsClientState *stp = reinterpret_cast<WatdfsClientState *>(userdata);

    time_t now = time(nullptr);
    bool fresh_time = (stp && stp->cache_interval > 0)
                          ? ((now - h->Tc) < stp->cache_interval)
                          : true;

    // If freshness by time expired, consult server for T_server before
    // comparing
    if (!fresh_time) {
      struct stat s_stat{};
      int s_rc = rpc_getattr_remote(path, &s_stat);
      if (s_rc == 0) {
        h->T_server = s_stat.st_mtime;
      }
    }

    if (!fresh_time && h->T_client != h->T_server) {
      struct fuse_file_info rfi = {};
      rfi.flags = O_WRONLY;

      struct fuse_file_info use_fi = h->has_remote_writer ? h->remote_fi : rfi;

      int rc = upload_from_cache(path, h->cache_fd, &use_fi);
      if (rc == 0) {
        int ut_rc = rpc_utimensat_from_stat(path, &st);
        if (ut_rc < 0) {
          DLOG("rpc_utimensat_from_stat not successful");
        }
        h->T_server = h->T_client;
        h->Tc = now;
        h->dirty = false;
      } else {
        DLOG("upload_from_cache not successful");
      }
    } else if (!fresh_time) {
      h->Tc = now;
    }

    return 0;
  }

  WatdfsClientState *stp = reinterpret_cast<WatdfsClientState *>(userdata);
  std::string local_path = cache_full_path(stp, path);

  // Ensure we operate on a fresh local copy
  struct stat s_stat{};
  int dl_rc = download_to_cache(path, local_path, &s_stat);
  if (dl_rc < 0) {
    return dl_rc;
  }

  int fd = ::open(local_path.c_str(), O_RDWR);
  if (fd < 0) {
    return -errno;
  }

  int rc = 0;
  if (ftruncate(fd, newsize) < 0) {
    rc = -errno;
  } else {
    // Push updated contents and times back to server
    int up_rc = upload_from_cache(path, fd, nullptr);
    if (up_rc < 0) {
      rc = up_rc;
    } else {
      struct stat lst{};
      if (fstat(fd, &lst) == 0) {
        rpc_utimensat_from_stat(path, &lst);
      }
    }
  }

  ::close(fd);
  return rc;
}

int watdfs_cli_fsync(void *userdata, const char *path,
                     struct fuse_file_info *fi) {
  // Force a flush of file data from cache to server.
  DLOG("watdfs_cli_fsync called for '%s'", path);
  watdfs_handle *h = reinterpret_cast<watdfs_handle *>(fi->fh);
  if (!h) {
    return -EINVAL;
  }

  DLOG("watdfs_handle exists");

  int acc = (h->flags & O_ACCMODE);

  if (acc == O_RDONLY) {
    return -EMFILE;
  }

  struct fuse_file_info remote_fi = {};
  remote_fi.flags = O_WRONLY;

  // Use reserved remote writer handle if available
  struct fuse_file_info use_fi =
      h->has_remote_writer ? h->remote_fi : remote_fi;
  int rc = upload_from_cache(path, h->cache_fd, &use_fi);

  DLOG("upload_from_cache successful");

  if (rc == 0) {
    struct stat lst;
    if (fstat(h->cache_fd, &lst) == 0) {
      DLOG("fstat successful");

      // After successful upload, push updated metadata to server
      int ut_rc = rpc_utimensat_from_stat(path, &lst);
      if (ut_rc < 0) {
        rc = ut_rc;
      }

      h->T_client = lst.st_mtime;
      if (rc == 0) {
        h->T_server = h->T_client;
        h->dirty = false;
      }

      h->Tc = time(nullptr);
    } else {
      DLOG("fstat not successful");
    }
  }

  return rc;
}

int watdfs_cli_utimensat(void *userdata, const char *path,
                         const struct timespec ts[2]) {
  // Change file access and modification times locally; propagate on
  // fsync/release.
  DLOG("watdfs_cli_utimensat called for '%s'", path);
  watdfs_handle *h = find_handle(path);
  if (h) {
    int acc = (h->flags & O_ACCMODE);
    // return -EMFILE when read only
    if (acc == O_RDONLY)
      return -EMFILE;

    // Use futimens if available via fd
    if (::futimens(h->cache_fd, ts) < 0) {
      return -errno;
    }

    struct stat st{};
    if (fstat(h->cache_fd, &st) == 0) {
      h->T_client = st.st_mtime;
      h->dirty = true;
    }
    // Apply freshness rule at end of metadata update
    WatdfsClientState *stp = reinterpret_cast<WatdfsClientState *>(userdata);

    time_t now = time(nullptr);
    bool fresh_time = (stp && stp->cache_interval > 0)
                          ? ((now - h->Tc) < stp->cache_interval)
                          : true;

    if (!fresh_time) {
      // Consult server for T_server
      struct stat s_stat{};
      int s_rc = rpc_getattr_remote(path, &s_stat);
      if (s_rc == 0) {
        h->T_server = s_stat.st_mtime;
      }
    }

    if (!fresh_time && h->T_client != h->T_server) {
      struct fuse_file_info rfi = {};
      rfi.flags = O_WRONLY;

      struct fuse_file_info use_fi = h->has_remote_writer ? h->remote_fi : rfi;

      int up_rc = upload_from_cache(path, h->cache_fd, &use_fi);
      if (up_rc < 0) {
        return up_rc;
      }

      int ut_rc = rpc_utimensat_from_stat(path, &st);

      h->T_server = h->T_client;
      h->Tc = now;
      h->dirty = false;
    } else if (!fresh_time) {
      h->Tc = now;
    }
    return 0;
  }

  WatdfsClientState *stp = reinterpret_cast<WatdfsClientState *>(userdata);
  std::string local_path = cache_full_path(stp, path);

  // Ensure we have a local cached copy to operate on
  struct stat s_stat{};
  int dl_rc = download_to_cache(path, local_path, &s_stat);
  if (dl_rc < 0) {
    return dl_rc;
  }

  if (::utimensat(AT_FDCWD, local_path.c_str(), ts, 0) < 0) {
    return -errno;
  }

  // Push updated times to the server
  struct stat lst{};
  if (::stat(local_path.c_str(), &lst) == 0) {
    rpc_utimensat_from_stat(path, &lst);
  }

  return 0;
}
