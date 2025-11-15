# WatDFS

FUSE-based virtual file system with a simple client-server architecture and client-side caching.

## Features

- POSIX-style interface implemented via FUSE
- Supported system calls:
	- open, creat
	- close
	- mknod
	- read, write
	- pread, pwrite
	- stat, lstat, fstat
	- truncate, ftruncate
	- fsync
	- utimensat

## Prerequisites

- Linux
- libfuse and fusermount
- g++ and make

On Debian/Ubuntu you can install dependencies with:

```bash
sudo apt-get update
sudo apt-get install -y g++ make pkg-config libfuse-dev fuse
```

## Build

Compile the project:

```bash
make clean all
```

## Run the server

1) Create a directory for the server’s backing store and start the server:

```bash
mkdir -p /tmp/$USER/server
./watdfs_server /tmp/$USER/server
```

2) From the server output, note the host and port, then export these for the client:

```bash
export SERVER_ADDRESS=<host-from-server-output>
export SERVER_PORT=<port-from-server-output>
```

## Run the client

Run the client in a separate terminal from the server.

```bash
# Required: address and port of the running server (from above)
export SERVER_ADDRESS=ubuntu1804-008
export SERVER_PORT=12345

# Client-side cache interval (seconds). Use any integer >= 0.
export CACHE_INTERVAL_SEC=5

# Create local cache and mount directories
mkdir -p /tmp/$USER/cache /tmp/$USER/mount

# Start the FUSE client
./watdfs_client -s -f -o direct_io /tmp/$USER/cache /tmp/$USER/mount
```

Flags used:
- `-s` runs FUSE single-threaded (useful for debugging/consistency in coursework).
- `-f` runs in the foreground so you can see logs.
- `-o direct_io` bypasses the kernel page cache to ensure expected consistency semantics.

To stop the client, press Ctrl+C in the client terminal.

## Environment variables

- `SERVER_ADDRESS`: Hostname or IP address where the server is running.
- `SERVER_PORT`: TCP port exposed by the server.
- `CACHE_INTERVAL_SEC`: Non-negative integer controlling the validity window for client-side cached entries (0 = always revalidate).

## Unmount

When you’re done, unmount the FUSE filesystem:

```bash
fusermount -u /tmp/$USER/mount
```

## Troubleshooting

• Error: “fuse: bad mount point 'client_mount_dir': Transport endpoint is not connected”

This usually means a previous FUSE instance did not terminate cleanly. Unmount the path:

```bash
fusermount -u /path/to/client_mount_dir
```
