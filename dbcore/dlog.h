#pragma once

/* A high-performance log manager.
 *
 * By default this is used as an append-only log, but users can be creative and
 * use it to represent heap regions via third-party lambda plugins.
 *
 * The basic design is a distributed log consisting of multiple log files, each
 * of which owns a dedicated log buffer.
 */
#include <vector>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <liburing.h>
#include <numa.h>

#include <glog/logging.h>

#include "dlog-defs.h"

namespace ermia {

namespace dlog {

extern std::atomic<uint64_t> current_csn;

// A segment of the log, i.e., a file
struct segment {
  // File descriptor for the underlying file
  int fd;

  // Amount of data that has been written
  uint32_t size;

  // ctor and dtor
  segment(int dfd, const char *segname);
  ~segment();
};

// A thread/transaction-local log which consists of one or multiple segments. No
// CC because this is per thread, and it is expected that no more than one
// transaction will be using the log at the same time.
class tls_log {
private:
  // ID of this log; can be seen as 'partition ID' -
  // caller/user should make sure this is unique
  uint32_t id;

  // Which NUMA node is this log supposed to run on?
  // This affect where the log buffers are allocated.
  int numa_node;

  // Is there an outstanding I/O flushing the log buffer?
  bool flushing;

  // Log buffer size in bytes
  uint64_t logbuf_size;

  // Two log buffers (double buffering)
  char *logbuf[2];

  // The log buffer accepting new writes
  char *active_logbuf;

  // Offset of the first available byte in the log buffer
  uint32_t logbuf_offset;

  // Durable LSN for this log
  tlog_lsn durable_lsn;

  // Current LSN for this log
  tlog_lsn current_lsn;

  // All segments belonging to this partition. The last one
  // segments[segments.size()-1] is the currently open segment
  std::vector<segment> segments;

  // io_uring structures
  struct io_uring ring;

private:
  // Get the currently open segment
  inline segment *current_segment() { return &segments[segments.size() - 1]; }

  // Issue an async I/O to flush the current active log buffer
  void issue_flush(const char *buf, uint32_t size);

  // Poll for log I/O completion. We only allow one active I/O at any time
  // (io_uring requests may come back out of order).
  void poll_flush();

public:
  // Dummy ctor and dtor. The user must use initialize/uninitialize() to make
  // sure we capture the proper parameters set in ermia::config which may get
  // initialized/created after tls_logs are created.
  tls_log() {}
  ~tls_log() {}

  // Initialize/uninitialize this tls-log object
  void initialize(const char *log_dir, uint32_t log_id, uint32_t node, uint32_t logbuf_mb);
  void uninitialize();

  // Get a tls-log instance (used by transaction commit)
  tls_log *get_tls_log();

  // Commit (insert) a log block to the log
  void insert(log_block *block);
};

}  // namespace dlog

}  // namespace ermia
