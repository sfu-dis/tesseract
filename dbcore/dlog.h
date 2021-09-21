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
#include "pcommit.h"

namespace ermia {

namespace dlog {

extern std::atomic<uint64_t> current_csn;

extern std::atomic<uint64_t> commit_csn;

void flush_all();

// A segment of the log, i.e., a file
struct segment {
  // File descriptor for the underlying file
  int fd;

  // Amount of data that has been written
  uint64_t size;

  // Amount of data that has been written and pending for flush
  uint64_t expected_size;

  // ctor and dtor
  segment(int dfd, const char *segname);
  ~segment();
};

// A thread/transaction-local log which consists of one or multiple segments. No
// CC because this is per thread, and it is expected that no more than one
// transaction will be using the log at the same time.
class tls_log {
private:
  // Directory where the segment files should be created.
  const char *dir;
  
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

  // Last csn for each log buffer
  uint64_t last_csns[2];

  // Latest csn
  uint64_t latest_csn;

  // The log buffer accepting new writes
  char *active_logbuf;

  // Offset of the first available byte in the log buffer
  uint32_t logbuf_offset;

  // Durable LSN for this log
  tlog_lsn durable_lsn;

  // Current LSN for this log
  tlog_lsn current_lsn;

  // Segment size
  uint64_t segment_size;

  // All segments belonging to this partition. The last one
  // segments[segments.size()-1] is the currently open segment
  std::vector<segment> segments;

  // io_uring structures
  struct io_uring ring;

  // Committer
  pcommit::tls_committer tcommitter;

  // Lock
  mcs_lock lock;

  // Whether dirty
  bool dirty;

private:
  // Get the currently open segment
  inline segment *current_segment() { return &segments[segments.size() - 1]; }

  // Do flush when doing enqueue commits
  void enqueue_flush();

  // Issue an async I/O to flush the current active log buffer
  void issue_flush(const char *buf, uint64_t size);

  // Poll for log I/O completion. We only allow one active I/O at any time
  // (io_uring requests may come back out of order).
  void poll_flush();

  // Create a new segment when the current segment is about to exceed the max segment size.
  void create_segment();

public:
  // Dummy ctor and dtor. The user must use initialize/uninitialize() to make
  // sure we capture the proper parameters set in ermia::config which may get
  // initialized/created after tls_logs are created.
  tls_log() {}
  ~tls_log() {}

  // Initialize/uninitialize this tls-log object
  void initialize(const char *log_dir, uint32_t log_id, uint32_t node, uint64_t logbuf_mb, uint64_t max_segment_mb);
  void uninitialize();

  inline uint32_t get_id() { return id; }

  inline bool is_dirty() { return dirty; }

  inline void set_dirty(bool _dirty) { dirty = _dirty; } 

  inline std::vector<segment> *get_segments() { return &segments; }

  inline uint64_t get_logbuf_size() { return logbuf_size; }

  inline uint64_t get_latest_csn() { return latest_csn; }

  inline pcommit::tls_committer *get_committer() { return &tcommitter; }

  inline uint64_t get_latency() { return tcommitter.get_latency(); }

  // reset this committer
  inline void reset_committer(bool zero) { tcommitter.reset(zero); }

  // Commit (insert) a log block to the log - [block] must *not* be allocated
  // using allocate_log_block.
  //void insert(log_block *block);

  // Allocate a log block in-place on the log buffer
  log_block *allocate_log_block(uint32_t payload_size, uint64_t *out_cur_lsn,
                                uint64_t *out_seg_num, uint64_t block_csn);

  void commit_log_block(log_block *block);

  // Enqueue commit queue
  void enqueue_committed_xct(uint64_t csn, uint64_t start_time);

  // Dequeue commit queue
  void wrap_dequeue_committed_xcts(bool is_last);

  // Last flush
  void last_flush();

  // CDC
  void cdc(transaction *t, uint64_t begin_csn, uint64_t end_csn, std::vector<char *> bufs);
};

extern tls_log *tlogs[config::MAX_THREADS];

}  // namespace dlog

}  // namespace ermia