#pragma once

/* A high-performance log manager.
 *
 * By default this is used as an append-only log, but users can be creative and
 * use it to represent heap regions via third-party lambda plugins.
 *
 * The basic design is a distributed log consisting of multiple log files, each
 * of which owns a dedicated log buffer.
 */
#include <fcntl.h>
#include <glog/logging.h>
#include <liburing.h>
#include <numa.h>
#include <stdio.h>
#include <string.h>

#include <vector>

#include "dlog-defs.h"
#include "pcommit.h"

namespace ermia {

namespace dlog {

extern std::atomic<uint64_t> current_csn;

extern std::atomic<uint64_t> commit_csn;

void flush_all();

void dequeue_committed_xcts();

// Commit daemon function - only useful when dedicated commit thread is enabled
void commit_daemon();

void wakeup_commit_daemon();

void initialize();
void uninitialize();

extern std::thread *pcommit_thread;

// A segment of the log, i.e., a file
struct segment {
  // File descriptor for the underlying file
  int fd;

  // The (global) beginning address this segment covers
  uint64_t start_offset;

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

  // Whether do normal operations
  bool normal;

  // Whether doing DDL
  bool doing_ddl;

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

  // Create a new segment when the current segment is about to exceed the max
  // segment size.
  void create_segment();

 public:
  // Dummy ctor and dtor. The user must use initialize/uninitialize() to make
  // sure we capture the proper parameters set in ermia::config which may get
  // initialized/created after tls_logs are created.
  tls_log() {}
  ~tls_log() {}

  // Initialize/uninitialize this tls-log object
  void initialize(const char *log_dir, uint32_t log_id, uint32_t node,
                  uint64_t logbuf_mb, uint64_t max_segment_mb);
  void uninitialize();

  inline uint32_t get_id() { return id; }
  inline segment *get_segment(uint32_t segnum) { return &segments[segnum]; }

  inline int get_numa_node() { return numa_node; }

  inline bool is_dirty() { return dirty; }

  inline void set_dirty(bool _dirty) { dirty = _dirty; }

  inline bool is_normal() { return normal; }

  inline void set_normal(bool _normal) { normal = _normal; }

  inline bool is_doing_ddl() { return doing_ddl; }

  inline void set_doing_ddl(bool _doing_ddl) { doing_ddl = _doing_ddl; }

  inline std::vector<segment> *get_segments() { return &segments; }

  inline uint64_t get_logbuf_size() { return logbuf_size; }

  inline uint64_t get_latest_csn() { return latest_csn; }

  inline pcommit::tls_committer *get_committer() { return &tcommitter; }

  inline uint64_t get_latency() { return tcommitter.get_latency(); }

  inline tlog_lsn get_durable_lsn() { return durable_lsn; }

  // reset this committer
  inline void reset_committer(bool set_zero) { tcommitter.reset(set_zero); }

  // Allocate a log block in-place on the log buffer
  log_block *allocate_log_block(uint32_t payload_size, uint64_t *out_cur_lsn,
                                uint64_t *out_seg_num, uint64_t block_csn);

  // Enqueue commit queue
  void enqueue_committed_xct(uint64_t csn);

  // Dequeue commit queue
  inline void dequeue_committed_xcts() { tcommitter.dequeue_committed_xcts(); }

  inline uint32_t get_commit_queue_size() {
    return tcommitter.get_queue_size();
  }

  // Last flush
  void last_flush();

  inline void switch_log_buffers() {
    active_logbuf = (active_logbuf == logbuf[0]) ? logbuf[1] : logbuf[0];
    logbuf_offset = 0;
  }

  void issue_read(int fd, char *buf, uint64_t size, uint64_t offset);

  bool peek_read(char *buf, uint64_t size);

  // CDC flush
  void cdc_flush() { enqueue_flush(); }

  // Reset log buffer size
  void reset_logbuf(uint64_t logbuf_m);
};

extern std::vector<tls_log *> tlogs;

}  // namespace dlog

}  // namespace ermia
