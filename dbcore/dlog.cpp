#include <atomic>
#include <dirent.h>

#include "dlog.h"
#include "sm-common.h"
#include "sm-config.h"
#include "../macros.h"

// io_uring code based off of examples from https://unixism.net/loti/tutorial/index.html

namespace ermia {

namespace dlog {

// Segment file name template: tlog-id-segnum
#define SEGMENT_FILE_NAME_FMT "tlog-%08x-%08x"
#define SEGMENT_FILE_NAME_BUFSZ sizeof("tlog-01234567-01234567")

tls_log *tlogs[config::MAX_THREADS];

std::atomic<uint64_t> current_csn(0);

std::mutex tls_log_lock;

void flush_all() {
  // Flush rest blocks
  for (uint i = 0; i < config::MAX_THREADS; i++) {
    tls_log *tlog = tlogs[i];
    if (tlog) {
      tlog->last_flush();
    }
  }

  // Dequeue rest txns
  for (uint i = 0; i < config::MAX_THREADS; i++) {
    tls_log *tlog = tlogs[i];
    if (tlog) {
      tlog->last_dequeue_committed_xcts();
    }
  }
}

void tls_log::initialize(const char *log_dir, uint32_t log_id, uint32_t node,
                         uint64_t logbuf_mb, uint64_t max_segment_mb) {
  std::lock_guard<std::mutex> lock(tls_log_lock);
  dir = log_dir;
  id = log_id;
  numa_node = node;
  flushing = false;
  logbuf_size = logbuf_mb * uint32_t{1024 * 1024};
  logbuf[0] = (char *)numa_alloc_onnode(logbuf_size, numa_node);
  LOG_IF(FATAL, !logbuf[0]) << "Unable to allocate log buffer";
  logbuf[1] = (char *)numa_alloc_onnode(logbuf_size, numa_node);
  LOG_IF(FATAL, !logbuf[1]) << "Unable to allocate log buffer";
  segment_size = max_segment_mb * uint32_t{1024 * 1024};
  LOG_IF(FATAL, segment_size > SEGMENT_MAX_SIZE) << "Unable to allocate log buffer";

  logbuf_offset = 0;
  active_logbuf = logbuf[0];
  durable_lsn = 0;
  current_lsn = 0;

  // Create a new segment
  create_segment();

  // Initialize io_uring
  int ret = io_uring_queue_init(16, &ring, 0);
  LOG_IF(FATAL, ret != 0) << "Error setting up io_uring: " << strerror(ret);

  // Initialize committer
  tcommitter.initialize(log_id);
}

void tls_log::uninitialize() {
  CRITICAL_SECTION(cs, lock);
  if (logbuf_offset) {
    issue_flush(active_logbuf, logbuf_offset);
    poll_flush();
  }
  io_uring_queue_exit(&ring);
}

void tls_log::enqueue_flush() {
  CRITICAL_SECTION(cs, lock);
  if (logbuf_offset) {
    issue_flush(active_logbuf, logbuf_offset);
    active_logbuf = (active_logbuf == logbuf[0]) ? logbuf[1] : logbuf[0];
    logbuf_offset = 0;
  }
}

void tls_log::last_flush() {
  CRITICAL_SECTION(cs, lock);
  if (flushing) {
    poll_flush();
    flushing = false;
  }

  if (logbuf_offset) {
    issue_flush(active_logbuf, logbuf_offset);
    active_logbuf = (active_logbuf == logbuf[0]) ? logbuf[1] : logbuf[0];
    logbuf_offset = 0;
    poll_flush();
    flushing = false;
  }
}

void tls_log::issue_flush(const char *buf, uint64_t size) {
  if (config::null_log_device) {
    durable_lsn += size;
    return;
  }

  if (flushing) {
    poll_flush();
    flushing = false;
  }

  // Issue an async I/O to flush the buffer into the current open segment
  flushing = true;

  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  LOG_IF(FATAL, !sqe);

  io_uring_prep_write(sqe, current_segment()->fd, buf, size, current_segment()->size);
  sqe->flags |= IOSQE_IO_LINK;

  // Encode data size which is useful upon completion (to add to durable_lsn)
  // Must be set after io_uring_prep_write (which sets user_data to 0)
  sqe->user_data = size;
  current_segment()->expected_size += size;

  int nsubmitted = io_uring_submit(&ring);
  LOG_IF(FATAL, nsubmitted != 1);
}

void tls_log::poll_flush() {
  if (config::null_log_device) {
    return;
  }

  struct io_uring_cqe *cqe = nullptr;
  int ret = io_uring_wait_cqe(&ring, &cqe);
  LOG_IF(FATAL, ret < 0) << "Error waiting for completion: " << strerror(-ret);
  LOG_IF(FATAL, cqe->res < 0) << "Error in async operation: " << strerror(-cqe->res);
  uint64_t size = cqe->user_data;
  io_uring_cqe_seen(&ring, cqe);
  durable_lsn += size;
  current_segment()->size += size;

  // get last tls durable csn
  uint64_t last_tls_durable_csn =
      (active_logbuf == logbuf[0]) ? last_csns[1] : last_csns[0];

  // set tls durable csn
  tcommitter.set_tls_durable_csn(last_tls_durable_csn);
  ALWAYS_ASSERT(tcommitter.get_tls_durable_csn() == last_tls_durable_csn);

  // get the lowest tls durable csn
  uint64_t lowest_tls_durable_csn = tcommitter.get_lowest_tls_durable_csn();

  // dequeue some committed txns
  util::timer t;
  tcommitter.dequeue_committed_xcts(lowest_tls_durable_csn, t.get_start());
}

void tls_log::create_segment() { 
  char buf[SEGMENT_FILE_NAME_BUFSZ];
  size_t n = snprintf(buf, sizeof(buf), SEGMENT_FILE_NAME_FMT, id, (unsigned int)segments.size());
  DIR *logdir = opendir(dir);
  ALWAYS_ASSERT(logdir);
  segments.emplace_back(dirfd(logdir), buf);
}

/*
void tls_log::insert(log_block *block) {
  if (block->total_size() + logbuf_offset > logbuf_size) {
    issue_flush(active_logbuf, logbuf_offset);
    active_logbuf = (active_logbuf == logbuf[0]) ? logbuf[1] : logbuf[0];
    logbuf_offset = 0;
  }
  memcpy(active_logbuf + logbuf_offset, block, block->total_size());
  logbuf_offset += block->total_size();
  current_lsn += block->total_size();
}
*/

log_block *tls_log::allocate_log_block(uint32_t payload_size,
                                       uint64_t *out_cur_lsn,
                                       uint64_t *out_seg_num,
                                       uint64_t block_csn) {
  if (payload_size == 0) {
    return nullptr;
  }

  CRITICAL_SECTION(cs, lock);
  tcommitter.set_dirty_flag();

  uint32_t alloc_size = payload_size + sizeof(log_block);
  LOG_IF(FATAL, alloc_size > logbuf_size) << "Total size too big";

  
  // If this allocated log block would span across segments, we need a new segment.
  bool create_new_segment = false;
  if (alloc_size + logbuf_offset + current_segment()->expected_size > segment_size) {
    create_new_segment = true; 
  } 

  // If the allocated size exceeds the available space in the active logbuf,
  // or we need to create a new segment for this log block,
  // flush the active logbuf, and switch to the other logbuf.
  if (alloc_size + logbuf_offset > logbuf_size || create_new_segment) {
    if (logbuf_offset) {
      issue_flush(active_logbuf, logbuf_offset);
      active_logbuf = (active_logbuf == logbuf[0]) ? logbuf[1] : logbuf[0];
      logbuf_offset = 0;
    }

    if (create_new_segment) {
      create_segment();
    }
  }

  log_block *lb = (log_block *)(active_logbuf + logbuf_offset);
  logbuf_offset += alloc_size;
  if (out_cur_lsn) {
    *out_cur_lsn = current_lsn;
  }
  current_lsn += alloc_size;

  if (out_seg_num) {
    *out_seg_num = segments.size() - 1;
  }

  // Store the latest csn of log block
  latest_csn = block_csn;

  if (active_logbuf == logbuf[0]) {
    last_csns[0] = block_csn;
  } else {
    last_csns[1] = block_csn;
  }

  new (lb) log_block(payload_size);
  lb->csn = block_csn;
  return lb;
}

void tls_log::commit_log_block(log_block *block) {
}

void tls_log::enqueue_committed_xct(uint64_t csn, uint64_t start_time) {
  if (config::null_log_device) {
    return;
  }

  bool flush = false;
  bool insert = true;
retry:
  if (flush) {
    for (uint i = 0; i < config::MAX_THREADS; i++) {
      tls_log *tlog = tlogs[i];
      if (tlog) {
        tlog->enqueue_flush();
      }
    }
    flush = false;
  }
  if (insert) {
    tcommitter.enqueue_committed_xct(csn, start_time, &flush, &insert);
    if (flush) {
      goto retry;
    }
  }
}

void tls_log::last_dequeue_committed_xcts() {
  // get the lowest tls durable csn
  uint64_t lowest_tls_durable_csn = tcommitter.get_lowest_tls_durable_csn();

  // dequeue some committed txns
  util::timer t;
  tcommitter.dequeue_committed_xcts(lowest_tls_durable_csn, t.get_start());
  ALWAYS_ASSERT(tcommitter.get_queue_size() == 0);
}

segment::segment(int dfd, const char *segname) : size(0), expected_size(0) {
  fd = openat(dfd, segname, O_RDWR | O_SYNC | O_CREAT | O_TRUNC, 0644);
  LOG_IF(FATAL, fd < 0);
}

segment::~segment() {
  if (fd >= 0) {
    close(fd);
  }
}

}  // namespace dlog

}  // namespace ermia
