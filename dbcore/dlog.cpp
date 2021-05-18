#include <dirent.h>
#include "dlog.h"
#include "../macros.h"

// io_uring code based off of examples from https://unixism.net/loti/tutorial/index.html

namespace ermia {

namespace dlog {

// Segment file name template: tlog-id-segnum
#define SEGMENT_FILE_NAME_FMT "tlog-%08x-%08x"
#define SEGMENT_FILE_NAME_BUFSZ sizeof("tlog-01234567-01234567")

void tls_log::initialize(const char *log_dir, int log_id, int node, uint32_t logbuf_mb) {
  id = log_id;
  numa_node = node;
  flushing = false;
  logbuf_size = logbuf_mb * uint32_t{1024 * 1024};
  logbuf[0] = (char *)numa_alloc_onnode(logbuf_size, numa_node);
  LOG_IF(FATAL, !logbuf[0]) << "Unable to allocate log buffer";
  logbuf[1] = (char *)numa_alloc_onnode(logbuf_size, numa_node);
  LOG_IF(FATAL, !logbuf[1]) << "Unable to allocate log buffer";

  logbuf_offset = 0;
  active_logbuf = logbuf[0];
  durable_lsn = 0;
  current_lsn = 0;

  // Create a new segment
  char buf[SEGMENT_FILE_NAME_BUFSZ];
  size_t n = snprintf(buf, sizeof(buf), SEGMENT_FILE_NAME_FMT, id, (unsigned int)segments.size());
  DIR *logdir = opendir(log_dir);
  ALWAYS_ASSERT(logdir);
  segments.emplace_back(dirfd(logdir), buf);

  // Initialize io_uring
  int ret = io_uring_queue_init(2, &ring, 0);
  LOG_IF(FATAL, ret != 0) << "Error setting up io_uring";
}

void tls_log::uninitialize() {
  if (logbuf_offset) {
    issue_flush(active_logbuf, logbuf_offset);
    poll_flush();
  }
  io_uring_queue_exit(&ring);
}

void tls_log::issue_flush(const char *buf, uint32_t size) {
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
  io_uring_sqe_set_data(sqe, (void *)size);
  //sqe->user_data = size;

  int nsubmitted = io_uring_submit(&ring);
  LOG_IF(FATAL, nsubmitted != 1);
}

void tls_log::poll_flush() {
  struct io_uring_cqe *cqe = nullptr;
  int ret = io_uring_wait_cqe(&ring, &cqe);
  LOG_IF(FATAL, ret < 0) << "Error waiting for completion: " << strerror(-ret);
  LOG_IF(FATAL, cqe->res < 0) << "Error in async operation: " << strerror(-cqe->res);
  uint64_t size = cqe->user_data;
  io_uring_cqe_seen(&ring, cqe);
  durable_lsn += size;
  current_segment()->size += size;
  printf("DLSN %lu\n", durable_lsn);
}

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

segment::segment(int dfd, const char *segname) : size(0) {
  fd = openat(dfd, segname, O_RDWR | O_SYNC | O_CREAT | O_TRUNC, 0644);
  LOG_IF(FATAL, fd < 0);

  // Test a write
  //char BUF[16];
  //memset(BUF, 'a', 16);
  //write(fd, BUF, 16);

}

segment::~segment() {
  close(fd);
}

}  // namespace dlog

}  // namespace ermia
