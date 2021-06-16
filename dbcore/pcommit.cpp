#include <atomic>

#include "pcommit.h"
#include "sm-common.h"
#include "../macros.h"

namespace ermia {

namespace pcommit {

// Store tls durable csns
uint64_t *_tls_durable_csn =
    (uint64_t *)malloc(sizeof(uint64_t) * config::MAX_THREADS);

void commit_queue::push_back(uint64_t csn, uint64_t start_time, bool *flush, bool *insert) {
  if (items >= config::group_commit_queue_length * 0.8) {
    *flush = true;
  }
  if (items < config::group_commit_queue_length) {
    uint32_t idx = (start + items) % config::group_commit_queue_length;
    volatile_write(queue[idx].csn, csn);
    volatile_write(queue[idx].start_time, start_time);
    volatile_write(items, items + 1);
    ASSERT(items == size());
    *insert = false;
  }
}

void tls_committer::initialize(uint32_t id) {
  commit_id = id;
  _commit_queue = new commit_queue(id);
}

void tls_committer::reset() {
  _commit_queue = new commit_queue(commit_id);
  _tls_durable_csn[commit_id] = 0;
}

uint64_t tls_committer::get_lowest_tls_durable_csn() {
  bool found = false;
  uint64_t min_dirty = std::numeric_limits<uint64_t>::max();
  uint64_t max_clean = 0;
  for (uint32_t i = 0; i < config::MAX_THREADS; i++) {
    uint64_t csn = volatile_read(_tls_durable_csn[i]);
    if (csn) {  
      if (csn & DIRTY_FLAG) {
        min_dirty = std::min(csn & ~DIRTY_FLAG, min_dirty);
	found = true;
      } else 
	if (max_clean < csn) {
        max_clean = csn;
      }
    }
  }
  return found ? min_dirty : max_clean;
}

uint64_t tls_committer::get_tls_durable_csn() {
  return volatile_read(_tls_durable_csn[commit_id]);
}

void tls_committer::set_tls_durable_csn(uint64_t csn) {
  volatile_write(_tls_durable_csn[commit_id], csn);
}

void tls_committer::enqueue_committed_xct(uint64_t csn, uint64_t start_time, bool *flush, bool *insert) {
  _commit_queue->push_back(csn, start_time, flush, insert);
}

void tls_committer::dequeue_committed_xcts(uint64_t upto_csn, uint64_t end_time) {
  uint32_t n = volatile_read(_commit_queue->start);
  uint32_t size = _commit_queue->size();
  uint32_t dequeue = 0;
  for (uint32_t j = 0; j < size; ++j) {
    uint32_t idx = (n + j) % config::group_commit_queue_length;
    auto &entry = _commit_queue->queue[idx];
    if (volatile_read(entry.csn) > upto_csn) {
      break;
    }
    _commit_queue->total_latency_us += end_time - entry.start_time;
    dequeue++;
  }
  _commit_queue->items -= dequeue;
  volatile_write(_commit_queue->start, (n + dequeue) % config::group_commit_queue_length);
}

} // namespace pcommit

} // namespace ermia
