#include <atomic>

#include "../macros.h"
#include "pcommit.h"
#include "sm-common.h"

namespace ermia {

namespace pcommit {

// Store tls durable csns
uint64_t *_tls_durable_csn =
    (uint64_t *)malloc(sizeof(uint64_t) * config::MAX_THREADS);

std::atomic<uint64_t> lowest_csn(0);

void commit_queue::push_back(uint64_t csn, uint64_t start_time, bool *flush,
                             bool *insert) {
  CRITICAL_SECTION(cs, lock);
  if (items >= group_commit_queue_length * 0.8) {
    *flush = true;
  }
  if (*insert && items < group_commit_queue_length) {
    uint32_t idx = (start + items) % group_commit_queue_length;
    volatile_write(queue[idx].csn, csn);
    volatile_write(queue[idx].start_time, start_time);
    volatile_write(items, items + 1);
    ASSERT(items == size());
    *insert = false;
  }
}

void commit_queue::extend() {
  group_commit_queue_length = group_commit_queue_length * 2;
  Entry *queue_tmp = new Entry[group_commit_queue_length];
  for (uint i = 0; i < (group_commit_queue_length / 2); i++) {
    if (volatile_read(queue[i].csn)) {
      queue_tmp[i].csn = volatile_read(queue[i].csn);
      queue_tmp[i].start_time = volatile_read(queue[i].start_time);
    }
  }
  Entry *queue_delete = queue;
  queue = queue_tmp;
  delete[] queue_delete;
}

void tls_committer::initialize(uint32_t id) {
  commit_id = id;
  _commit_queue = new commit_queue(id);
}

void tls_committer::reset(bool set_zero) {
  _commit_queue->~commit_queue();
  _commit_queue = new commit_queue(commit_id);
  if (set_zero) {
    printf("%u set all 0\n", commit_id);
    memset(_tls_durable_csn, 0, sizeof(uint64_t) * config::MAX_THREADS);
  } else {
    printf("%u set lowest csn\n", commit_id);
    _tls_durable_csn[commit_id] = lowest_csn.load(std::memory_order_relaxed);
  }
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
      } else if (max_clean < csn) {
        max_clean = csn;
      }
    }
  }
  uint64_t lowest_tls_durable_csn = found ? min_dirty : max_clean;
  lowest_csn.store(lowest_tls_durable_csn, std::memory_order_seq_cst);
  return lowest_tls_durable_csn;
}

void tls_committer::enqueue_committed_xct(uint64_t csn, uint64_t start_time,
                                          bool *flush, bool *insert) {
  _commit_queue->push_back(csn, start_time, flush, insert);
}

void tls_committer::dequeue_committed_xcts(uint64_t upto_csn,
                                           uint64_t end_time) {
  CRITICAL_SECTION(cs, _commit_queue->lock);
  uint32_t n = volatile_read(_commit_queue->start);
  uint32_t size = _commit_queue->size();
  uint32_t dequeue = 0;
  for (uint32_t j = 0; j < size; ++j) {
    uint32_t idx = (n + j) % _commit_queue->group_commit_queue_length;
    auto &entry = _commit_queue->queue[idx];
    if (volatile_read(entry.csn) > upto_csn) {
      break;
    }
    _commit_queue->total_latency_us += end_time - entry.start_time;
    dequeue++;
  }
  _commit_queue->items -= dequeue;
  volatile_write(_commit_queue->start,
                 (n + dequeue) % _commit_queue->group_commit_queue_length);
}

void tls_committer::extend_queue() { _commit_queue->extend(); }

} // namespace pcommit

} // namespace ermia
