#include <atomic>

#include "pcommit.h"
#include "sm-common.h"
#include "../macros.h"

namespace ermia {

namespace pcommit {

static const uint64_t DIRTY_FLAG = uint64_t{1} << 63;

// Store tls durable csns
uint64_t *_tls_durable_csn =
    (uint64_t *)malloc(sizeof(uint64_t) * config::MAX_THREADS);

// Mark running committers as true, false otherwise
bool *_running_committer =
    (bool *)malloc(sizeof(bool) * config::MAX_THREADS);

// Store the number of running committer
std::atomic<uint32_t> _running_committer_num(0);

// Store the largest durable csn among all threads
std::atomic<uint64_t> _durable_csn(0);
mcs_lock _durable_csn_lock;

uint64_t commit_queue::total_latency_us = 0;

void commit_queue::push_back(uint64_t csn, uint64_t start_time, bool *flush, bool *insert) {
  CRITICAL_SECTION(cs, lock);
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
  running = false;
  _commit_queue = new commit_queue(id);
  set_tls_durable_csn(0);
}

void tls_committer::start() {
  volatile_write(running, true);
  
  if (_running_committer_num.load(std::memory_order_relaxed) == 0) {
    memset(_tls_durable_csn, 0, sizeof(uint64_t) * config::MAX_THREADS);
    memset(_running_committer, false, sizeof(bool) * config::MAX_THREADS);
  }

  _running_committer_num.fetch_add(1);

  volatile_write(_running_committer[commit_id], true);
  
  // Wait until all running committers are ready
  while (_running_committer_num.load(std::memory_order_relaxed) < 
		  config::worker_threads) {
    continue;
  }

  ALWAYS_ASSERT(volatile_read(_running_committer[commit_id]) != 0);
}

uint64_t tls_committer::get_lowest_tls_durable_csn() {
  bool found = false;
  uint64_t min_dirty = _durable_csn.load(std::memory_order_relaxed);
  uint64_t max_clean = 0;
  for (uint32_t i = 0; i < config::MAX_THREADS; i++) {
    uint64_t csn = volatile_read(_tls_durable_csn[i]);
    if (csn && volatile_read(_running_committer[i])) {
      if (csn & DIRTY_FLAG) {
        min_dirty = std::min(csn & ~DIRTY_FLAG, min_dirty);
	found = true;
      } else if (max_clean < csn) {
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
  CRITICAL_SECTION(cs, _durable_csn_lock);
  if (_durable_csn.load(std::memory_order_relaxed) < csn) {
    _durable_csn.store(csn, std::memory_order_relaxed);
  }
}

void tls_committer::set_dirty_flag() {
  uint64_t *my_csn = &_tls_durable_csn[commit_id];
  volatile_write(*my_csn, *my_csn | DIRTY_FLAG);
}

void tls_committer::enqueue_committed_xct(uint64_t csn, uint64_t start_time, bool *flush, bool *insert) {
  _commit_queue->push_back(csn, start_time, flush, insert);
}

void tls_committer::dequeue_committed_xcts(uint64_t upto_csn, uint64_t end_time) {
  CRITICAL_SECTION(cs, _commit_queue->lock);
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
