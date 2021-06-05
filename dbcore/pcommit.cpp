#include <atomic>

#include "pcommit.h"
#include "sm-common.h"
#include "../macros.h"

namespace ermia {

namespace pcommit {

uint64_t *_tls_durable_csn =
    (uint64_t *)malloc(sizeof(uint64_t) * config::MAX_THREADS);
uint64_t commit_queue::total_latency_us = 0;

void commit_queue::push_back(uint64_t csn, uint64_t start_time, bool *flush, bool *insert) {
  CRITICAL_SECTION(cs, lock);
  if (items >= config::group_commit_queue_length * 0.8) {
    *flush = true;
    // printf("committer %u: retry\n", id);
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
  set_tls_durable_csn(0);
}

uint64_t tls_committer::get_lowest_tls_durable_csn() {
  uint64_t lowest_tls_durable_csn = volatile_read(_tls_durable_csn[0]);
  uint32_t j = 0;
  for (uint32_t i = 1; i < thread::next_thread_id; i++) {
    uint64_t csn = volatile_read(_tls_durable_csn[i]);
    if (csn) {
      if (lowest_tls_durable_csn < csn) {
        lowest_tls_durable_csn = csn;
	j = i;
      }
    }
  }
  // printf("lowest_tls_durable_csn: %lu, id: %u\n", lowest_tls_durable_csn, j);
  return lowest_tls_durable_csn;
}

uint64_t tls_committer::get_tls_durable_csn(uint32_t id) {
  return volatile_read(_tls_durable_csn[id]);
}

void tls_committer::set_tls_durable_csn(uint64_t csn) {
  volatile_write(_tls_durable_csn[commit_id], csn);
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
  // printf("commitid %u: %u, %u\n", commit_id, _commit_queue->items, _commit_queue->start);
}

} // namespace pcommit

} // namespace ermia
