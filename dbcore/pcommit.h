#pragma once

#include "sm-config.h"
#include "sm-thread.h"

namespace ermia {

namespace pcommit {

static const uint64_t DIRTY_FLAG = uint64_t{1} << 63;
extern uint64_t *_tls_durable_csn CACHE_ALIGNED;

struct commit_queue {
  struct Entry {
    uint64_t csn;
    uint64_t start_time;
    Entry() : csn(0), start_time(0) {}
  };
  Entry *queue;
  uint32_t start;
  uint32_t items;
  uint64_t total_latency_us;
  uint32_t length;
  uint32_t commits;
  uint64_t max_latency;
  mcs_lock lock;
  commit_queue()
      : start(0),
        items(0),
        total_latency_us(0),
        length(config::pcommit_queue_length),
        commits(0),
        max_latency(0) {
    queue = new Entry[length];
  }
  ~commit_queue() { delete[] queue; }
  void push_back(uint64_t csn, uint64_t start_time, bool *flush, bool *insert);
  inline uint32_t size() { return items; }
  void extend();
  void reset_start_time(uint64_t start_time);
};

class tls_committer {
 private:
  // Same as log id and thread id
  uint32_t id;
  commit_queue *_commit_queue CACHE_ALIGNED;

 public:
  tls_committer() {}
  ~tls_committer() {}

  inline uint32_t get_queue_size() { return _commit_queue->size(); }

  inline uint64_t get_latency() { return _commit_queue->total_latency_us; }

  inline uint32_t get_commits() { return _commit_queue->commits; }

  // Mark committer as ongoing: some log blocks have not been durable
  inline void set_dirty_flag() {
    volatile_write(_tls_durable_csn[id], _tls_durable_csn[id] | DIRTY_FLAG);
  }

  // Get tls durable csn of this thread
  inline uint64_t get_tls_durable_csn() {
    return volatile_read(_tls_durable_csn[id]);
  }

  // Set tls durable csn of this thread
  inline void set_tls_durable_csn(uint64_t csn) {
    volatile_write(_tls_durable_csn[id], csn);
  }

  // Initialize a tls_committer object
  void initialize(uint32_t id);

  // Reset a tls_committer so commit queue is empty now
  void reset(bool set_zero);

  // Get the lowest tls durable csn among all threads
  uint64_t get_global_durable_csn();

  // Enqueue commit queue of this thread
  inline void enqueue_committed_xct(uint64_t csn, bool *flush, bool *insert) {
    util::timer t;
    _commit_queue->push_back(csn, t.get_start(), flush, insert);
  }

  // Dequeue commit queue of this thread
  void dequeue_committed_xcts();

  // Extend commit queue
  inline void extend_queue() { _commit_queue->extend(); }

  // Reset the start time of the latest added one,
  // only needed when config::commit_latency_only is false
  inline void reset_latest_start_time(uint64_t start_time) {
    _commit_queue->reset_start_time(start_time);
  }

  // Get max latency and reset to 0
  uint64_t get_and_reset_max_latency();
};

}  // namespace pcommit

}  // namespace ermia
