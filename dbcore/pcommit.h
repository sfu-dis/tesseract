#pragma once

#include "mcs_lock.h"
#include "sm-config.h"
#include "sm-thread.h"

namespace ermia {

namespace pcommit {

static const uint64_t DIRTY_FLAG = uint64_t{1} << 63;
extern uint64_t *_tls_durable_csn CACHE_ALIGNED;
extern std::atomic<uint64_t> lowest_csn;

struct commit_queue {
  struct Entry {
    uint64_t csn;
    uint64_t start_time;
    Entry() : csn(0), start_time(0) {}
  };
  uint32_t id;
  Entry *queue;
  uint32_t start;
  uint32_t items;
  mcs_lock lock;
  uint64_t total_latency_us;
  uint32_t group_commit_queue_length;
  commit_queue(uint32_t _id)
      : id(_id), start(0), items(0), total_latency_us(0), 
	group_commit_queue_length(config::group_commit_queue_length) {
    queue = new Entry[group_commit_queue_length];
  }
  ~commit_queue() { delete[] queue; }
  void push_back(uint64_t csn, uint64_t start_time, bool *flush, bool *insert);
  inline uint32_t size() { return items; }
  void extend() {
    group_commit_queue_length = group_commit_queue_length * 2;
    Entry *queue_tmp = new Entry[group_commit_queue_length];
    for (uint i = 0; i < (group_commit_queue_length / 2); i++) {
      queue_tmp[i].csn = queue[i].csn;
      queue_tmp[i].start_time = queue[i].start_time;
    }
    Entry *queue_delete = queue;
    queue = queue_tmp;
    delete[] queue_delete;
  }
};

class tls_committer {
private:
  // Same as log id and thread id
  uint32_t commit_id;
  commit_queue *_commit_queue CACHE_ALIGNED;

public:
  tls_committer() {}
  ~tls_committer() {}

  inline uint32_t get_queue_size() { return _commit_queue->size(); }

  inline uint64_t get_latency() { return _commit_queue->total_latency_us; }

  // Mark committer as ongoing: some log blocks have not been durable
  inline void set_dirty_flag() {
    volatile_write(_tls_durable_csn[commit_id],
                   _tls_durable_csn[commit_id] | DIRTY_FLAG);
  }

  // Get tls durable csn of this thread
  inline uint64_t get_tls_durable_csn() {
    return volatile_read(_tls_durable_csn[commit_id]);
  }

  // Set tls durable csn of this thread
  inline void set_tls_durable_csn(uint64_t csn) {
    volatile_write(_tls_durable_csn[commit_id], csn);
  }

  // Initialize a tls_committer object
  void initialize(uint32_t id);

  // Reset a tls_committer to get a real latency for workloads
  void reset(bool zero);

  // Get the lowest tls durable csn among all threads
  uint64_t get_lowest_tls_durable_csn();

  // Enqueue commit queue of this thread
  void enqueue_committed_xct(uint64_t csn, uint64_t start_time, bool *flush,
                             bool *insert);

  // Dequeue commit queue of this thread
  void dequeue_committed_xcts(uint64_t upto_csn, uint64_t end_time);

  // Extend commit queue
  void extend_queue();
};

} // namespace pcommit

} // namespace ermia
