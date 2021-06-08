#pragma once

#include "mcs_lock.h"
#include "sm-config.h"
#include "sm-thread.h"

namespace ermia {

namespace pcommit {

static const uint64_t DIRTY_FLAG = uint64_t{1} << 63;
extern uint64_t *_tls_durable_csn;
extern bool *_running_committer;
extern std::atomic<uint32_t> _running_committer_num;
extern std::atomic<uint64_t> _durable_csn;
extern mcs_lock _durable_csn_lock;

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
  uint64_t total_latency_us;
  commit_queue(uint32_t _id) : id(_id), start(0), items(0), total_latency_us(0) {
    queue = new Entry[config::group_commit_queue_length];
  }
  ~commit_queue() { delete[] queue; }
  void push_back(uint64_t csn, uint64_t start_time, bool *flush, bool *insert);
  inline uint32_t size() { return items; }
};

class tls_committer {
private:
  // Same as log id and thread id
  uint32_t commit_id;
  commit_queue *_commit_queue;
  bool running;

public:
  tls_committer() {}
  ~tls_committer() {}

  inline bool is_running() { return volatile_read(running); }

  inline uint64_t get_latency() { return _commit_queue->total_latency_us; }

  // Mark committer as ongoing: some log blocks have not been durable
  inline void set_dirty_flag() {
    volatile_write(_tls_durable_csn[commit_id], _tls_durable_csn[commit_id] | DIRTY_FLAG);
  }

  // Initialize a tls_committer object
  void initialize(uint32_t id);

  // Start a tls_committer
  void start();

  // Get the lowest tls durable csn among all threads
  uint64_t get_lowest_tls_durable_csn();

  // Get tls durable csn of this thread
  uint64_t get_tls_durable_csn();
  
  // Set tls durable csn of this thread
  void set_tls_durable_csn(uint64_t csn);

  // Enqueue commit queue of this thread
  void enqueue_committed_xct(uint64_t csn, uint64_t start_time, bool *flush, bool *insert);

  // Dequeue commit queue of this thread
  void dequeue_committed_xcts(uint64_t upto_csn, uint64_t end_time);
};

} // namespace pcommit

} // namespace ermia
