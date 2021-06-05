#pragma once

#include "mcs_lock.h"
#include "sm-config.h"
#include "sm-thread.h"

namespace ermia {

namespace pcommit {

extern uint64_t *_tls_durable_csn;

struct commit_queue {
  struct Entry {
    uint64_t csn;
    uint64_t start_time;
    Entry() : csn(0), start_time(0) {}
  };
  uint32_t id;
  Entry *queue;
  mcs_lock lock;
  uint32_t start;
  uint32_t items;
  static uint64_t total_latency_us;
  commit_queue(uint32_t _id) : id(_id), start(0), items(0) {
    queue = new Entry[config::group_commit_queue_length];
  }
  ~commit_queue() { delete[] queue; }
  void push_back(uint64_t csn, uint64_t start_time, bool *flush, bool *insert);
  inline uint32_t size() { return items; }
};

class tls_committer {
private:
  uint32_t commit_id;
  commit_queue *_commit_queue;

public:
  tls_committer() {}
  ~tls_committer() {}

  void initialize(uint32_t id);

  uint64_t get_lowest_tls_durable_csn();

  uint64_t get_tls_durable_csn(uint32_t id);
  
  void set_tls_durable_csn(uint64_t csn);

  void enqueue_committed_xct(uint64_t csn, uint64_t start_time, bool *flush, bool *insert);

  void dequeue_committed_xcts(uint64_t upto_csn, uint64_t end_time);
};

} // namespace pcommit

} // namespace ermia
