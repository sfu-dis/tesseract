#pragma once

#include <stdio.h>
#include <string.h>

#include "../str_arena.h"
#include "rcu.h"
#include "sm-config.h"
#include "sm-rc.h"
#include "sm-table.h"
#include "sm-thread.h"

namespace ermia {

class OrderedIndex;

namespace ddl {

// Schema reformation function
typedef std::function<varstr *(varstr &value, str_arena *arena,
                               uint64_t schema_version)>
    Reformat;

class ddl_executor {
private:
  // New schema version
  uint64_t new_v;

  // Old schema version
  uint64_t old_v;

  // Schema reformation function index
  uint64_t reformat_idx;

  // New table descriptor
  TableDescriptor *new_td;

  // Old table descriptor;
  TableDescriptor *old_td;

  // Index
  OrderedIndex *index;

  // State
  uint64_t state;

public:
  // Constructor and destructor
  ddl_executor(uint64_t _new_v, uint64_t _old_v, uint64_t _reformat_idx,
               TableDescriptor *_new_td, TableDescriptor *_old_td,
               OrderedIndex *_index, uint64_t _state) {
    new_v = _new_v;
    old_v = _old_v;
    reformat_idx = _reformat_idx;
    new_td = _new_td;
    old_td = _old_td;
    index = _index;
    state = _state;
  }
  ~ddl_executor() {}

  // Scan and copy
  rc_t scan_copy(transaction *t, str_arena *arena, varstr &value);

  // CDC
  bool changed_data_capture_impl(transaction *t, uint32_t thread_id,
                                 uint32_t ddl_thread_id, uint32_t begin_log,
                                 uint32_t end_log, str_arena *arena,
                                 util::fast_random &r);
};

extern std::vector<Reformat> reformats;

} // namespace ddl

} // namespace ermia
