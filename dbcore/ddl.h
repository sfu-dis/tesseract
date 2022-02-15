#pragma once

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
typedef std::function<varstr *(varstr *key, varstr &value, str_arena *arena,
                               uint64_t schema_version, FID fid, OID oid)>
    Reformat;

// Schema constraint function
typedef std::function<bool(varstr &value, uint64_t schema_version)> Constraint;

// DDL type
enum ddl_type {
  COPY_ONLY,
  VERIFICATION_ONLY,
  COPY_VERIFICATION,
  NO_COPY_VERIFICATION,
};

ddl_type ddl_type_map(uint32_t type);

enum schema_state_type {
  READY,
  NOT_READY,
};

// struct of DDL executor parameters
struct ddl_executor_paras {
  // New schema version
  uint64_t new_v;

  // Old schema version
  uint64_t old_v;

  // DDL type
  ddl_type type;

  // Schema reformation function index
  uint64_t reformat_idx;

  // Schema constraint function index
  uint64_t constraint_idx;

  // New table descriptor
  TableDescriptor *new_td;

  // Old table descriptor;
  TableDescriptor *old_td;

  // Index
  OrderedIndex *index;

  // State
  schema_state_type state;

  // Schema secondary index key creation function index
  uint64_t secondary_index_key_create_idx;

  // Whether handle insert
  bool handle_insert;

  // Whether handle update
  bool handle_update;

  // Schema reformation function index when scanning
  uint64_t scan_reformat_idx;

  ddl_executor_paras(uint64_t new_v, uint64_t old_v, ddl_type type,
                     uint64_t reformat_idx, uint64_t constraint_idx,
                     TableDescriptor *new_td, TableDescriptor *old_td,
                     OrderedIndex *index, schema_state_type state,
                     uint64_t secondary_index_key_create_idx,
                     bool handle_insert, bool handle_update,
                     uint64_t scan_reformat_idx)
      : new_v(new_v), old_v(old_v), type(type), reformat_idx(reformat_idx),
        constraint_idx(constraint_idx), new_td(new_td), old_td(old_td),
        index(index), state(state),
        secondary_index_key_create_idx(secondary_index_key_create_idx),
        handle_insert(handle_insert), handle_update(handle_update),
        scan_reformat_idx(scan_reformat_idx) {}
};

class ddl_executor {
  friend class transaction;

private:
  // List of DDL executor parameters
  std::vector<struct ddl_executor_paras *> ddl_executor_paras_list;

  // CDC workers
  std::vector<ermia::thread::Thread *> cdc_workers;

  // Scan workers
  std::vector<ermia::thread::Thread *> scan_workers;

public:
  // Constructor and destructor
  ddl_executor() {}
  ~ddl_executor() {}

  inline void add_ddl_executor_paras(
      uint64_t new_v, uint64_t old_v, ddl_type type, uint64_t reformat_idx,
      uint64_t constraint_idx, TableDescriptor *new_td, TableDescriptor *old_td,
      OrderedIndex *index, schema_state_type state,
      uint64_t secondary_index_key_create_idx = -1, bool handle_insert = true,
      bool handle_update = true, uint64_t scan_reformat_idx = -1) {
    ddl_executor_paras_list.push_back(new ddl_executor_paras(
        new_v, old_v, type, reformat_idx, constraint_idx, new_td, old_td, index,
        state, secondary_index_key_create_idx, handle_insert, handle_update,
        scan_reformat_idx));
  }

  inline void
  set_cdc_workers(std::vector<ermia::thread::Thread *> _cdc_workers) {
    cdc_workers = _cdc_workers;
  }

  inline std::vector<ermia::thread::Thread *> get_cdc_workers() {
    return cdc_workers;
  }

  inline void join_scan_workers() {
    for (std::vector<thread::Thread *>::const_iterator it =
             scan_workers.begin();
         it != scan_workers.end(); ++it) {
      (*it)->Join();
      thread::PutThread(*it);
    }
  }

  // Scan and do operations (copy, verification)
  rc_t scan(transaction *t, str_arena *arena);

  // CDC
  rc_t changed_data_capture_impl(transaction *t, uint32_t thread_id,
                                 uint32_t ddl_thread_id, uint32_t begin_log,
                                 uint32_t end_log, str_arena *arena,
                                 bool *ddl_end, uint32_t count);

  // Build map for join and aggregation
  rc_t build_map(transaction *t, str_arena *arena, TableDescriptor *td);
};

extern std::vector<Reformat> reformats;
extern std::vector<Constraint> constraints;
extern std::vector<uint32_t> ddl_worker_logical_threads;

} // namespace ddl

} // namespace ermia
