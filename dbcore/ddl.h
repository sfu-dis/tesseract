#pragma once

#include "../str_arena.h"
#include "rcu.h"
#include "sm-config.h"
#include "sm-rc.h"
#include "sm-table.h"
#include "sm-thread.h"

namespace ermia {

class OrderedIndex;
#if defined(SIDDL) || defined(BLOCKDDL)
struct ddl_write_set_t;
#endif

namespace ddl {

extern volatile bool ddl_start;
extern volatile bool ddl_running;
extern volatile bool cdc_first_phase;
extern volatile bool cdc_second_phase;
extern volatile bool ddl_failed;
extern volatile bool cdc_running;
extern volatile bool ddl_td_set;
extern volatile bool cdc_test;
extern std::atomic<uint64_t> cdc_end_total;
extern uint64_t *_tls_durable_lsn CACHE_ALIGNED;

// Schema reformation function
typedef std::function<varstr *(varstr *key, varstr &value, str_arena *arena,
                               uint64_t schema_version, FID fid, OID oid)>
    Reformat;

// Schema constraint function
typedef std::function<bool(varstr &value, uint64_t schema_version)> Constraint;

// DDL type
enum ddl_type {
  INVALID,
  COPY_ONLY,
  VERIFICATION_ONLY,
  COPY_VERIFICATION,
  NO_COPY_VERIFICATION,
};

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
      : new_v(new_v),
        old_v(old_v),
        type(type),
        reformat_idx(reformat_idx),
        constraint_idx(constraint_idx),
        new_td(new_td),
        old_td(old_td),
        index(index),
        state(state),
        secondary_index_key_create_idx(secondary_index_key_create_idx),
        handle_insert(handle_insert),
        handle_update(handle_update),
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

  // DDL type
  ddl_type dt;

#if defined(SIDDL) || defined(BLOCKDDL)
  // DDL write set
  ddl_write_set_t *ddl_write_set;
#endif

 public:
  // Constructor and destructor
  ddl_executor() : dt(ddl_type::INVALID) {}
  ddl_executor(ddl_type dt) : dt(dt) {}
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

  inline ddl_type get_ddl_type() { return dt; }

  inline void join_scan_workers() {
    for (std::vector<thread::Thread *>::const_iterator it =
             scan_workers.begin();
         it != scan_workers.end(); ++it) {
      (*it)->Join();
      thread::PutThread(*it);
    }
    scan_workers.clear();
  }

  inline void join_cdc_workers() {
    for (std::vector<thread::Thread *>::const_iterator it = cdc_workers.begin();
         it != cdc_workers.end(); ++it) {
      (*it)->Join();
      thread::PutThread(*it);
    }
    cdc_workers.clear();
  }

  // Scan and do operations (copy, verification)
  rc_t scan(transaction *t, str_arena *arena);

  // Scan impl
  rc_t scan_impl(transaction *t, str_arena *arena, OID oid, FID old_fid,
                 TXN::xid_context *xc, oid_array *old_tuple_array,
                 oid_array *key_array, dlog::log_block *lb, int wid);

  // CDC
  uint32_t changed_data_capture(transaction *t);

  // CDC impl
  rc_t changed_data_capture_impl(transaction *t, uint32_t thread_id,
                                 uint32_t ddl_thread_id, uint32_t begin_log,
                                 uint32_t end_log, str_arena *arena,
                                 bool *ddl_end, uint32_t count);

#if defined(SIDDL) || defined(BLOCKDDL)
  // Get DDL write set
  inline ddl_write_set_t *get_ddl_write_set() { return ddl_write_set; }

  // Init DDl write set
  void init_ddl_write_set();

  // DDL write set commit
  void ddl_write_set_commit(transaction *t, dlog::log_block *lb,
                            uint64_t *out_cur_lsn, uint64_t *out_seg_num);

  // DDL write set abort
  void ddl_write_set_abort();
#endif
};

extern std::vector<Reformat> reformats;
extern std::vector<Constraint> constraints;

}  // namespace ddl

}  // namespace ermia
