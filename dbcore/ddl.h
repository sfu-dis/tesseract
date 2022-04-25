#pragma once

#include "../str_arena.h"
#include "rcu.h"
#include "sm-config.h"
#include "sm-rc.h"
#include "sm-table.h"
#include "sm-thread.h"

namespace ermia {

class OrderedIndex;
struct write_set_t;
#if defined(SIDDL) || defined(BLOCKDDL)
struct ddl_write_set_t;
#endif

namespace ddl {

// In case table scan is too slow, stop it when a DDL starts
extern volatile bool ddl_start;
// For verification related DDL, if true, make some violations for 2nd round of CDC
extern volatile bool cdc_test;

#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
struct bitmap {
  FID fid;
  OID size;
  uint64_t *data CACHE_ALIGNED;
  bitmap(FID fid, OID size, uint64_t *data) : fid(fid), size(size), data(data) {}
};

bool migrate_record(FID fid, OID oid);
#endif

// Schema reformation function
typedef std::function<varstr *(varstr *key, varstr &value, str_arena *arena,
                               uint64_t schema_version, FID fid, OID oid,
                               transaction *t, uint64_t begin)>
    Reformat;

// Schema constraint function
typedef std::function<bool(varstr *key, varstr &value, str_arena *arena,
                           uint64_t schema_version, uint64_t csn)> Constraint;

// DDL flags
struct ddl_flags {
  volatile bool cdc_second_phase = false;
  volatile bool ddl_failed = false;
  volatile bool cdc_running = false;
  std::atomic<uint64_t> cdc_end_total{0};
  uint64_t *_tls_durable_lsn CACHE_ALIGNED =
      (uint64_t *)malloc(sizeof(uint64_t) * config::MAX_THREADS);
};

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
  COMPLETE,
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
  // Transaction where DDL executor resides
  transaction *t;

  // List of DDL executor parameters
  std::vector<struct ddl_executor_paras *> ddl_executor_paras_list;

  // CDC workers
  std::vector<ermia::thread::Thread *> cdc_workers;

  // Scan workers
  std::vector<ermia::thread::Thread *> scan_workers;

  // DDL type
  ddl_type dt;

  // DDL flags
  ddl_flags flags;

  // Old table descriptor
  TableDescriptor *old_td;

  // New table descriptors
  std::unordered_map<FID, TableDescriptor *> new_td_map;

  // Old table descriptors
  std::unordered_map<FID, TableDescriptor *> old_td_map;

#if defined(SIDDL) || defined(BLOCKDDL)
  // DDL write set
  ddl_write_set_t *ddl_write_set;
#endif

 public:
  // Constructor and destructor
  ddl_executor() : dt(ddl_type::INVALID) {
#if defined(SIDDL) || defined(BLOCKDDL)
    init_ddl_write_set();
#endif
  }
  ~ddl_executor() {
#if defined(SIDDL) || defined(BLOCKDDL)
    free_ddl_write_set();
#endif
  }

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

  inline void set_ddl_type(ddl_type _dt) { dt = _dt; }

  inline void join_scan_workers() {
    for (auto &w : scan_workers) {
      w->Join();
      thread::PutThread(w);
    }
    scan_workers.clear();
  }

  inline void join_cdc_workers() {
    for (auto &w : cdc_workers) {
      w->Join();
      thread::PutThread(w);
    }
    cdc_workers.clear();
  }

  inline ddl_flags *get_ddl_flags() { return &flags; }

  inline TableDescriptor *get_old_td() { return old_td; }

  inline void set_old_td(TableDescriptor *_old_td) { old_td = _old_td; }

  inline std::unordered_map<FID, TableDescriptor *> *get_new_td_map() {
    return &new_td_map;
  }

  inline std::unordered_map<FID, TableDescriptor *> *get_old_td_map() {
    return &old_td_map;
  }

  inline void add_new_td_map(TableDescriptor *new_td) {
    new_td_map[new_td->GetTupleFid()] = new_td;
  }

  inline void add_old_td_map(TableDescriptor *old_td) {
    old_td_map[old_td->GetTupleFid()] = old_td;
  }

  inline void set_transaction(transaction *_t) { t = _t; }

  // Scan and do operations (copy, verification)
  rc_t scan(str_arena *arena);

  // Scan impl
  rc_t scan_impl(str_arena *arena, OID oid, FID old_fid, TXN::xid_context *xc,
                 oid_array *old_tuple_array, oid_array *key_array,
                 dlog::log_block *lb, int wid, ddl_executor *ddl_exe);

  // DDL operations in commit
  rc_t commit_op(dlog::log_block *lb, uint64_t *lb_lsn, uint64_t *segnum);

  // Set schema records' states
  void set_schema_state(dlog::log_block *lb, uint64_t *lb_lsn, uint64_t *segnum, schema_state_type state);

#if defined(COPYDDL) && !defined(LAZYDDL)
  // CDC
  uint32_t changed_data_capture();

  // CDC impl
  rc_t changed_data_capture_impl(uint32_t thread_id, uint32_t ddl_thread_id,
                                 uint32_t begin_log, uint32_t end_log,
                                 str_arena *arena, bool *ddl_end,
                                 uint32_t count);
#endif

#if defined(SIDDL) || defined(BLOCKDDL)
  // Get DDL write set
  inline ddl_write_set_t *get_ddl_write_set() { return ddl_write_set; }

  // Init DDl write set
  void init_ddl_write_set();

  // Delete DDL write set
  void free_ddl_write_set();

  // DDL write set commit
  void ddl_write_set_commit(dlog::log_block *lb, uint64_t *out_cur_lsn, uint64_t *out_seg_num);

  // DDL write set abort
  void ddl_write_set_abort();
#endif
};

extern std::vector<Reformat> reformats;
extern std::vector<Constraint> constraints;
#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
extern mcs_lock *lazy_ddl_locks CACHE_ALIGNED;
extern std::vector<bitmap *> bitmaps;
#endif

}  // namespace ddl

}  // namespace ermia
