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
typedef std::function<varstr *(varstr &value, str_arena *arena,
                               uint64_t schema_version)>
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

class ddl_executor {
private:
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
  uint64_t state;

  // New schema fat ptr
  fat_ptr new_schema_fat_ptr;

  // CDC workers
  std::vector<ermia::thread::Thread *> cdc_workers;

public:
  // Constructor and destructor
  ddl_executor(uint64_t _new_v, uint64_t _old_v, ddl_type _type,
               uint64_t _reformat_idx, uint64_t _constraint_idx,
               TableDescriptor *_new_td, TableDescriptor *_old_td,
               OrderedIndex *_index, uint64_t _state) {
    new_v = _new_v;
    old_v = _old_v;
    type = _type;
    reformat_idx = _reformat_idx;
    constraint_idx = _constraint_idx;
    new_td = _new_td;
    old_td = _old_td;
    index = _index;
    state = _state;
  }
  ~ddl_executor() {}

  inline ddl_type get_ddl_type() { return type; }

  inline void store_new_schema(varstr *value, TXN::xid_context *xc) {
    new_schema_fat_ptr = Object::Create(value, xc->begin_epoch);
  }

  inline fat_ptr get_new_schema_fat_ptr() { return new_schema_fat_ptr; }

  inline void
  set_cdc_workers(std::vector<ermia::thread::Thread *> _cdc_workers) {
    cdc_workers = _cdc_workers;
  }

  inline std::vector<ermia::thread::Thread *> get_cdc_workers() {
    return cdc_workers;
  }

  // Scan and do operations (copy, verfication)
  rc_t scan(transaction *t, str_arena *arena, varstr &value);

  // CDC
  rc_t changed_data_capture_impl(transaction *t, uint32_t thread_id,
                                 uint32_t ddl_thread_id, uint32_t begin_log,
                                 uint32_t end_log, str_arena *arena,
                                 bool *ddl_end);
};

extern std::vector<Reformat> reformats;
extern std::vector<Constraint> constraints;

} // namespace ddl

} // namespace ermia
