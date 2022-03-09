#pragma once

#include <stdint.h>
#include <sys/types.h>

#include <sparsehash/dense_hash_map>
#include <vector>

#include "dbcore/ddl.h"
#include "dbcore/dlog-tx.h"
#include "dbcore/dlog.h"
#include "dbcore/sm-config.h"
#include "dbcore/sm-object.h"
#include "dbcore/sm-oid.h"
#include "dbcore/sm-rc.h"
#include "dbcore/xid.h"
#include "macros.h"
#include "masstree/masstree_btree.h"
#include "str_arena.h"
#include "tuple.h"
using google::dense_hash_map;

namespace ermia {

struct schema_record;

#if defined(SSN) || defined(SSI)
#define set_tuple_xstamp(tuple, s)                                    \
  {                                                                   \
    uint64_t x;                                                       \
    do {                                                              \
      x = volatile_read(tuple->xstamp);                               \
    } while (x < s and                                                \
             not __sync_bool_compare_and_swap(&tuple->xstamp, x, s)); \
  }
#endif

// A write-set entry is essentially a pointer to the OID array entry
// begin updated. The write-set is naturally de-duplicated: repetitive
// updates will leave only one entry by the first update. Dereferencing
// the entry pointer results a fat_ptr to the new object.
struct write_record_t {
  fat_ptr *entry;
  FID fid;
  OID oid;
  uint64_t size;
  bool is_insert;
  write_record_t(fat_ptr *entry, FID fid, OID oid, uint64_t size, bool insert)
      : entry(entry), fid(fid), oid(oid), size(size), is_insert(insert) {}
  write_record_t()
      : entry(nullptr), fid(0), oid(0), size(0), is_insert(false) {}
  inline Object *get_object() { return (Object *)entry->offset(); }
};

struct write_record_block {
  static const uint32_t kMaxEntries = 256;
  write_record_t entries[kMaxEntries];
  uint32_t num_entries;
  write_record_block *next;
  write_record_block() : num_entries(0), next(nullptr) {}
  inline void emplace_back(fat_ptr *oe, FID fid, OID oid, uint32_t size,
                           bool insert) {
    ALWAYS_ASSERT(num_entries < kMaxEntries);
    new (&entries[num_entries]) write_record_t(oe, fid, oid, size, insert);
    ++num_entries;
    ASSERT(entries[num_entries - 1].entry == oe);
  }
  inline uint32_t size() { return num_entries; }
  inline void clear() { num_entries = 0; }
  inline write_record_t &operator[](uint32_t idx) { return entries[idx]; }
};

#if defined(SIDDL) || defined(BLOCKDDL)
struct write_record_block_info {
  write_record_block *first_block;
  write_record_block *cur_block;
  uint32_t total_entries;
  uint64_t log_size;
  write_record_block_info() : total_entries(0), log_size(0) {
    first_block = new ermia::write_record_block();
    cur_block = first_block;
  }
};

struct ddl_write_set_t {
  std::vector<write_record_block_info *> write_record_block_info_vec;
  inline void init() {
    for (uint32_t i = 0; i < config::scan_threads + config::cdc_threads; i++) {
      write_record_block_info_vec.push_back(new write_record_block_info());
    }
  }
  inline void emplace_back(fat_ptr *oe, FID fid, OID oid, uint32_t size,
                           bool insert, uint64_t logrec_size, int wid) {
    write_record_block_info *tmp = write_record_block_info_vec[wid];
    if (tmp->cur_block->size() == write_record_block::kMaxEntries) {
      tmp->cur_block->next = new write_record_block();
      tmp->cur_block = tmp->cur_block->next;
    }
    tmp->cur_block->emplace_back(oe, fid, oid, size, insert);
    tmp->total_entries++;
    tmp->log_size += logrec_size;
  }
  inline uint32_t size() {
    uint32_t total = 0;
    for (std::vector<write_record_block_info *>::const_iterator it =
             write_record_block_info_vec.begin();
         it != write_record_block_info_vec.end(); ++it) {
      total += (*it)->total_entries;
    }
    return total;
  }
};
#endif

class transaction {
  friend class ConcurrentMasstreeIndex;
  friend struct sm_oid_mgr;

 public:
  typedef TXN::txn_state txn_state;

#if defined(SSN) || defined(SSI) || defined(MVOCC)
  typedef std::vector<dbtuple *> read_set_t;
#endif

  enum {
    // use the low-level scan protocol for checking scan consistency,
    // instead of keeping track of absent ranges
    TXN_FLAG_LOW_LEVEL_SCAN = 0x1,

    // true to mark a read-only transaction- if a txn marked read-only
    // does a write, it is aborted. SSN uses it to implement to safesnap.
    // No bookeeping is done with SSN if this is enable for a tx.
    TXN_FLAG_READ_ONLY = 0x2,

    TXN_FLAG_READ_MOSTLY = 0x3,

    // A context-switch transaction doesn't enter/exit thread during
    // construct/destruct.
    TXN_FLAG_CSWITCH = 0x8,

    TXN_FLAG_DML = 0x10,

    TXN_FLAG_DDL = 0x20,
  };

  inline bool is_read_mostly() { return flags & TXN_FLAG_READ_MOSTLY; }
  inline bool is_read_only() { return flags & TXN_FLAG_READ_ONLY; }
  inline bool is_dml() { return flags & TXN_FLAG_DML; }
  inline bool is_ddl() { return flags & TXN_FLAG_DDL; }

 protected:
  inline txn_state state() const { return xc->state; }

  // the absent set is a mapping from (masstree node -> version_number).
  typedef dense_hash_map<const ConcurrentMasstree::node_opaque_t *, uint64_t>
      MasstreeAbsentSet;
  MasstreeAbsentSet masstree_absent_set;

 public:
  transaction(uint64_t flags, str_arena &sa, uint32_t coro_batch_idx);
  ~transaction() {}

  void uninitialize();

  inline void ensure_active() {
    volatile_write(xc->state, TXN::TXN_ACTIVE);
    ASSERT(state() == TXN::TXN_ACTIVE);
  }

  rc_t commit();
#ifdef SSN
  rc_t parallel_ssn_commit();
  rc_t ssn_read(dbtuple *tuple);
#elif defined SSI
  rc_t parallel_ssi_commit();
  rc_t ssi_read(dbtuple *tuple);
#elif defined MVOCC
  rc_t mvocc_commit();
  rc_t mvocc_read(dbtuple *tuple);
#else
  rc_t si_commit();
#endif

  bool DMLConsistencyHandler();

  bool MasstreeCheckPhantom();
  void Abort();

  // Insert a record to the underlying table
  OID Insert(TableDescriptor *td, varstr *value, dbtuple **out_tuple = nullptr);

  // DDL insert used for unoptimized lazy DDL
  OID DDLInsert(TableDescriptor *td, varstr *value,
                fat_ptr **out_entry = nullptr);

  // DDL scan insert
  void DDLScanInsert(TableDescriptor *td, OID oid, varstr *value,
                     dlog::log_block *block = nullptr);

  // DDL scan update
  void DDLScanUpdate(TableDescriptor *td, OID oid, varstr *value,
                     dlog::log_block *block = nullptr, int wid = -1);

  // DDL CDC insert
  PROMISE(rc_t)
  DDLCDCInsert(TableDescriptor *td, OID oid, varstr *value, uint64_t tuple_csn,
               dlog::log_block *block = nullptr);

  // DDL CDC update
  PROMISE(rc_t)
  DDLCDCUpdate(TableDescriptor *td, OID oid, varstr *value, uint64_t tuple_csn,
               dlog::log_block *block = nullptr);

  // Set DDL schema state to be Ready
  PROMISE(rc_t)
  DDLSchemaReady(TableDescriptor *td, OID oid, varstr *value);

  // DML & DDL overlap check
  PROMISE(bool)
  OverlapCheck(TableDescriptor *new_td, TableDescriptor *old_td, OID oid);

  PROMISE(rc_t)
  Update(TableDescriptor *td, OID oid, const varstr *k, varstr *v,
         int wid = -1);

  // Same as Update but without support for logging key
  inline PROMISE(rc_t)
      Update(TableDescriptor *td, OID oid, varstr *v, int wid = -1) {
    auto rc = AWAIT Update(td, oid, nullptr, v, wid);
    RETURN rc;
  }

  void LogIndexInsert(OrderedIndex *index, OID oid, const varstr *key);

  // Table scan for single record
  PROMISE(rc_t)
  table_scan_single(TableDescriptor *td, const varstr *key, OID &oid);

  // Table scan for multiple records
  PROMISE(void)
  table_scan_multi(
      TableDescriptor *td, const varstr *start_key, const varstr *end_key,
      ConcurrentMasstree::low_level_search_range_callback &callback);

  // Table reverse scan for multiple records
  PROMISE(void)
  table_rscan_multi(
      TableDescriptor *td, const varstr *start_key, const varstr *end_key,
      ConcurrentMasstree::low_level_search_range_callback &callback);

 public:
  // Reads the contents of tuple into v within this transaction context
  rc_t DoTupleRead(dbtuple *tuple, varstr *out_v);

  // expected public overrides

  inline str_arena &string_allocator() { return *sa; }

  inline void add_to_write_set(bool is_allowed, fat_ptr *entry, FID fid,
                               OID oid, uint64_t size, bool insert,
                               int wid = -1) {
#ifndef NDEBUG
    for (uint32_t i = 0; i < write_set.size(); ++i) {
      auto &w = write_set.entries[i];
      ASSERT(w.entry);
      ASSERT(w.entry != entry);
    }
#endif

    if (!is_ddl() || (is_ddl() && is_allowed)) {
      // Work out the encoded size to be added to the log block later
      auto logrec_size =
          align_up(size + sizeof(dbtuple) + sizeof(dlog::log_record));
#if defined(SIDDL) || defined(BLOCKDDL)
      if (wid >= 0 && is_ddl()) {
        auto *ddl_write_set = ddl_exe->get_ddl_write_set();
        ddl_write_set->emplace_back(entry, fid, oid, size + sizeof(dbtuple),
                                    insert, logrec_size, wid);
        return;
      }
#endif
      log_size += logrec_size;
      // Each write set entry still just records the size of the actual "data"
      // to be inserted to the log excluding dlog::log_record, which will be
      // prepended by log_insert/update etc.
      if (cur_write_record_block->size() == write_record_block::kMaxEntries) {
        cur_write_record_block->next = new write_record_block();
        cur_write_record_block = cur_write_record_block->next;
      }
      cur_write_record_block->emplace_back(entry, fid, oid,
                                           size + sizeof(dbtuple), insert);
    }
  }

  inline TXN::xid_context *GetXIDContext() { return xc; }

  inline void SetWaitForNewSchema(bool _wait_for_new_schema) {
    wait_for_new_schema = _wait_for_new_schema;
  }

  inline bool IsWaitForNewSchema() { return wait_for_new_schema; }

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

  inline void set_ddl_executor(ddl::ddl_executor *_ddl_exe) {
    ddl_exe = _ddl_exe;
#if defined(SIDDL) || defined(BLOCKDDL)
    ddl_exe->init_ddl_write_set();
#endif
  }

  inline ddl::ddl_executor *get_ddl_executor() {
    return ddl_exe;
  }

  inline dlog::tls_log *get_log() { return log; }

  enum lock_type { INVALID, SHARED, EXCLUSIVE };

  struct table_info {
    OID oid;
    FID fid;
    uint32_t version;
    lock_type lt;
    table_info() : oid(0), fid(0), version(0), lt(lock_type::INVALID) {}
    table_info(OID oid, FID fid, uint32_t version, lock_type lt)
        : oid(oid), fid(fid), version(version), lt(lt) {}
  };

  inline std::vector<table_info *> *get_table_set() { return &table_set; }

  inline void add_to_table_set(OID oid, FID fid, uint32_t version,
                               lock_type lt) {
    table_set.push_back(new table_info(oid, fid, version, lt));
  }

#ifdef BLOCKDDL
  inline void lock_table(FID table_fid, lock_type lt) {
    if (lt == lock_type::SHARED) {
      ReadLock(table_fid);
    } else if (lt == lock_type::EXCLUSIVE) {
      WriteLock(table_fid);
    }
    // Refresh begin timestamp after lock is granted
    xc->begin = dlog::current_csn.load(std::memory_order_relaxed);
  }

  inline void ReadLock(FID table_fid) {
    if (find(table_fid)) {
      return;
    }
    int ret = pthread_rwlock_rdlock(lock_map[table_fid]);
    LOG_IF(FATAL, ret);
    add_to_table_set(0, table_fid, 0, lock_type::SHARED);
  }

  inline void ReadUnlock(FID table_fid) {
    int ret = pthread_rwlock_unlock(lock_map[table_fid]);
    LOG_IF(FATAL, ret);
  }

  inline void WriteLock(FID table_fid) {
    table_info *l_info = find(table_fid);
    if (l_info && l_info->lt == lock_type::SHARED) {
      // Upgrade shared lock to exclusive lock
      ReadUnlock(table_fid);
    }
    if (l_info && l_info->lt == lock_type::EXCLUSIVE) {
      return;
    }
    int ret = pthread_rwlock_wrlock(lock_map[table_fid]);
    LOG_IF(FATAL, ret);
    add_to_table_set(0, table_fid, 0, lock_type::EXCLUSIVE);
  }

  inline void WriteUnlock(FID table_fid) {
    int ret = pthread_rwlock_unlock(lock_map[table_fid]);
    LOG_IF(FATAL, ret);
  }

  inline table_info *find(FID table_fid) {
    for (std::vector<table_info *>::const_iterator it = table_set.begin();
         it != table_set.end(); ++it) {
      if ((*it)->fid == table_fid) {
        return *it;
      }
    }
    return nullptr;
  }

  inline void UnlockAll() {
    for (std::vector<table_info *>::const_iterator it = table_set.begin();
         it != table_set.end(); ++it) {
      ReadUnlock((*it)->fid);
    }
  }

 public:
  static std::unordered_map<FID, pthread_rwlock_t *> lock_map;
  static std::mutex map_rw_latch;
#endif

 protected:
  const uint64_t flags;
  XID xid;
  TXN::xid_context *xc;
  dlog::tls_log *log;
  uint64_t log_size;
  str_arena *sa;
  uint32_t coro_batch_idx;  // its index in the batch
  std::vector<table_info *> table_set;
  std::unordered_map<FID, TableDescriptor *> new_td_map;
  TableDescriptor *old_td;
  std::unordered_map<FID, TableDescriptor *> old_td_map;
  bool wait_for_new_schema;
  ddl::ddl_executor *ddl_exe;
  util::timer timer;
  write_record_block *cur_write_record_block;
  write_record_block write_set;
#if defined(SSN) || defined(SSI) || defined(MVOCC)
  read_set_t read_set;
#endif
};

}  // namespace ermia
