#pragma once

#include <stdint.h>
#include <sys/types.h>

#include <vector>

#include "dbcore/dlog.h"
#include "dbcore/dlog-tx.h"
#include "dbcore/xid.h"
#include "dbcore/sm-config.h"
#include "dbcore/sm-oid.h"
#include "dbcore/sm-object.h"
#include "dbcore/sm-rc.h"
#include "masstree/masstree_btree.h"
#include "macros.h"
#include "str_arena.h"
#include "tuple.h"

#include <sparsehash/dense_hash_map>
using google::dense_hash_map;

extern volatile bool ddl_running_1;
extern volatile bool ddl_running_2;
extern std::atomic<uint64_t> ddl_end;
extern uint64_t *_tls_durable_lsn CACHE_ALIGNED;

namespace ermia {

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
  dlog::log_record::logrec_type type;
  const varstr *str;
  write_record_t(fat_ptr *entry, FID fid, OID oid, uint64_t size, dlog::log_record::logrec_type type, const varstr *str)
    : entry(entry), fid(fid), oid(oid), size(size), type(type), str(str) {}
  write_record_t() : entry(nullptr), fid(0), oid(0), size(0), type(dlog::log_record::logrec_type::INVALID), str(nullptr) {}
  inline Object *get_object() { return (Object *)entry->offset(); }
};

struct write_set_t {
  static const uint32_t kMaxEntries = 256, kMaxEntries_ = 50000000;
  uint32_t num_entries;
  write_record_t entries[kMaxEntries];
  write_record_t *entries_;
  mcs_lock lock;
  write_set_t() : num_entries(0) {}
  inline void emplace_back(bool is_ddl, fat_ptr *oe, FID fid, OID oid, uint32_t size,
                           dlog::log_record::logrec_type type, const varstr *str) {
    if (num_entries >= kMaxEntries_)
      printf("beyond\n");
    ALWAYS_ASSERT(num_entries < kMaxEntries_);
    if (is_ddl) {
      CRITICAL_SECTION(cs, lock);
      new (&entries_[num_entries]) write_record_t(oe, fid, oid, size, type, str);
      ++num_entries;
    } else {
      new (&entries[num_entries]) write_record_t(oe, fid, oid, size, type, str);
      ++num_entries;
    }
  }
  inline uint32_t size() { return num_entries; }
  inline void clear() { num_entries = 0; }
  inline write_record_t get(bool is_ddl, uint32_t idx) { return is_ddl ? entries_[idx] : entries[idx]; }
  inline void init_large_write_set() { entries_ = new write_record_t[kMaxEntries_]; }
};

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

    // A context-switch transaction doesn't enter/exit thread during construct/destruct.
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
  typedef dense_hash_map<const ConcurrentMasstree::node_opaque_t *, uint64_t > MasstreeAbsentSet;
  MasstreeAbsentSet masstree_absent_set;

 public:
  transaction(uint64_t flags, str_arena &sa, uint32_t coro_batch_idx);
  ~transaction() {}

  void uninitialize();

  inline void ensure_active() {
    // volatile_write(xc->state, TXN::TXN_ACTIVE);
    // ASSERT(state() == TXN::TXN_ACTIVE);
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

#ifdef COPYDDL
#if !defined(LAZYDDL) && !defined(DCOPYDDL)
  std::vector<ermia::thread::Thread *> changed_data_capture();
  void join_changed_data_capture_threads(
      std::vector<ermia::thread::Thread *> cdc_workers);
  inline void set_ddl_running_1(bool dr1) { ddl_running_1 = dr1; }
#endif
  bool DMLConsistencyHandler();
#endif
  
  bool MasstreeCheckPhantom();
  void Abort();

  // Insert a record to the underlying table
  OID Insert(TableDescriptor *td, const varstr *k, varstr *value, dbtuple **out_tuple = nullptr);

  rc_t Update(TableDescriptor *td, OID oid, const varstr *k, varstr *v);

  // Same as Update but without support for logging key
  inline rc_t Update(TableDescriptor *td, OID oid, varstr *v) {
    return Update(td, oid, nullptr, v);
  }

  void LogIndexInsert(OrderedIndex *index, OID oid, const varstr *key);

public:
  // Reads the contents of tuple into v within this transaction context
  rc_t DoTupleRead(dbtuple *tuple, varstr *out_v);

  // expected public overrides

  inline str_arena &string_allocator() { return *sa; }

  inline void add_to_write_set(bool is_ddl, fat_ptr *entry, FID fid, OID oid, uint64_t size, dlog::log_record::logrec_type type, const varstr *str) {
#ifndef NDEBUG
    for (uint32_t i = 0; i < is_ddl && write_set.size(); ++i) {
      auto &w = write_set.entries_[i];
      ASSERT(w.entry);
      ASSERT(w.entry != entry);
    }
#endif

    // Work out the encoded size to be added to the log block later
    uint64_t logrec_size = 0;
    if (type != dlog::log_record::logrec_type::OID_KEY) {
      logrec_size = align_up(size + sizeof(dbtuple) + sizeof(dlog::log_record));
    }
    uint64_t str_size = align_up(str->size() + sizeof(varstr) + sizeof(dlog::log_record));
    log_size += logrec_size + str_size;
    write_set.emplace_back(is_ddl, entry, fid, oid, logrec_size, type, str);
  }

  inline TXN::xid_context *GetXIDContext() { return xc; }

#ifdef COPYDDL
  inline void set_table_descriptors(TableDescriptor *_new_td, TableDescriptor *_old_td) { new_td = _new_td, old_td = _old_td; }
#endif

 protected:
  const uint64_t flags;
  XID xid;
  TXN::xid_context *xc;
  dlog::tls_log *log;
  uint64_t log_size;
  str_arena *sa;
  uint32_t coro_batch_idx; // its index in the batch
#ifdef COPYDDL
  std::unordered_map<TableDescriptor*, OID> schema_read_map;
  TableDescriptor *new_td;
  TableDescriptor *old_td;
#endif
  util::timer timer;
  write_set_t write_set;
#if defined(SSN) || defined(SSI) || defined(MVOCC)
  read_set_t read_set;
#endif
};

} // namespace ermia
