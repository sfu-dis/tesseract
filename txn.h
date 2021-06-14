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
  bool is_insert;
  write_record_t(fat_ptr *entry, FID fid, OID oid, uint64_t size, bool insert)
    : entry(entry), fid(fid), oid(oid), size(size), is_insert(insert) {}
  write_record_t() : entry(nullptr), fid(0), oid(0), size(0), is_insert(false) {}
  inline Object *get_object() { return (Object *)entry->offset(); }
};

struct write_set_t {
  static const uint32_t kMaxEntries = 256, kMaxEntries_ = 50000000;
  uint32_t num_entries;
  write_record_t entries[kMaxEntries];
  write_record_t *entries_;
  write_set_t() : num_entries(0) {}
  inline void emplace_back(bool is_ddl, fat_ptr *oe, FID fid, OID oid, uint32_t size, bool insert) {
    if (is_ddl) {
    ALWAYS_ASSERT(num_entries < kMaxEntries_);
    new (&entries_[num_entries]) write_record_t(oe, fid, oid, size, insert);
    ++num_entries;
    ASSERT(entries_[num_entries - 1].entry == oe);
    } else {
    ALWAYS_ASSERT(num_entries < kMaxEntries);
    new (&entries[num_entries]) write_record_t(oe, fid, oid, size, insert);
    ++num_entries;
    ASSERT(entries[num_entries - 1].entry == oe);
    }
  }
  inline uint32_t size() { return num_entries; }
  inline void clear() { num_entries = 0; }
  inline write_record_t get(bool is_ddl, uint32_t idx) { 
    if (is_ddl)
      return entries_[idx]; 
    else
      return entries[idx]; 
  }
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
  ~transaction();

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

  inline void add_to_write_set(bool is_ddl, fat_ptr *entry, FID fid, OID oid, uint64_t size, bool insert) {
#ifndef NDEBUG
    for (uint32_t i = 0; i < write_set.size(); ++i) {
      auto &w = write_set[i];
      ASSERT(w.entry);
      ASSERT(w.entry != entry);
    }
#endif

    // Work out the encoded size to be added to the log block later
    auto logrec_size = align_up(size + sizeof(dbtuple) + sizeof(dlog::log_record));
    log_size += logrec_size;
    write_set.emplace_back(is_ddl, entry, fid, oid, logrec_size, insert);
  }

  inline TXN::xid_context *GetXIDContext() { return xc; }

 protected:
  const uint64_t flags;
  XID xid;
  TXN::xid_context *xc;
  dlog::tls_log *log;
  uint32_t log_size;
  str_arena *sa;
  uint32_t coro_batch_idx; // its index in the batch
  std::unordered_map<TableDescriptor*, OID> schema_read_map;
  write_set_t write_set;
#if defined(SSN) || defined(SSI) || defined(MVOCC)
  read_set_t read_set;
#endif
};

}  // namespace ermia
