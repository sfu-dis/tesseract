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
  write_record_t(fat_ptr *entry, FID fid, OID oid) : entry(entry), fid(fid), oid(oid) {}
  write_record_t() : entry(nullptr), fid(0), oid(0) {}
  inline Object *get_object() { return (Object *)entry->offset(); }
};

struct write_set_t {
  static const uint32_t kMaxEntries = 256;
  uint32_t num_entries;
  write_record_t entries[kMaxEntries];
  write_set_t() : num_entries(0) {}
  inline void emplace_back(fat_ptr *oe, FID fid, OID oid) {
    ALWAYS_ASSERT(num_entries < kMaxEntries);
    new (&entries[num_entries]) write_record_t(oe, fid, oid);
    ++num_entries;
    ASSERT(entries[num_entries - 1].entry == oe);
  }
  inline uint32_t size() { return num_entries; }
  inline void clear() { num_entries = 0; }
  inline write_record_t &operator[](uint32_t idx) { return entries[idx]; }
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
  };

  inline bool is_read_mostly() { return flags & TXN_FLAG_READ_MOSTLY; }
  inline bool is_read_only() { return flags & TXN_FLAG_READ_ONLY; }

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

  inline void record_log_request(uint32_t payload_size) {
    log_size += (sizeof(dlog::log_record) + payload_size);
  }

  inline void add_to_write_set(fat_ptr *entry, FID fid, OID oid) {
#ifndef NDEBUG
    for (uint32_t i = 0; i < write_set.size(); ++i) {
      auto &w = write_set[i];
      ASSERT(w.entry);
      ASSERT(w.entry != entry);
    }
#endif
    write_set.emplace_back(entry, fid, oid);
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
  write_set_t write_set;
#if defined(SSN) || defined(SSI) || defined(MVOCC)
  read_set_t read_set;
#endif
};

}  // namespace ermia
