#pragma once

#include "txn.h"
#include "varstr.h"
#include "engine_internal.h"
#include "../benchmarks/record/encoder.h"
#include <experimental/coroutine>
#include "../benchmarks/ddl-schemas.h"
#include "rwlatch.h"
#include "../benchmarks/tpcc.h"

namespace ermia {

extern bool is_loading;

dlog::tls_log *GetLog();

class Table;

extern TableDescriptor *schema_td;

class Engine {
private:
  std::unordered_map<std::string, ReaderWriterLatch*> lock_map;
  ReaderWriterLatch *map_rw_latch = new ReaderWriterLatch(); 

private:
  void LogIndexCreation(bool primary, FID table_fid, FID index_fid, const std::string &index_name);
  void CreateIndex(const char *table_name, const std::string &index_name, bool is_primary);

public:
  Engine();
  ~Engine() {}

  // All supported index types
  static const uint16_t kIndexConcurrentMasstree = 0x1;

  // Create a table without any index (at least yet)
  TableDescriptor *CreateTable(const char *name);

  // Create the primary index for a table
  inline void CreateMasstreePrimaryIndex(const char *table_name, const std::string &index_name) {
    CreateIndex(table_name, index_name, true);
  }

  // Create a secondary masstree index
  inline void CreateMasstreeSecondaryIndex(const char *table_name, const std::string &index_name) {
    CreateIndex(table_name, index_name, false);
  }

  inline transaction *NewTransaction(uint64_t txn_flags, str_arena &arena, transaction *buf, uint32_t coro_batch_idx = 0) {
    // Reset the arena here - can't rely on the benchmark/user code to do it
    arena.reset();
    new (buf) transaction(txn_flags, arena, coro_batch_idx);
    return buf;
  }

  inline rc_t Commit(transaction *t) {
    rc_t rc = t->commit();
    if (!rc.IsAbort()) {
      t->~transaction();
    }
    if (config::state == config::kStateLoading) {
      t->enqueue_committed_xct();
    }
    return rc;
  }

  inline void Abort(transaction *t) {
    t->Abort();
    t->~transaction();
  }

  inline bool BuildIndexMap(std::string table_name) {
    map_rw_latch->WLock();
    if (lock_map.find(table_name) != lock_map.end()) {
      map_rw_latch->WUnlock();
      return false; 
    } else {
      lock_map[table_name] = new ReaderWriterLatch();
      map_rw_latch->WUnlock();
      return true;
    }
  }

  inline void ReadLock(std::string table_name) {
    lock_map[table_name]->RLock();
  }

  inline void ReadUnlock(std::string table_name) {
    lock_map[table_name]->RUnlock();
  }

  inline void WriteLock(std::string table_name) {
    lock_map[table_name]->WLock();
  }

  inline void WriteUnlock(std::string table_name) {
    lock_map[table_name]->WUnlock();
  }
};

// User-facing table abstraction, operates on OIDs only
class Table {
private:
  TableDescriptor *td;

public:
  rc_t Insert(transaction &t, varstr *value, OID *out_oid);
  rc_t Update(transaction &t, OID oid, varstr &value);
  rc_t Read(transaction &t, OID oid, varstr *out_value);
  rc_t Remove(transaction &t, OID oid);
};

// User-facing concurrent Masstree
class ConcurrentMasstreeIndex : public OrderedIndex {
  friend struct sm_log_recover_impl;
  friend class sm_chkpt_mgr;

private:
  ConcurrentMasstree masstree_;

  struct SearchRangeCallback {
    SearchRangeCallback(OrderedIndex::ScanCallback &upcall)
        : upcall(&upcall), return_code(rc_t{RC_FALSE}) {}
    ~SearchRangeCallback() {}

    inline bool Invoke(const ConcurrentMasstree::string_type &k,
                       const varstr &v) {
      return upcall->Invoke(k.data(), k.length(), v);
    }

    OrderedIndex::ScanCallback *upcall;
    rc_t return_code;
  };

  struct XctSearchRangeCallback
      : public ConcurrentMasstree::low_level_search_range_callback {
    XctSearchRangeCallback(transaction *t, SearchRangeCallback *caller_callback)
        : t(t), caller_callback(caller_callback) {}

    virtual void
    on_resp_node(const typename ConcurrentMasstree::node_opaque_t *n,
                 uint64_t version);
    virtual bool invoke(const ConcurrentMasstree *btr_ptr,
                        const typename ConcurrentMasstree::string_type &k,
                        dbtuple *v,
                        const typename ConcurrentMasstree::node_opaque_t *n,
                        uint64_t version);

  private:
    transaction *const t;
    SearchRangeCallback *const caller_callback;
  };

  struct PurgeTreeWalker : public ConcurrentMasstree::tree_walk_callback {
    virtual void
    on_node_begin(const typename ConcurrentMasstree::node_opaque_t *n);
    virtual void on_node_success();
    virtual void on_node_failure();

  private:
    std::vector<std::pair<typename ConcurrentMasstree::value_type, bool>>
        spec_values;
  };

  static rc_t DoNodeRead(transaction *t,
                         const ConcurrentMasstree::node_opaque_t *node,
                         uint64_t version);

public:
  ConcurrentMasstreeIndex(const char *table_name, bool primary) : OrderedIndex(table_name, primary) {}

  ConcurrentMasstree &GetMasstree() { return masstree_; }

  inline void *GetTable() override { return masstree_.get_table(); }

  // A multi-get interface using AMAC
  void amac_MultiGet(transaction *t,
                     std::vector<ConcurrentMasstree::AMACState> &requests,
                     std::vector<varstr *> &values);

#ifdef ADV_COROUTINE
  // A multi-get interface using nested coroutines
  void adv_coro_MultiGet(transaction *t, std::vector<varstr *> &keys, std::vector<varstr *> &values,
                         std::vector<ermia::coro::task<bool>> &index_probe_tasks,
                         std::vector<ermia::coro::task<void>> &get_record_tasks);
#endif

  // A multi-get interface using coroutines
  void simple_coro_MultiGet(transaction *t, std::vector<varstr *> &keys,
                            std::vector<varstr *> &values,
                            std::vector<std::experimental::coroutine_handle<>> &handles);

  // A multi-ops interface using coroutines
  static void simple_coro_MultiOps(std::vector<rc_t> &rcs,
		                   std::vector<std::experimental::coroutine_handle<ermia::coro::generator<rc_t>::promise_type>> &handles);

  ermia::coro::generator<rc_t> coro_GetRecord(transaction *t, const varstr &key, varstr &value, OID *out_oid = nullptr);
  ermia::coro::generator<rc_t> coro_GetRecordSV(transaction *t, const varstr &key, varstr &value, OID *out_oid = nullptr);
  ermia::coro::generator<rc_t> coro_UpdateRecord(transaction *t, const varstr &key, varstr &value);
  ermia::coro::generator<rc_t> coro_InsertRecord(transaction *t, const varstr &key, varstr &value, OID *out_oid = nullptr);
  ermia::coro::generator<bool> coro_InsertOID(transaction *t, const varstr &key, OID oid);
  ermia::coro::generator<rc_t> coro_Scan(transaction *t, const varstr &start_key, const varstr *end_key,
                              ScanCallback &callback, uint32_t max_keys = ~uint32_t{0});

  PROMISE(rc_t) WriteSchemaTable(transaction *t, rc_t &rc, const varstr &key, varstr &value) override;

  PROMISE(void) ReadSchemaTable(transaction *t, rc_t &rc, const varstr &key, varstr &value, 
		  OID *out_oid = nullptr) override;

  PROMISE(rc_t) WriteNormalTable(str_arena *arena, OrderedIndex *index, transaction *t, varstr &value) override;

  PROMISE(rc_t) CheckNormalTable(str_arena *arena, OrderedIndex *index, transaction *t) override;

  PROMISE(void) GetRecord(transaction *t, rc_t &rc, const varstr &key, varstr &value, OID *out_oid = nullptr) override;
  PROMISE(rc_t) UpdateRecord(transaction *t, const varstr &key, varstr &value) override;
  PROMISE(rc_t) InsertRecord(transaction *t, const varstr &key, varstr &value, OID *out_oid = nullptr) override;
  PROMISE(rc_t) RemoveRecord(transaction *t, const varstr &key) override;
  PROMISE(bool) InsertOID(transaction *t, const varstr &key, OID oid) override;

  PROMISE(rc_t) Scan(transaction *t, const varstr &start_key, const varstr *end_key,
                     ScanCallback &callback, str_arena *arena = nullptr) override;
  PROMISE(rc_t) ReverseScan(transaction *t, const varstr &start_key,
                            const varstr *end_key, ScanCallback &callback, str_arena *arena = nullptr) override;

  inline size_t Size() override { return masstree_.size(); }
  std::map<std::string, uint64_t> Clear() override;
  inline void SetArrays(bool primary) override { masstree_.set_arrays(table_descriptor, primary); }

  inline PROMISE(void)
  GetOID(const varstr &key, rc_t &rc, TXN::xid_context *xc, OID &out_oid,
         ConcurrentMasstree::versioned_node_t *out_sinfo = nullptr) override {
    bool found = AWAIT masstree_.search(key, out_oid, xc->begin_epoch, out_sinfo);
    volatile_write(rc._val, found ? RC_TRUE : RC_FALSE);
  }

private:
  PROMISE(bool) InsertIfAbsent(transaction *t, const varstr &key, OID oid) override;
};
} // namespace ermia
