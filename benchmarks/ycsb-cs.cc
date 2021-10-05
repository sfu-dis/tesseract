/* 
 * A YCSB implementation based off of Silo's and equivalent to FOEDUS's.
 */
#include "bench.h"
#include "ycsb.h"

#ifndef ADV_COROUTINE

extern uint g_reps_per_tx;
extern uint g_rmw_additional_reads;
extern ReadTransactionType g_read_txn_type;
extern YcsbWorkload ycsb_workload;

class ycsb_cs_worker : public ycsb_base_worker {
public:
  ycsb_cs_worker(
      unsigned int worker_id, unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      spin_barrier *barrier_a, spin_barrier *barrier_b)
      : ycsb_base_worker(worker_id, seed, db, open_tables, barrier_a, barrier_b) {
  }

  // Essentially a coroutine scheduler that switches between active transactions
  virtual void MyWork(char *) override {
    tlog = ermia::GetLog();

    // No replication support
    ALWAYS_ASSERT(is_worker);
    workload = get_workload();
    txn_counts.resize(workload.size());

    if (ermia::config::coro_batch_schedule) {
      //PipelineScheduler();
      BatchScheduler();
    } else {
      Scheduler();
    }
  }

  virtual workload_desc_vec get_workload() const override {
    workload_desc_vec w;

    if (ycsb_workload.insert_percent() || ycsb_workload.update_percent()) {
      LOG(FATAL) << "Not implemented";
    }

    LOG_IF(FATAL, g_read_txn_type != ReadTransactionType::SimpleCoro) << "Read txn type must be simple-coro";

    if (ycsb_workload.read_percent()) {
      w.push_back(workload_desc("Read", double(ycsb_workload.read_percent()) / 100.0, nullptr, TxnRead));
    }

    if (ycsb_workload.rmw_percent()) {
      LOG_IF(FATAL, ermia::config::index_probe_only) << "Not supported";
      w.push_back(workload_desc("RMW", double(ycsb_workload.rmw_percent()) / 100.0, nullptr, TxnRMW));
    }

    if (ycsb_workload.scan_percent()) {
      if (ermia::config::scan_with_it) {
        w.push_back(workload_desc("ScanWithIterator", double(ycsb_workload.scan_percent()) / 100.0,
                    nullptr, TxnScanWithIterator));
      } else {
        LOG_IF(FATAL, ermia::config::index_probe_only) << "Not supported";
        w.push_back(workload_desc("Scan", double(ycsb_workload.scan_percent()) / 100.0, nullptr, TxnScan));
      }
    }
    return w;
  }

  static ermia::coro::generator<rc_t> TxnRead(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<ycsb_cs_worker *>(w)->txn_read(idx, begin_epoch);
  }

  static ermia::coro::generator<rc_t> TxnRMW(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<ycsb_cs_worker *>(w)->txn_rmw(idx, begin_epoch);
  }

  static ermia::coro::generator<rc_t> TxnScan(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<ycsb_cs_worker *>(w)->txn_scan(idx, begin_epoch);
  }

  static ermia::coro::generator<rc_t> TxnScanWithIterator(bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<ycsb_cs_worker *>(w)->txn_scan_with_iterator(idx, begin_epoch);
  }

  // Read transaction with context-switch using simple coroutine
  ermia::coro::generator<rc_t> txn_read(uint32_t idx, ermia::epoch_num begin_epoch) {
    ermia::transaction *txn = nullptr;
    if (ermia::config::index_probe_only) {
      arenas[idx].reset();
    } else {
      txn = db->NewTransaction(
          ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY,
          arenas[idx],
          &transactions[idx],
          idx);
      ermia::TXN::xid_context *xc = txn->GetXIDContext();
      xc->begin_epoch = begin_epoch;
    }

    for (int i = 0; i < g_reps_per_tx; ++i) {
      ermia::varstr &v = str(arenas[idx], sizeof(ycsb_kv::value));
      rc_t rc = rc_t{RC_INVALID};

      if (ermia::config::index_probe_only) {
        ermia::varstr &k = str(arenas[idx], sizeof(ycsb_kv::key));
        new (&k) ermia::varstr((char *)&k + sizeof(ermia::varstr), sizeof(ycsb_kv::key));
        BuildKey(rng_gen_key(), k);

        ermia::ConcurrentMasstree::threadinfo ti(begin_epoch);
        ermia::ConcurrentMasstree::versioned_node_t sinfo;
        ermia::OID oid = ermia::INVALID_OID;
        rc._val = (co_await table_index->GetMasstree().search_coro(k, oid, ti, &sinfo)) ? RC_TRUE : RC_FALSE;
      } else {
        ermia::varstr &k = GenerateKey(txn);
        rc = co_await table_index->coro_GetRecord(txn, k, v);
      }
#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatchCoro(rc);
#else
      // Under SI this must succeed
      ALWAYS_ASSERT(rc._val == RC_TRUE);
      ASSERT(ermia::config::index_probe_only || *(char*)v.data() == 'a');
#endif
      if (!ermia::config::always_load && !ermia::config::index_probe_only) {
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), sizeof(ycsb_kv::value));
      }
    }

#ifndef CORO_BATCH_COMMIT
    if (!ermia::config::index_probe_only) {
        TryCatchCoro(db->Commit(txn));
    }
#endif
    co_return {RC_TRUE};
  }

  // Read-modify-write transaction with context-switch using simple coroutine
  ermia::coro::generator<rc_t> txn_rmw(uint32_t idx, ermia::epoch_num begin_epoch) {
    auto *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH, arenas[idx], &transactions[idx], idx);
    ermia::TXN::xid_context *xc = txn->GetXIDContext();
    xc->begin_epoch = begin_epoch;

    for (int i = 0; i < g_reps_per_tx; ++i) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(arenas[idx], sizeof(ycsb_kv::value));
      rc_t rc = rc_t{RC_INVALID};

      rc = co_await table_index->coro_GetRecord(txn, k, v);

#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatchCoro(rc);
#else
      // Under SI this must succeed
      LOG_IF(FATAL, rc._val != RC_TRUE);
      ALWAYS_ASSERT(rc._val == RC_TRUE);
      ASSERT(*(char*)v.data() == 'a');
#endif

      ASSERT(v.size() == sizeof(ycsb_kv::value));
      if (!ermia::config::always_load) {
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), v.size());
      }

      // Re-initialize the value structure to use my own allocated memory -
      // DoTupleRead will change v.p to the object's data area to avoid memory
      // copy (in the read op we just did).
      new (&v) ermia::varstr((char *)&v + sizeof(ermia::varstr), sizeof(ycsb_kv::value));
      new (v.data()) ycsb_kv::value("a");
      rc = co_await table_index->coro_UpdateRecord(txn, k, v);  // Modify-write

      TryCatchCoro(rc);
    }

    for (int i = 0; i < g_rmw_additional_reads; ++i) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(arenas[idx], sizeof(ycsb_kv::value));
      rc_t rc = rc_t{RC_INVALID};

      rc = co_await table_index->coro_GetRecord(txn, k, v);

#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatchCoro(rc);
#else
      // Under SI this must succeed
      ALWAYS_ASSERT(rc._val == RC_TRUE);
      ASSERT(*(char*)v.data() == 'a');
#endif

      ASSERT(v.size() == sizeof(ycsb_kv::value));
      if (!ermia::config::always_load) {
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), v.size());
      }
    }
#ifndef CORO_BATCH_COMMIT
    TryCatchCoro(db->Commit(txn));
#endif
    co_return {RC_TRUE};
  }

  ermia::coro::generator<rc_t> txn_scan(uint32_t idx, ermia::epoch_num begin_epoch) {
    auto *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY,
                                   arenas[idx], &transactions[idx], idx);
    ermia::TXN::xid_context *xc = txn->GetXIDContext();
    xc->begin_epoch = begin_epoch;

    for (int i = 0; i < g_reps_per_tx; ++i) {
      rc_t rc = rc_t{RC_INVALID};
      ScanRange range = GenerateScanRange(txn);
      ycsb_scan_callback callback;
      rc = co_await table_index->coro_Scan(txn, range.start_key, &range.end_key, callback);

      ALWAYS_ASSERT(callback.size() <= g_scan_max_length);
#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatchCoro(rc);
#else
      ALWAYS_ASSERT(rc._val == RC_TRUE);
#endif
    }
#ifndef CORO_BATCH_COMMIT
    TryCatchCoro(db->Commit(txn));
#endif
    co_return {RC_TRUE};
  }

  ermia::coro::generator<rc_t> txn_scan_with_iterator(uint32_t idx, ermia::epoch_num begin_epoch) {
    auto *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_CSWITCH | ermia::transaction::TXN_FLAG_READ_ONLY,
                                   arenas[idx], &transactions[idx], idx);
    ermia::TXN::xid_context *xc = txn->GetXIDContext();
    xc->begin_epoch = begin_epoch;

    for (int i = 0; i < g_reps_per_tx; ++i) {
      rc_t rc = rc_t{RC_INVALID};
      ScanRange range = GenerateScanRange(txn);
      ycsb_scan_callback callback;
      ermia::ConcurrentMasstree::coro_ScanIterator</*IsRerverse=*/false>
          iter(txn->GetXIDContext(), &table_index->GetMasstree(), range.start_key, &range.end_key);
      bool more = co_await iter.init();

      ermia::varstr valptr;
      ermia::dbtuple* tuple = nullptr;
      while (more) {
        if (!ermia::config::index_probe_only) {
          tuple = ermia::oidmgr->oid_get_version(
              iter.tuple_array(), iter.value(), txn->GetXIDContext());
          if (tuple) {
            rc = txn->DoTupleRead(tuple, &valptr);
            if (rc._val == RC_TRUE) {
              callback.Invoke(iter.key().data(), iter.key().length(), valptr);
            }
          }
#if defined(SSI) || defined(SSN) || defined(MVOCC)
          TryCatchCoro(rc);
#else
          ALWAYS_ASSERT(rc._val == RC_TRUE);
#endif
        }
        more = iter.next();
      }
      ALWAYS_ASSERT(callback.size() <= g_scan_max_length);
    }
#ifndef CORO_BATCH_COMMIT
    TryCatchCoro(db->Commit(txn));
#endif
    co_return {RC_TRUE};
  }
};

void ycsb_cs_do_test(ermia::Engine *db, int argc, char **argv) {
  ycsb_parse_options(argc, argv);
  ycsb_bench_runner<ycsb_cs_worker> r(db);
  r.run();
}

#endif  // ADV_COROUTINE
