#include "engine.h"
#include "benchmarks/tpcc-common.h"
#include "dbcore/rcu.h"
#include "dbcore/sm-thread.h"
#include "txn.h"

namespace ermia {

TableDescriptor *schema_td = NULL;
uint64_t *_cdc_last_csn =
    (uint64_t *)malloc(sizeof(uint64_t) * config::MAX_THREADS);
uint64_t t3 = 0;

thread_local dlog::tls_log tlog;
std::mutex tlog_lock;

dlog::tls_log *GetLog(uint32_t logid) {
  // XXX(tzwang): this lock may become a problem; should be safe to not use it -
  // the set of tlogs are stable before the system starts to run, i.e., only
  // needed when creating logs
  std::lock_guard<std::mutex> guard(tlog_lock);
  return dlog::tlogs[logid];
}

dlog::tls_log *GetLog() {
  thread_local bool initialized = false;
  if (!initialized) {
    std::lock_guard<std::mutex> guard(tlog_lock);
    tlog.initialize(config::log_dir.c_str(), dlog::tlogs.size(),
                    numa_node_of_cpu(sched_getcpu()), config::log_buffer_mb,
                    config::log_segment_mb);
    initialized = true;
    dlog::tlogs.push_back(&tlog);
  }
  return &tlog;
}

class ddl_add_column_scan_callback : public OrderedIndex::ScanCallback {
public:
  ddl_add_column_scan_callback(OrderedIndex *index, transaction *t,
                               uint64_t schema_version, ermia::str_arena *arena)
      : _index(index), _txn(t), _version(schema_version), n(0) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const varstr &value) {
    MARK_REFERENCED(value);
    _arena->reset();

    varstr *k = _arena->next(keylen);
    ASSERT(k);
    k->copy_from(keyp, keylen);

    varstr *d_v = nullptr;

#ifdef MICROBENCH
    uint64_t a = 0;
    if (_version == 1) {
      struct Schema1 record;
      memcpy(&record, (char *)value.data(), sizeof(record));
      a = record.a;
    } else {
      struct Schema2 record;
      memcpy(&record, (char *)value.data(), sizeof(record));
      a = record.a;
    }

    char str2[sizeof(Schema2)];
    struct Schema2 record2;
    record2.v = _version;
    record2.a = a;
    record2.b = _version;
    record2.c = _version;
    memcpy(str2, &record2, sizeof(str2));
    d_v = _arena->next(sizeof(str2));
    d_v->copy_from(str2, sizeof(str2));
#else

    order_line::value v_ol_temp;
    const order_line::value *v_ol = Decode(value, v_ol_temp);

    order_line_1::value v_ol_1;
    v_ol_1.ol_i_id = v_ol->ol_i_id;
    v_ol_1.ol_delivery_d = v_ol->ol_delivery_d;
    v_ol_1.ol_amount = v_ol->ol_amount;
    v_ol_1.ol_supply_w_id = v_ol->ol_supply_w_id;
    v_ol_1.ol_quantity = v_ol->ol_quantity;
    v_ol_1.v = _version;
    v_ol_1.ol_tax = 0;

    const size_t order_line_sz = Size(v_ol_1);
    d_v = _arena->next(order_line_sz);
    d_v = &Encode(*d_v, v_ol_1);
#endif

#if defined(BLOCKDDL) || defined(SIDDL)
    invoke_status = _index->UpdateRecord(_txn, *k, *d_v);
    if (invoke_status._val != RC_TRUE) {
      DLOG(INFO) << "DDL normal record update false";
      return false;
    }
#elif defined(COPYDDL)
    invoke_status = _index->InsertRecord(_txn, *k, *d_v);
    if (invoke_status._val != RC_TRUE) {
      DLOG(INFO) << "DDL normal record insert false";
      return false;
    }
#endif
#ifndef NDEBUG
#if !defined(MICROBENCH)
    rc_t rc = rc_t{RC_INVALID};
    ermia::varstr valptr;
    order_line_1::value v_ol_1_temp;
    _index->GetRecord(_txn, rc, *k, valptr);
    if (rc._val != RC_TRUE)
      DLOG(INFO) << "DDL insert failed";
    const order_line_1::value *v_ol_2 = Decode(valptr, v_ol_1_temp);
    ALWAYS_ASSERT(v_ol_2->ol_tax == 0);
    ALWAYS_ASSERT(v_ol_2->v == _version);
#endif
#endif
    ++n;
    return true;
  }
  std::vector<varstr *> output;
  OrderedIndex *_index;
  transaction *_txn;
  uint64_t _version;
  ermia::str_arena *_arena = new ermia::str_arena(ermia::config::arena_size_mb);
  rc_t invoke_status = rc_t{RC_TRUE};
  size_t n;
};

rc_t ConcurrentMasstreeIndex::WriteSchemaTable(transaction *t, rc_t &rc,
                                               const varstr &key,
                                               varstr &value) {
  rc = UpdateRecord(t, key, value);
#ifndef NDEBUG
  if (rc._val != RC_TRUE) {
    DLOG(INFO) << "DDL schema update false";
    return rc;
  }
#endif

  return rc;
}

void ConcurrentMasstreeIndex::ReadSchemaTable(transaction *t, rc_t &rc,
                                              const varstr &key, varstr &value,
                                              OID *out_oid) {
retry:
  GetRecord(t, rc, key, value, out_oid);
#ifndef NDEBUG
  if (rc._val != RC_TRUE)
    DLOG(INFO) << "Read schema table failed";
#endif
  if (rc._val != RC_TRUE) {
    goto retry;
  }

#ifdef COPYDDL
  if (t->is_dml()) {
    struct Schema_record schema;
    memcpy(&schema, (char *)value.data(), sizeof(schema));
    ALWAYS_ASSERT(schema.td != schema.old_td);
    if (schema.td->GetTupleArray() != schema.index->GetTupleArray()) {
      goto retry;
    }
    if (schema.state == 2) {
      if (config::cdc_schema_lock) {
        goto retry;
      } else {
        t->SetWaitForNewSchema(true);
        t->set_table_descriptors(schema.td, schema.old_td);
      }
    }
    t->schema_read_map[schema.td] = *out_oid;
  }
#endif
}

rc_t ConcurrentMasstreeIndex::WriteNormalTable(str_arena *arena,
                                               OrderedIndex *index,
                                               transaction *t, varstr &value) {
  rc_t r;
#ifdef COPYDDL
  struct Schema_record schema;
#else
  struct Schema_base schema;
#endif
  memcpy(&schema, (char *)value.data(), sizeof(schema));
  uint64_t schema_version = schema.v;

  // printf("DDL scan begins\n");
  ALWAYS_ASSERT(table_descriptor->GetTupleArray() ==
                t->old_td->GetTupleArray());
  uint64_t count = 0;
  auto *alloc = oidmgr->get_allocator(t->old_td->GetTupleFid());
  uint32_t himark = alloc->head.hiwater_mark;
  auto *old_tuple_array = t->old_td->GetTupleArray();
  // printf("himark: %d\n", himark);
  auto *new_tuple_array = t->new_td->GetTupleArray();
  new_tuple_array->ensure_size(new_tuple_array->alloc_size(himark));
  // oidmgr->recreate_allocator(t->new_td->GetTupleFid(), himark);

  uint32_t scan_threads =
      (thread::cpu_cores.size() / 2) * config::numa_nodes - config::threads;
  printf("scan_threads: %d\n", scan_threads);
  uint32_t num_per_scan_thread = himark / (scan_threads + 1);

#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
  printf("First CDC begins\n");
  std::vector<ermia::thread::Thread *> cdc_workers = t->changed_data_capture();
#endif

  /*std::vector<ermia::thread::Thread *> scan_workers;
  for (uint32_t i = 1; i <= scan_threads; i++) {
    uint32_t begin = i * num_per_scan_thread;
    uint32_t end = (i + 1) * num_per_scan_thread;
    if (i == scan_threads)
      end = himark;
    // printf("%d, %d\n", begin, end);

    thread::Thread *thread =
        thread::GetThread(config::cdc_physical_workers_only);
    ALWAYS_ASSERT(thread);
    scan_workers.push_back(thread);

    auto parallel_scan = [=](char *) {
      for (uint32_t oid = begin; oid <= end; oid++) {
        fat_ptr *entry = old_tuple_array->get(oid);
        if (*entry != NULL_PTR) {
          dbtuple *tuple =
              AWAIT oidmgr->oid_get_version(old_tuple_array, oid, t->xc);
          // Object *object = (Object *)entry->offset();
          // dbtuple *tuple = (dbtuple *)object->GetPayload();
          varstr tuple_value;
          if (tuple && t->DoTupleRead(tuple, &tuple_value)._val == RC_TRUE) {
            arena->reset();
            varstr *d_v = nullptr;
#ifdef MICROBENCH
            uint64_t a = 0;
            if (schema_version == 1) {
              struct Schema1 record;
              memcpy(&record, (char *)tuple_value.data(), sizeof(record));
              a = record.a;
            } else {
              struct Schema2 record;
              memcpy(&record, (char *)tuple_value.data(), sizeof(record));
              a = record.a;
            }

            char str2[sizeof(Schema2)];
            struct Schema2 record2;
            record2.v = schema_version;
            record2.a = a;
            record2.b = schema_version;
            record2.c = schema_version;
            memcpy(str2, &record2, sizeof(str2));
            d_v = arena->next(sizeof(str2));
            d_v->copy_from(str2, sizeof(str2));
#else
            order_line::value v_ol_temp;
            const order_line::value *v_ol = Decode(tuple_value, v_ol_temp);

            order_line_1::value v_ol_1;
            v_ol_1.ol_i_id = v_ol->ol_i_id;
            v_ol_1.ol_delivery_d = v_ol->ol_delivery_d;
            v_ol_1.ol_amount = v_ol->ol_amount;
            v_ol_1.ol_supply_w_id = v_ol->ol_supply_w_id;
            v_ol_1.ol_quantity = v_ol->ol_quantity;
            v_ol_1.v = schema_version;
            v_ol_1.ol_tax = 0.1;

            const size_t order_line_sz = ::Size(v_ol_1);
            d_v = arena->next(order_line_sz);
            d_v = &Encode(*d_v, v_ol_1);
#endif

            t->DDLCDCInsert(t->new_td, oid, d_v, t->GetXIDContext()->begin);
          }
        }
      }
    };

    thread->StartTask(parallel_scan);
  }*/

  for (OID oid = 0; oid <= himark; oid++) {
    fat_ptr *entry = old_tuple_array->get(oid);
    if (*entry != NULL_PTR) {
      dbtuple *tuple =
          AWAIT oidmgr->oid_get_version(old_tuple_array, oid, t->xc);
      // Object *object = (Object *)entry->offset();
      // dbtuple *tuple = (dbtuple *)object->GetPayload();
      varstr tuple_value;
      if (tuple && t->DoTupleRead(tuple, &tuple_value)._val == RC_TRUE) {
        arena->reset();
        varstr *d_v = nullptr;
#ifdef MICROBENCH
        uint64_t a = 0;
        if (schema_version == 1) {
          struct Schema1 record;
          memcpy(&record, (char *)tuple_value.data(), sizeof(record));
          a = record.a;
        } else {
          struct Schema2 record;
          memcpy(&record, (char *)tuple_value.data(), sizeof(record));
          a = record.a;
        }

        char str2[sizeof(Schema2)];
        struct Schema2 record2;
        record2.v = schema_version;
        record2.a = a;
        record2.b = schema_version;
        record2.c = schema_version;
        memcpy(str2, &record2, sizeof(str2));
        d_v = arena->next(sizeof(str2));
        d_v->copy_from(str2, sizeof(str2));
#else
        order_line::value v_ol_temp;
        const order_line::value *v_ol = Decode(tuple_value, v_ol_temp);

        order_line_1::value v_ol_1;
        v_ol_1.ol_i_id = v_ol->ol_i_id;
        v_ol_1.ol_delivery_d = v_ol->ol_delivery_d;
        v_ol_1.ol_amount = v_ol->ol_amount;
        v_ol_1.ol_supply_w_id = v_ol->ol_supply_w_id;
        v_ol_1.ol_quantity = v_ol->ol_quantity;
        v_ol_1.v = schema_version;
        v_ol_1.ol_tax = 0.1;

        const size_t order_line_sz = ::Size(v_ol_1);
        d_v = arena->next(order_line_sz);
        d_v = &Encode(*d_v, v_ol_1);
#endif

        t->DDLCDCInsert(t->new_td, oid, d_v, t->GetXIDContext()->begin);
      }
    }
  }

  /*for (std::vector<ermia::thread::Thread *>::const_iterator it =
           scan_workers.begin();
       it != scan_workers.end(); ++it) {
    (*it)->Join();
    ermia::thread::PutThread(*it);
  }*/

  uint64_t current_csn = dlog::current_csn.load(std::memory_order_relaxed);
  volatile_write(t3, current_csn - 1000000);
  printf("DDL scan ends, current csn: %lu, t3: %lu\n", current_csn, t3);

#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
  while (t->get_cdc_smallest_csn() < volatile_read(t3)) {
  }
  t->join_changed_data_capture_threads(cdc_workers);
  printf("First CDC ends\n");
  printf("Now go to grab a t4\n");
#endif

  return rc_t{RC_TRUE};
}

class ddl_precompute_aggregate_scan_callback
    : public OrderedIndex::ScanCallback {
public:
  ddl_precompute_aggregate_scan_callback(OrderedIndex *oorder_table_index,
                                         OrderedIndex *order_line_table_index,
                                         OrderedIndex *oorder_table_secondary_index,
					 transaction *t,
                                         uint64_t schema_version,
                                         ermia::str_arena *arena,
					 std::function<ermia::varstr *(
                                                        const char *keyp,
                                                        size_t keylen,
                                                        const ermia::varstr &value,
                                                        uint64_t schema_version,
                                                        ermia::transaction *txn,
                                                        ermia::str_arena *arena,
                                                        ermia::OrderedIndex *index)> op)
      : _oorder_table_index(oorder_table_index),
        _order_line_table_index(order_line_table_index),
	_oorder_table_secondary_index(oorder_table_secondary_index),
	_txn(t), _version(schema_version), _op(op) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const varstr &value) {
    MARK_REFERENCED(value);

    varstr *k = _arena->next(keylen);
    if (!k) {
      _arena = new ermia::str_arena(ermia::config::arena_size_mb);
      k = _arena->next(keylen);
    }
    ASSERT(k);
    k->copy_from(keyp, keylen);

    oorder::key k_oo_temp;
    const oorder::key *k_oo = Decode(keyp, k_oo_temp);

    oorder::value v_oo_temp;
    const oorder::value *v_oo = Decode(value, v_oo_temp);

    /*credit_check_order_line_scan_callback c_ol(_version);
    const order_line::key k_ol_0(k_oo->o_w_id, k_oo->o_d_id, k_oo->o_id, 1);
    const order_line::key k_ol_1(k_oo->o_w_id, k_oo->o_d_id, k_oo->o_id, 15);

    varstr *k_ol_0_str = _arena->next(Size(k_ol_0));
    if (!k_ol_0_str) {
      _arena = new ermia::str_arena(ermia::config::arena_size_mb);
      k_ol_0_str = _arena->next(Size(k_ol_0));
    }

    varstr *k_ol_1_str = _arena->next(Size(k_ol_1));
    if (!k_ol_1_str) {
      _arena = new ermia::str_arena(ermia::config::arena_size_mb);
      k_ol_1_str = _arena->next(Size(k_ol_1));
    }

    _order_line_table_index->Scan(_txn, Encode(*k_ol_0_str, k_ol_0),
                                  &Encode(*k_ol_1_str, k_ol_1), c_ol);

    oorder::value v_oo_temp;
    const oorder::value *v_oo = Decode(value, v_oo_temp);

    oorder_precompute_aggregate::value v_oo_pa;
    v_oo_pa.o_c_id = v_oo->o_c_id;
    v_oo_pa.o_carrier_id = v_oo->o_carrier_id;
    v_oo_pa.o_ol_cnt = v_oo->o_ol_cnt;
    v_oo_pa.o_all_local = v_oo->o_all_local;
    v_oo_pa.o_entry_d = v_oo->o_entry_d;
    v_oo_pa.o_total_amount = c_ol.sum;

    const size_t oorder_sz = Size(v_oo_pa);
    varstr *d_v = _arena->next(oorder_sz);

    if (!d_v) {
      _arena = new ermia::str_arena(ermia::config::arena_size_mb);
      d_v = _arena->next(oorder_sz);
    }
    d_v = &Encode(*d_v, v_oo_pa);
    */

    varstr *d_v = _op(keyp, keylen, value, _version, _txn, _arena, _order_line_table_index);
#if defined(BLOCKDDL) || defined(SIDDL)
    invoke_status = _oorder_table_index->UpdateRecord(_txn, *k, *d_v);
    if (invoke_status._val != RC_TRUE) {
      printf("DDL normal record update false\n");
      return false;
    }
#elif defined(COPYDDL)
    OID oid = 0;
    invoke_status = _oorder_table_index->InsertRecord(_txn, *k, *d_v, &oid);
    if (invoke_status._val != RC_TRUE) {
      printf("DDL normal record insert false\n");
      return false;
    }
    const oorder_c_id_idx::key k_idx(k_oo->o_w_id, k_oo->o_d_id,
                                     v_oo->o_c_id, k_oo->o_id);
    varstr *k_idx_str = _arena->next(Size(k_idx));
    if (!k_idx_str) {
      _arena = new ermia::str_arena(ermia::config::arena_size_mb);
      k_idx_str = _arena->next(Size(k_idx));
    }
    invoke_status = _oorder_table_secondary_index->InsertOID(_txn, Encode(*k_idx_str, k_idx), oid);
    if (invoke_status._val != RC_TRUE) {
      printf("DDL oid record insert false\n");
      return false;
    }
#endif

    return true;
  }
  OrderedIndex *_oorder_table_index;
  OrderedIndex *_order_line_table_index;
  OrderedIndex *_oorder_table_secondary_index;
  transaction *_txn;
  uint64_t _version;
  ermia::str_arena *_arena = new ermia::str_arena(ermia::config::arena_size_mb);
  std::function<ermia::varstr *(
                  const char *keyp,
                  size_t keylen,
                  const ermia::varstr &value,
                  uint64_t schema_version,
                  ermia::transaction *txn,
                  ermia::str_arena *arena,
                  ermia::OrderedIndex *index)> _op;
  rc_t invoke_status = rc_t{RC_INVALID};
};

rc_t ConcurrentMasstreeIndex::WriteNormalTable1(
    str_arena *arena, OrderedIndex *old_oorder_table_index,
    OrderedIndex *order_line_table_index, 
    OrderedIndex *oorder_table_secondary_index, transaction *t, varstr &value,
    std::function<ermia::varstr *(
                  const char *keyp,
                  size_t keylen,
                  const ermia::varstr &value,
                  uint64_t schema_version,
                  ermia::transaction *txn,
                  ermia::str_arena *arena,
                  ermia::OrderedIndex *index)> op) {
  rc_t r;
#ifdef COPYDDL
  struct Schema_record schema;
#else
  struct Schema_base schema;
#endif
  memcpy(&schema, (char *)value.data(), sizeof(schema));
  uint64_t schema_version = schema.v;

#ifdef COPYDDL
  ddl_precompute_aggregate_scan_callback c_precompute_aggregate(
      this, order_line_table_index, oorder_table_secondary_index, t, schema_version, arena, op);
#else
  ddl_precompute_aggregate_scan_callback c_precompute_aggregate(
      old_oorder_table_index, order_line_table_index, oorder_table_secondary_index, 
      t, schema_version, arena, op);
#endif

  const oorder::key k_oo_0(1, 1, 1);
  varstr *start_key = arena->next(::Size(k_oo_0));
  if (!start_key) {
    arena = new ermia::str_arena(ermia::config::arena_size_mb);
    start_key = arena->next(::Size(k_oo_0));
  }
  r = old_oorder_table_index->Scan(t, Encode(*start_key, k_oo_0), nullptr,
                                   c_precompute_aggregate, arena);
  if (r._val != RC_TRUE) {
    printf("DDL scan false\n");
    return r;
  }

  printf("scan invoke status: %hu\n",
         c_precompute_aggregate.invoke_status._val);

  return c_precompute_aggregate.invoke_status;
}

class ddl_add_constraint_scan_callback : public OrderedIndex::ScanCallback {
public:
  ddl_add_constraint_scan_callback(OrderedIndex *index, transaction *t,
                                   ermia::str_arena *arena,
                                   std::function<bool(uint64_t)> op)
      : _index(index), _txn(t), _arena(arena) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const varstr &value) {
    MARK_REFERENCED(value);
    //_arena->reset();

    varstr *k = _arena->next(keylen);
    ASSERT(k);
    if (!k) {
      _arena = new ermia::str_arena(ermia::config::arena_size_mb);
      k = _arena->next(keylen);
    }
    k->copy_from(keyp, keylen);

#ifdef MICROBENCH
    rc_t rc = rc_t{RC_INVALID};
    ermia::varstr valptr;
    _index->GetRecord(_txn, rc, *k, valptr);
    if (rc._val != RC_TRUE)
      printf("Get record failed\n");
    struct Schema1 record;
    memcpy(&record, (char *)valptr.data(), sizeof(record));
    if (record.b < 0) {
      invoke_status = rc_t{RC_ABORT_INTERNAL};
      printf("Constraint violation\n");
      return false;
    }
#endif

    return true;
  }
  std::vector<varstr *> output;
  OrderedIndex *_index;
  transaction *_txn;
  ermia::str_arena *_arena;
  rc_t invoke_status = rc_t{RC_TRUE};
};

rc_t ConcurrentMasstreeIndex::CheckNormalTable(
    str_arena *arena, OrderedIndex *index, transaction *t,
    std::function<bool(uint64_t)> op) {
  rc_t r;

  ddl_add_constraint_scan_callback c_add_constraint(this, t, arena, op);

  char str1[sizeof(uint64_t)];
  uint64_t start = 0;
  memcpy(str1, &start, sizeof(str1));
  varstr *const start_key = arena->next(sizeof(str1));
  start_key->copy_from(str1, sizeof(str1));

  r = index->Scan(t, *start_key, nullptr, c_add_constraint, arena);
  if (r._val != RC_TRUE) {
    printf("DDL scan false\n");
    return r;
  }

  printf("scan invoke status: %hu\n", c_add_constraint.invoke_status._val);

  return c_add_constraint.invoke_status;
}

#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
bool transaction::changed_data_capture_impl(uint32_t thread_id,
                                            uint32_t ddl_thread_id,
                                            uint32_t begin_log,
                                            uint32_t end_log, str_arena *arena,
                                            util::fast_random &r) {
  RCU::rcu_enter();
  uint64_t begin_csn = xc->begin;
  uint64_t end_csn = xc->end;
  bool ddl_end_tmp = true, ddl_end_flag = false;
  FID fid = this->old_td->GetTupleFid();
  ermia::ConcurrentMasstreeIndex *table_secondary_index = nullptr;
  if (this->new_td->GetSecIndexes().size()) {
    table_secondary_index = (ermia::ConcurrentMasstreeIndex *)(*(
        this->new_td->GetSecIndexes().begin()));
    ALWAYS_ASSERT(table_secondary_index);
  }
  uint32_t count = 0;
  for (uint32_t i = begin_log; i <= end_log; i++) {
    dlog::tls_log *tlog = dlog::tlogs[i];
    uint64_t csn = volatile_read(pcommit::_tls_durable_csn[i]);
    if (tlog && csn && tlog != GetLog() && i != ddl_thread_id) {
      count++;
    }
  }
  // printf("count: %d\n", count);
  for (uint32_t i = begin_log; i <= end_log; i++) {
    dlog::tls_log *tlog = dlog::tlogs[i];
    uint64_t csn = volatile_read(pcommit::_tls_durable_csn[i]);
    if (tlog && csn && tlog != GetLog() && i != ddl_thread_id) {
      // printf("log %d cdc\n", i);
    process:
      std::vector<dlog::segment> *segments = tlog->get_segments();
      if (segments->size() > 1)
        printf("segments > 1\n");
      bool stop_scan = false;
      uint64_t offset_in_seg = volatile_read(_tls_durable_lsn[i]);
      uint64_t offset_increment = 0;
      uint64_t last_csn = 0;
      int insert_total = 0, update_total = 0;
      int insert_fail = 0, update_fail = 0;
      for (std::vector<dlog::segment>::reverse_iterator seg =
               segments->rbegin();
           seg != segments->rend(); seg++) {
        uint64_t data_sz = seg->size;
        uint64_t block_sz = sizeof(dlog::log_block),
                 logrec_sz = sizeof(dlog::log_record),
                 tuple_sc = sizeof(dbtuple);

        while (offset_increment <
                   (data_sz = volatile_read(seg->size)) - offset_in_seg &&
               cdc_running) {
          char *header_buf = (char *)RCU::rcu_alloc(block_sz);
          // char *header_buf = (char *)malloc(block_sz);
          size_t m = os_pread(seg->fd, (char *)header_buf, block_sz,
                              offset_in_seg + offset_increment);
          dlog::log_block *header = (dlog::log_block *)(header_buf);
          if ((end_csn && begin_csn <= header->csn && header->csn <= end_csn) ||
              (!end_csn && begin_csn <= header->csn)) {
            last_csn = header->csn;
            volatile_write(_cdc_last_csn[i], last_csn);
            uint32_t block_total_sz = header->total_size();
            char *data_buf = (char *)RCU::rcu_alloc(block_total_sz);
            // char *data_buf = (char *)malloc(block_total_sz);
            size_t m = os_pread(seg->fd, (char *)data_buf, block_total_sz,
                                offset_in_seg + offset_increment);
            uint64_t offset_in_block = 0;
            varstr *insert_key, *update_key, *insert_key_idx;
            while (offset_in_block < header->payload_size && cdc_running) {
              dlog::log_record *logrec =
                  (dlog::log_record *)(data_buf + block_sz + offset_in_block);

              // if (header->csn != logrec->csn) printf("csn bug, header->csn:
              // %lu, logrec->csn: %lu, data_sz: %lu, offset_in_seg: %lu,
              // block_total_sz: %d, offset_in_block: %lu, header->payload_size:
              // %u, offset_increment: %lu\n", header->csn, logrec->csn,
              // data_sz, offset_in_seg, block_total_sz, offset_in_block,
              // header->payload_size, offset_increment);
              ALWAYS_ASSERT(header->csn == logrec->csn);

              FID f = logrec->fid;
              OID o = logrec->oid;

              if (f != fid) {
                offset_in_block += logrec->rec_size;
                continue;
              }

              arena->reset();

              if (logrec->type == dlog::log_record::logrec_type::INSERT) {
                dbtuple *tuple = (dbtuple *)(logrec->data);
                varstr value(tuple->get_value_start(), tuple->size);
                
		order_line::value v_ol_temp;
                const order_line::value *v_ol = Decode(value, v_ol_temp);

                order_line_1::value v_ol_1;
                v_ol_1.ol_i_id = v_ol->ol_i_id;
		v_ol_1.ol_delivery_d = v_ol->ol_delivery_d;
                v_ol_1.ol_amount = v_ol->ol_amount;
                v_ol_1.ol_supply_w_id = v_ol->ol_supply_w_id;
                v_ol_1.ol_quantity = v_ol->ol_quantity;
                v_ol_1.v = v_ol->v + 1;
                v_ol_1.ol_tax = 0.1;

                const size_t order_line_sz = ::Size(v_ol_1);
                varstr *d_v = arena->next(order_line_sz);
                d_v = &Encode(*d_v, v_ol_1);
		
		insert_total++;
                // if (!ddl_running_2) {
                if (DDLCDCInsert(this->new_td, o, d_v, logrec->csn)._val !=
                    RC_TRUE) {
                  insert_fail++;
                }
                //}
                if (table_secondary_index) {
                  //   table_secondary_index->InsertOID(t, *insert_key_idx,
                  //   oid);
                }
              } else if (logrec->type ==
                         dlog::log_record::logrec_type::UPDATE) {
                dbtuple *tuple = (dbtuple *)(logrec->data);
                varstr value(tuple->get_value_start(), tuple->size);

                varstr *d_v = nullptr;
#ifdef MICROBENCH
                struct Schema_base record_test;
                memcpy(&record_test, (char *)value.data(), sizeof(record_test));
                uint64_t version = record_test.v;
                uint64_t a = 0;
                if (version == 0) {
                  struct Schema1 record;
                  memcpy(&record, (char *)value.data(), sizeof(record));
                  a = record.a;
                } else {
                  struct Schema2 record;
                  memcpy(&record, (char *)value.data(), sizeof(record));
                  a = record.a;
                }

                version++;
                ALWAYS_ASSERT(version != 0);

                char str2[sizeof(Schema2)];
                struct Schema2 record2;
                record2.v = version;
                record2.a = a;
                record2.b = version;
                record2.c = version;
                memcpy(str2, &record2, sizeof(str2));
                d_v = arena->next(sizeof(str2));
                d_v->copy_from(str2, sizeof(str2));
#else
                order_line::value v_ol_temp;
                const order_line::value *v_ol = Decode(value, v_ol_temp);

                order_line_1::value v_ol_1;
                v_ol_1.ol_i_id = v_ol->ol_i_id;
                v_ol_1.ol_delivery_d = v_ol->ol_delivery_d;
                v_ol_1.ol_amount = v_ol->ol_amount;
                v_ol_1.ol_supply_w_id = v_ol->ol_supply_w_id;
                v_ol_1.ol_quantity = v_ol->ol_quantity;
                v_ol_1.v = v_ol->v + 1;
                v_ol_1.ol_tax = 0.1;

                const size_t order_line_sz = ::Size(v_ol_1);
                d_v = arena->next(order_line_sz);
                d_v = &Encode(*d_v, v_ol_1);
#endif

                update_total++;
                // if (!ddl_running_2) {
                if (this->DDLCDCUpdate(this->new_td, o, d_v, logrec->csn)
                        ._val != RC_TRUE) {
                  update_fail++;
                }
                //}
              }

              offset_in_block += logrec->rec_size;
            }
            RCU::rcu_free(data_buf);
            // free(data_buf);
          } else {
            if (end_csn && header->csn > end_csn) {
              RCU::rcu_free(header_buf);
              // free(header_buf);
              printf("header->csn > end_csn\n");
              // ddl_end_flag = true;
              count--;
              stop_scan = true;
              break;
            }
          }
          offset_increment += header->total_size();
          volatile_write(_tls_durable_lsn[i], offset_in_seg + offset_increment);
          RCU::rcu_free(header_buf);
          // free(header_buf);
        }
        // if (insert_total != 0 || update_total != 0)
        //   ddl_end_tmp = false;
        if (!stop_scan && (insert_total == 0 || update_total == 0)) {
          count--;
        }

        /*if (update_fail > 0) {
          printf("CDC update fail, %d\n", update_fail);
        }

        if (insert_fail > 0) {
          printf("CDC insert fail, %d\n", insert_fail);
        }*/

        // printf("%d, %d\n", update_total, insert_total);

        if (stop_scan)
          break;
      }

      if (offset_increment) {
        // volatile_write(_tls_durable_lsn[i], offset_in_seg +
        // offset_increment);
      }
      if (last_csn) {
        // volatile_write(_cdc_last_csn[i], last_csn);
        // printf("log %d, last csn: %lu, current csn: %lu\n", i, last_csn,
        //        dlog::current_csn.load(std::memory_order_relaxed));
        /*if (!ddl_running_2) {
          // volatile_write(_cdc_last_csn[i], last_csn);
          if (last_csn >=
              dlog::current_csn.load(std::memory_order_relaxed) - 100000) {
            // ddl_end_flag = true;
            count--;
          } else {
            goto process;
          }
        }*/
      }
      /*if (ddl_running_2 && !ddl_end_flag &&
          (insert_total != 0 || update_total != 0)) {
        goto process;
      }*/
    }
    if (!cdc_running) {
      break;
    }
  }
  RCU::rcu_exit();
  // return ddl_end_flag || ddl_end_tmp;
  // printf("count: %d\n", count);
  /*uint64_t current_csn = dlog::current_csn.load(std::memory_order_relaxed);
  for (uint32_t i = begin_log; i <= end_log; i++) {
    dlog::tls_log *tlog = dlog::tlogs[i];
    uint64_t csn = volatile_read(pcommit::_tls_durable_csn[i]);
    if (tlog && csn && tlog != GetLog() && i != ddl_thread_id) {
      if (!ddl_running_2) printf("log %d, gap: %lu\n", i, current_csn -
  _cdc_last_csn[i]);
    }
  }*/
  return count == 0;
}
#endif

// Engine initialization, including creating the OID, log, and checkpoint
// managers and recovery if needed.
Engine::Engine() {
  config::sanity_check();
  ALWAYS_ASSERT(config::log_dir.size());
  ALWAYS_ASSERT(!oidmgr);
  sm_oid_mgr::create();
  ALWAYS_ASSERT(oidmgr);
  ermia::dlog::initialize();
}

Engine::~Engine() { ermia::dlog::uninitialize(); }

TableDescriptor *Engine::CreateTable(const char *name) {
  auto *td = Catalog::NewTable(name);

  if (true) { //! sm_log::need_recovery) {
    // Note: this will insert to the log and therefore affect min_flush_lsn,
    // so must be done in an sm-thread which must be created by the user
    // application (not here in ERMIA library).
    // ASSERT(ermia::logmgr);

    // TODO(tzwang): perhaps make this transactional to allocate it from
    // transaction string arena to avoid malloc-ing memory (~10k size).
    // char *log_space = (char *)malloc(sizeof(sm_tx_log));
    // ermia::sm_tx_log *log = ermia::logmgr->new_tx_log(log_space);
    td->Initialize();
    // log->log_table(td->GetTupleFid(), td->GetKeyFid(), td->GetName());
    // log->commit(nullptr);
    // free(log_space);
  }
  return td;
}

void Engine::LogIndexCreation(bool primary, FID table_fid, FID index_fid,
                              const std::string &index_name) {
  /*
  if (!sm_log::need_recovery) {
    // Note: this will insert to the log and therefore affect min_flush_lsn,
    // so must be done in an sm-thread which must be created by the user
    // application (not here in ERMIA library).
    ASSERT(ermia::logmgr);

    // TODO(tzwang): perhaps make this transactional to allocate it from
    // transaction string arena to avoid malloc-ing memory (~10k size).
    char *log_space = (char *)malloc(sizeof(sm_tx_log));
    ermia::sm_tx_log *log = ermia::logmgr->new_tx_log(log_space);
    log->log_index(table_fid, index_fid, index_name, primary);
    log->commit(nullptr);
    free(log_space);
  }
  */
}

void Engine::CreateIndex(const char *table_name, const std::string &index_name,
                         bool is_primary) {
  auto *td = Catalog::GetTable(table_name);
  ALWAYS_ASSERT(td);
  auto *index = new ConcurrentMasstreeIndex(table_name, is_primary);
  if (is_primary) {
    td->SetPrimaryIndex(index, index_name);
  } else {
    td->AddSecondaryIndex(index, index_name);
  }
  FID index_fid = index->GetIndexFid();
  LogIndexCreation(is_primary, td->GetTupleFid(), index_fid, index_name);
}

PROMISE(rc_t)
ConcurrentMasstreeIndex::Scan(transaction *t, const varstr &start_key,
                              const varstr *end_key, ScanCallback &callback,
                              str_arena *arena) {
  /*if (t->IsWaitForNewSchema()) {
    RETURN rc_t{RC_ABORT_INTERNAL};
  }*/
  SearchRangeCallback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);

  t->ensure_active();
  if (end_key) {
    VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                      << ")::search_range_call [" << util::hexify(start_key)
                      << ", " << util::hexify(*end_key) << ")" << std::endl);
  } else {
    VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                      << ")::search_range_call [" << util::hexify(start_key)
                      << ", +inf)" << std::endl);
  }

  if (!unlikely(end_key && *end_key <= start_key)) {
    XctSearchRangeCallback cb(t, &c);
    AWAIT masstree_.search_range_call(start_key, end_key ? end_key : nullptr,
                                      cb, t->xc);
  }
  RETURN c.return_code;
}

PROMISE(rc_t)
ConcurrentMasstreeIndex::ReverseScan(transaction *t, const varstr &start_key,
                                     const varstr *end_key,
                                     ScanCallback &callback, str_arena *arena) {
  SearchRangeCallback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);

  t->ensure_active();
  if (!unlikely(end_key && start_key <= *end_key)) {
    XctSearchRangeCallback cb(t, &c);

    varstr lowervk;
    if (end_key) {
      lowervk = *end_key;
    }
    AWAIT masstree_.rsearch_range_call(start_key, end_key ? &lowervk : nullptr,
                                       cb, t->xc);
  }
  RETURN c.return_code;
}

std::map<std::string, uint64_t> ConcurrentMasstreeIndex::Clear() {
  PurgeTreeWalker w;
  masstree_.tree_walk(w);
  masstree_.clear();
  return std::map<std::string, uint64_t>();
}

PROMISE(void)
ConcurrentMasstreeIndex::GetRecord(transaction *t, rc_t &rc, const varstr &key,
                                   varstr &value, OID *out_oid,
                                   TableDescriptor *old_table_descriptor,
                                   TableDescriptor *old_table_descriptors[],
                                   uint64_t version) {
  OID oid = INVALID_OID;
  rc = {RC_INVALID};
  ConcurrentMasstree::versioned_node_t sinfo;

  if (!t) {
    auto e = MM::epoch_enter();
    rc._val = AWAIT masstree_.search(key, oid, e, &sinfo) ? RC_TRUE : RC_FALSE;
    MM::epoch_exit(0, e);
  } else {
    t->ensure_active();
    bool found = AWAIT masstree_.search(key, oid, t->xc->begin_epoch, &sinfo);

    dbtuple *tuple = nullptr;
    if (found) {
      // Key-OID mapping exists, now try to get the actual tuple to be sure
      tuple = AWAIT oidmgr->oid_get_version(table_descriptor->GetTupleArray(),
                                            oid, t->xc);
      if (!tuple) {
        found = false;
      }
    }

#ifdef LAZYDDL
    if (!found && old_table_descriptor) {
      int total = version;
      /*for (uint i = 0; i < 16; i++) {
        total++;
        if (old_table_descriptors[i] == old_table_descriptor) {
          break;
        }
      }*/
    retry:
      rc = {RC_INVALID};
      oid = INVALID_OID;
      // old_table_descriptor = old_table_descriptors[total - 1];
      AWAIT old_table_descriptor->GetPrimaryIndex()->GetOID(key, rc, t->xc, oid);

      if (rc._val == RC_TRUE) {
        tuple = AWAIT oidmgr->oid_get_version(old_table_descriptor->GetTupleArray(),
                                              oid, t->xc);
      } else {
        total--;
        if (total > 0) {
          old_table_descriptor = old_table_descriptors[total - 1];
          ALWAYS_ASSERT(old_table_descriptor != nullptr);
          goto retry;
        }
        volatile_write(rc._val, RC_FALSE);
        RETURN;
      }

      if (!tuple) {
        found = false;
      } else {
        if (t->DoTupleRead(tuple, &value)._val == RC_TRUE) {
          struct Schema_base record_test;
          memcpy(&record_test, value.data(), sizeof(record_test));

          struct Schema2 record2;

          if (record_test.v == 0) {
            struct Schema1 record;
            memcpy(&record, value.data(), sizeof(record));

            if (version == record_test.v + 1) {
              record2.v = record.v + 1;
              record2.a = record.a;
              record2.b = record.v + 1;
              record2.c = record.v + 1;
            } else {
              record2.v = version;
              record2.a = record.a;
              record2.b = version;
              record2.c = version;
            }
          } else {
            struct Schema2 record;
            memcpy(&record, value.data(), sizeof(record));

            if (version == record_test.v + 1) {
              record2.v = record.v + 1;
              record2.a = record.a;
              record2.b = record.v + 1;
              record2.c = record.v + 1;
            } else {
              record2.v = version;
              record2.a = record.a;
              record2.b = version;
              record2.c = version;
            }
          }

          char str2[sizeof(Schema2)];
          memcpy(str2, &record2, sizeof(str2));
          varstr *v2 = t->string_allocator().next(sizeof(str2));
          v2->copy_from(str2, sizeof(str2));

          rc = InsertRecord(t, key, *v2);
          if (rc._val != RC_TRUE) {
            volatile_write(rc._val, RC_ABORT_INTERNAL);
            RETURN;
          } else {
            found = true;
            memcpy(&value, v2, sizeof(str2));

            if (found) {
              volatile_write(rc._val, RC_TRUE);
            } else if (config::phantom_prot) {
              volatile_write(rc._val,
                             DoNodeRead(t, sinfo.first, sinfo.second)._val);
            }

            if (out_oid) {
              *out_oid = oid;
            }

            RETURN;
          }
        }
      }
    }
#endif

    if (found) {
      volatile_write(rc._val, t->DoTupleRead(tuple, &value)._val);
    } else if (config::phantom_prot) {
      volatile_write(rc._val, DoNodeRead(t, sinfo.first, sinfo.second)._val);
    } else {
      volatile_write(rc._val, RC_FALSE);
    }
#ifndef SSN
    ASSERT(rc._val == RC_FALSE || rc._val == RC_TRUE);
#endif
  }

  if (out_oid) {
    *out_oid = oid;
  }
}

void ConcurrentMasstreeIndex::PurgeTreeWalker::on_node_begin(
    const typename ConcurrentMasstree::node_opaque_t *n) {
  ASSERT(spec_values.empty());
  spec_values = ConcurrentMasstree::ExtractValues(n);
}

void ConcurrentMasstreeIndex::PurgeTreeWalker::on_node_success() {
  spec_values.clear();
}

void ConcurrentMasstreeIndex::PurgeTreeWalker::on_node_failure() {
  spec_values.clear();
}

PROMISE(bool)
ConcurrentMasstreeIndex::InsertIfAbsent(transaction *t, const varstr &key,
                                        OID oid) {
  typename ConcurrentMasstree::insert_info_t ins_info;
  bool inserted = AWAIT masstree_.insert_if_absent(key, oid, t->xc, &ins_info);

  if (!inserted) {
    RETURN false;
  }

  if (config::phantom_prot && !t->masstree_absent_set.empty()) {
    // Update node version number
    ASSERT(ins_info.node);
    auto it = t->masstree_absent_set.find(ins_info.node);
    if (it != t->masstree_absent_set.end()) {
      if (unlikely(it->second != ins_info.old_version)) {
        // Important: caller should unlink the version, otherwise we risk
        // leaving a dead version at chain head -> infinite loop or segfault...
        RETURN false;
      }
      // otherwise, bump the version
      it->second = ins_info.new_version;
    }
  }
  RETURN true;
}

////////////////// Index interfaces /////////////////

PROMISE(bool)
ConcurrentMasstreeIndex::InsertOID(transaction *t, const varstr &key, OID oid) {
  bool inserted = AWAIT InsertIfAbsent(t, key, oid);
  if (inserted) {
    t->LogIndexInsert(this, oid, &key);
    if (config::enable_chkpt) {
      auto *key_array = GetTableDescriptor()->GetKeyArray();
      volatile_write(key_array->get(oid)->_ptr, 0);
    }
  }
  RETURN inserted;
}

PROMISE(rc_t)
ConcurrentMasstreeIndex::InsertRecord(transaction *t, const varstr &key,
                                      varstr &value, OID *out_oid) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  /*if (t->IsWaitForNewSchema()) {
    RETURN rc_t{RC_ABORT_INTERNAL};
  }*/

  ASSERT((char *)key.data() == (char *)&key + sizeof(varstr));
  t->ensure_active();

  OID oid = 0;

#ifdef LAZYDDL
  // Search for OID
  /*rc_t rc = {RC_INVALID};
  AWAIT GetOID(key, rc, t->xc, oid);

  if (rc._val == RC_TRUE) {
    if (out_oid) {
      *out_oid = oid;
    }

    RETURN rc_t{RC_TRUE};
  }*/
#endif

  // Insert to the table first
  dbtuple *tuple = nullptr;
  TableDescriptor *td = nullptr;
  if (t->IsWaitForNewSchema()) {
    td = t->new_td;
    // oid = AWAIT t->DDLInsert(td, &value);
    oid = t->Insert(td, &value, &tuple);
    if (oid == INVALID_OID) {
      RETURN rc_t{RC_ABORT_INTERNAL};
    }
  } else {
    td = table_descriptor;
    oid = t->Insert(td, &value, &tuple);
  }

  // Done with table record, now set up index
  ASSERT((char *)key.data() == (char *)&key + sizeof(varstr));
  if (!AWAIT InsertOID(t, key, oid)) {
    if (config::enable_chkpt) {
      volatile_write(td->GetKeyArray()->get(oid)->_ptr, 0);
    }
    RETURN rc_t{RC_ABORT_INTERNAL};
  }

  // Succeeded, now put the key there if we need it
  if (config::enable_chkpt) {
    // XXX(tzwang): only need to install this key if we need chkpt; not a
    // realistic setting here to not generate it, the purpose of skipping
    // this is solely for benchmarking CC.
    varstr *new_key = (varstr *)MM::allocate(sizeof(varstr) + key.size());
    new (new_key) varstr((char *)new_key + sizeof(varstr), 0);
    new_key->copy_from(&key);
    auto *key_array = td->GetKeyArray();
    key_array->ensure_size(oid);
    oidmgr->oid_put(key_array, oid,
                    fat_ptr::make((void *)new_key, INVALID_SIZE_CODE));
  }

  if (out_oid) {
    *out_oid = oid;
  }

  RETURN rc_t{RC_TRUE};
}

PROMISE(rc_t)
ConcurrentMasstreeIndex::UpdateRecord(transaction *t, const varstr &key,
                                      varstr &value,
                                      TableDescriptor *old_table_descriptor,
                                      TableDescriptor *old_table_descriptors[],
                                      uint64_t version) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  /*if (t->IsWaitForNewSchema()) {
    RETURN rc_t{RC_ABORT_INTERNAL};
  }*/

  // Search for OID
  OID oid = 0;
  rc_t rc = {RC_INVALID};
  AWAIT GetOID(key, rc, t->xc, oid);

#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
  if (t->IsWaitForNewSchema() && rc._val == RC_TRUE) {
    if (AWAIT t->OverlapCheck(t->old_td, oid)) {
      RETURN rc_t{RC_ABORT_INTERNAL};
    }
  }
#endif

#if defined(LAZYDDL) || defined(DCOPYDDL)
#ifdef DCOPYDDL
  if (!ddl_running_1) {
    goto exit;
  }
#endif

  if (rc._val != RC_TRUE && old_table_descriptor) {
    int total = version;
  retry:
    oid = 0;
    rc = {RC_INVALID};
    AWAIT old_table_descriptor->GetPrimaryIndex()->GetOID(key, rc, t->xc, oid);
    if (rc._val == RC_TRUE) {
      RETURN InsertRecord(t, key, value);
    } else {
      total--;
      if (total > 0) {
        old_table_descriptor = old_table_descriptors[total - 1];
        ALWAYS_ASSERT(old_table_descriptor != nullptr);
        goto retry;
      }
      RETURN rc_t{RC_ABORT_INTERNAL};
    }
  }
#endif

exit:
  if (rc._val == RC_TRUE) {
    rc_t rc = rc_t{RC_INVALID};
    if (t->IsWaitForNewSchema()) {
      rc = AWAIT t->DDLCDCUpdate(t->new_td, oid, &value, 0);
    } else {
      rc = AWAIT t->Update(table_descriptor, oid, &key, &value);
    }
    RETURN rc;
  } else {
    RETURN rc_t{RC_ABORT_INTERNAL};
  }
}

PROMISE(rc_t)
ConcurrentMasstreeIndex::RemoveRecord(transaction *t, const varstr &key,
                                      TableDescriptor *old_table_descriptor) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  // Search for OID
  OID oid = 0;
  rc_t rc = {RC_INVALID};
  AWAIT GetOID(key, rc, t->xc, oid);

#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
  if (t->IsWaitForNewSchema() && rc._val == RC_TRUE) {
    if (AWAIT t->OverlapCheck(t->old_td, oid)) {
      RETURN rc_t{RC_ABORT_INTERNAL};
    }
  }
#endif

#ifdef LAZYDDL
  if (rc._val != RC_TRUE && old_table_descriptor) {
    return rc_t{RC_TRUE};
  }
#endif

  if (rc._val == RC_TRUE) {
    // Allocate an empty record version as the "new" version
    varstr *null_val = t->string_allocator().next(0);
    TableDescriptor *td = table_descriptor;
    if (t->IsWaitForNewSchema()) {
      td = t->new_td;
    }
    rc_t rc = AWAIT t->Update(td, oid, &key, null_val);
    RETURN rc;
  } else {
    RETURN rc_t{RC_ABORT_INTERNAL};
  }
}

rc_t ConcurrentMasstreeIndex::DoNodeRead(
    transaction *t, const ConcurrentMasstree::node_opaque_t *node,
    uint64_t version) {
  ALWAYS_ASSERT(config::phantom_prot);
  ASSERT(node);
  auto it = t->masstree_absent_set.find(node);
  if (it == t->masstree_absent_set.end()) {
    t->masstree_absent_set[node] = version;
  } else if (it->second != version) {
    return rc_t{RC_ABORT_PHANTOM};
  }
  return rc_t{RC_TRUE};
}

void ConcurrentMasstreeIndex::XctSearchRangeCallback::on_resp_node(
    const typename ConcurrentMasstree::node_opaque_t *n, uint64_t version) {
  VERBOSE(std::cerr << "on_resp_node(): <node=0x" << util::hexify(intptr_t(n))
                    << ", version=" << version << ">" << std::endl);
  VERBOSE(std::cerr << "  " << ConcurrentMasstree::NodeStringify(n)
                    << std::endl);
  if (config::phantom_prot) {
#ifdef SSN
    if (t->flags & transaction::TXN_FLAG_READ_ONLY) {
      return;
    }
#endif
    rc_t rc = DoNodeRead(t, n, version);
    if (rc.IsAbort()) {
      caller_callback->return_code = rc;
    }
  }
}

bool ConcurrentMasstreeIndex::XctSearchRangeCallback::invoke(
    const ConcurrentMasstree *btr_ptr,
    const typename ConcurrentMasstree::string_type &k, dbtuple *v,
    const typename ConcurrentMasstree::node_opaque_t *n, uint64_t version) {
  MARK_REFERENCED(btr_ptr);
  MARK_REFERENCED(n);
  MARK_REFERENCED(version);
  t->ensure_active();
  VERBOSE(std::cerr << "search range k: " << util::hexify(k) << " from <node=0x"
                    << util::hexify(n) << ", version=" << version << ">"
                    << std::endl
                    << "  " << *((dbtuple *)v) << std::endl);
  varstr vv;
  caller_callback->return_code = t->DoTupleRead(v, &vv);
  if (caller_callback->return_code._val == RC_TRUE) {
    return caller_callback->Invoke(k, vv);
  } else if (caller_callback->return_code.IsAbort()) {
    // don't continue the read if the tx should abort
    // ^^^^^ note: see masstree_scan.hh, whose scan() calls
    // visit_value(), which calls this function to determine
    // if it should stop reading.
    return false; // don't continue the read if the tx should abort
  }
  return true;
}

////////////////// End of index interfaces //////////

////////////////// Table interfaces /////////////////
rc_t Table::Insert(transaction &t, varstr *value, OID *out_oid) {
  t.ensure_active();
  OID oid = t.Insert(td, value);
  if (out_oid) {
    *out_oid = oid;
  }
  return oid == INVALID_OID ? rc_t{RC_FALSE} : rc_t{RC_FALSE};
}

rc_t Table::Read(transaction &t, OID oid, varstr *out_value) {
  auto *tuple = sync_wait_coro(
      oidmgr->oid_get_version(td->GetTupleArray(), oid, t.GetXIDContext()));
  rc_t rc = {RC_INVALID};
  if (tuple) {
    // Record exists
    volatile_write(rc._val, t.DoTupleRead(tuple, out_value)._val);
  } else {
    volatile_write(rc._val, RC_FALSE);
  }
  ASSERT(rc._val == RC_FALSE || rc._val == RC_TRUE);
  return rc;
}

PROMISE(rc_t) Table::Update(transaction &t, OID oid, varstr &value) {
  rc_t rc = AWAIT t.Update(td, oid, &value);
  RETURN rc;
}

PROMISE(rc_t) Table::Remove(transaction &t, OID oid) {
  rc_t rc = AWAIT t.Update(td, oid, nullptr);
  RETURN rc;
}

////////////////// End of Table interfaces //////////

OrderedIndex::OrderedIndex(std::string table_name, bool is_primary)
    : is_primary(is_primary) {
  table_descriptor = Catalog::GetTable(table_name);
  self_fid = oidmgr->create_file(true);
}

} // namespace ermia

