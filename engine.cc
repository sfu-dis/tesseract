#include "engine.h"
#include "benchmarks/tpcc-common.h"
#include "dbcore/rcu.h"
#include "dbcore/sm-thread.h"
#include "txn.h"

namespace ermia {

TableDescriptor *schema_td = NULL;

std::unordered_map<uint32_t, uint32_t> test_map;

thread_local dlog::tls_log tlog;
dlog::tls_log *GetLog() {
  thread_local bool initialized = false;
  if (!initialized) {
    tlog.initialize(config::log_dir.c_str(), thread::MyId(),
                    numa_node_of_cpu(sched_getcpu()), config::log_buffer_mb,
                    config::log_segment_mb);
    initialized = true;
    dlog::tlogs[thread::MyId()] = &tlog;
  }
  return &tlog;
}

class ddl_add_column_scan_callback : public OrderedIndex::ScanCallback {
public:
  ddl_add_column_scan_callback(
      OrderedIndex *index, transaction *t, uint64_t schema_version,
      ermia::str_arena *arena,
      std::function<ermia::varstr *(
          const char *keyp, size_t keylen, const ermia::varstr &value,
          uint64_t schema_version, ermia::transaction *txn,
          ermia::str_arena *arena, ermia::OrderedIndex *index)>
          op)
      : _index(index), _txn(t), _version(schema_version), _op(op), n(0) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const varstr &value) {
    MARK_REFERENCED(value);
    // _arena->reset();

    varstr *k = _arena->next(keylen);
    if (!k) {
      _arena = new ermia::str_arena(ermia::config::arena_size_mb);
      k = _arena->next(keylen);
    }
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
    if (!d_v) {
      _arena = new ermia::str_arena(ermia::config::arena_size_mb);
      d_v = _arena->next(sizeof(str2));
    }
    d_v->copy_from(str2, sizeof(str2));
#else

    /*order_line::key k_ol_temp;
    const order_line::key *k_ol_test = Decode(*k, k_ol_temp);

    if (k_ol_test->ol_w_id == 2 && k_ol_test->ol_d_id == 7 && k_ol_test->ol_o_id == 2 && k_ol_test->ol_number == 2) {
      printf("DDL find this, k_ol_test->ol_o_id: %u, k_ol_test->ol_number: %u\n", k_ol_test->ol_o_id, k_ol_test->ol_number);
    }*/

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

    if (!d_v) {
      _arena = new ermia::str_arena(ermia::config::arena_size_mb);
      // _arena->reset();
      d_v = _arena->next(order_line_sz);
    }
    d_v = &Encode(*d_v, v_ol_1);
#endif

    //varstr *d_v = _op(keyp, keylen, value, _version, _txn, _arena, nullptr);
    //printf("d_v size: %u\n", d_v->size());
#if defined(BLOCKDDL) || defined(SIDDL)
    invoke_status = _index->UpdateRecord(_txn, *k, *d_v);
    if (invoke_status._val != RC_TRUE) {
      printf("DDL normal record update false\n");
      return false;
    }
#elif defined(COPYDDL)
    /*invoke_status = _index->InsertRecord(_txn, *k, *d_v);
    if (invoke_status._val != RC_TRUE) {
      printf("DDL normal record insert false\n");
      return false;
    }*/
    if (_index->InsertRecord(_txn, *k, *d_v)._val != RC_TRUE) {
      printf("DDL normal record insert false\n");
    }
#endif
#if !defined(MICROBENCH)
    rc_t rc = rc_t{RC_INVALID};
    ermia::varstr valptr;
    order_line_1::value v_ol_1_temp;
    _index->GetRecord(_txn, rc, *k, valptr);
    if (rc._val != RC_TRUE) printf("DDL insert failed\n");
    const order_line_1::value *v_ol_2 = Decode(valptr, v_ol_1_temp);
    ALWAYS_ASSERT(v_ol_2->ol_tax == 0);
    ALWAYS_ASSERT(v_ol_2->v == _version);
#endif
    ++n;
    return true;
  }
  std::vector<varstr *> output;
  OrderedIndex *_index;
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
  rc_t invoke_status = rc_t{RC_TRUE};
  size_t n;
};

rc_t ConcurrentMasstreeIndex::WriteSchemaTable(transaction *t, rc_t &rc,
                                               const varstr &key,
                                               varstr &value) {
  rc = UpdateRecord(t, key, value);
  if (rc._val != RC_TRUE) {
    printf("DDL schema update false\n");
    return rc;
  }

  // printf("Update schema table ok\n");
  return rc;
}

void ConcurrentMasstreeIndex::ReadSchemaTable(transaction *t, rc_t &rc,
                                              const varstr &key, varstr &value,
                                              OID *out_oid) {
  GetRecord(t, rc, key, value, out_oid);
  if (rc._val != RC_TRUE)
    printf("Read schema table failed\n");
  ALWAYS_ASSERT(rc._val == RC_TRUE);

#ifdef COPYDDL
  if (t->is_dml()) {
    struct Schema_record schema;
    memcpy(&schema, (char *)value.data(), sizeof(schema));
    ALWAYS_ASSERT(schema.td != nullptr);
    t->schema_read_map[schema.td] = *out_oid;
  }
#endif
}

rc_t ConcurrentMasstreeIndex::WriteNormalTable(
    str_arena *arena, OrderedIndex *index, transaction *t, varstr &value,
    std::function<ermia::varstr *(
        const char *keyp, size_t keylen, const ermia::varstr &value,
        uint64_t schema_version, ermia::transaction *txn,
        ermia::str_arena *arena, ermia::OrderedIndex *index)>
        op,
    OrderedIndex *district_index) {
  rc_t r;
#ifdef COPYDDL
  struct Schema_record schema;
#else
  struct Schema_base schema;
#endif
  memcpy(&schema, (char *)value.data(), sizeof(schema));
  uint64_t schema_version = schema.v;
  printf("schema v: %lu\n", schema_version);

  // Here we assume we can get table and index information
  // such as statistical info (MIN, MAX, AVG, etc.) and index key type
  /*
  char str1[sizeof(uint64_t)];
  uint64_t start = 0;
  memcpy(str1, &start, sizeof(str1));
  varstr *const start_key = arena->next(sizeof(str1));
  start_key->copy_from(str1, sizeof(str1));
  */

  printf("begin op\n");

#ifdef MICROBENCH
  ddl_add_column_scan_callback c_add_column(this, t, schema_version, arena, op);

  char str1[sizeof(uint64_t)];
  uint64_t start = 0;
  memcpy(str1, &start, sizeof(str1));
  varstr *start_key = arena->next(sizeof(str1));
  if (!start_key) {
    arena = new ermia::str_arena(ermia::config::arena_size_mb);
    start_key = arena->next(sizeof(str1));
  }
  start_key->copy_from(str1, sizeof(str1));

  r = index->Scan(t, *start_key, nullptr, c_add_column, arena);
  ALWAYS_ASSERT(c_add_column.n == 10000000);
#else
  ddl_add_column_scan_callback c_add_column(this, t, schema_version, arena, op);

  const order_line::key k_ol_0(1, 1, 1, 0);
  varstr *start_key = arena->next(::Size(k_ol_0));
  if (!start_key) {
    arena = new ermia::str_arena(ermia::config::arena_size_mb);
    start_key = arena->next(::Size(k_ol_0));
  }
  r = index->Scan(t, Encode(*start_key, k_ol_0), nullptr, c_add_column, arena);

  /*for (uint w = 1; w <= NumWarehouses(); w++) {
    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
      for (uint c = 1; c <= NumCustomersPerDistrict(); c++) {
        // test_map[c] = 0;
        const order_line::key k_ol_0(w, d, c, 0);
        const order_line::key k_ol_1(w, d, c,
  std::numeric_limits<int32_t>::max()); varstr *start_key =
  arena->next(::Size(k_ol_0)); if (!start_key) { arena = new
  ermia::str_arena(ermia::config::arena_size_mb); start_key =
  arena->next(::Size(k_ol_0));
        }
        varstr *end_key = arena->next(::Size(k_ol_1));
        if (!end_key) {
          arena = new ermia::str_arena(ermia::config::arena_size_mb);
          end_key = arena->next(::Size(k_ol_1));
        }

        ddl_add_column_scan_callback c_add_column(this, t, schema_version,
  arena, op); r = index->Scan(t, Encode(*start_key, k_ol_0), &Encode(*end_key,
  k_ol_1), c_add_column, arena); if (c_add_column.n < 5 || c_add_column.n > 15)
  { printf("c_add_column.n: %zu\n", c_add_column.n);
        }
        if (r._val != RC_TRUE) {
          printf("DDL scan false\n");
          return {RC_ABORT_INTERNAL};
        }
      }
    }
  }

  for (uint w = 1; w <= NumWarehouses(); w++) {
    // printf("w: %u\n", w);
    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
      // printf("d: %u\n", d);
      //for (uint l = 1; l <= 15; l++) {
        // printf("c: %u\n", c);

        const new_order::key k_no_0(w, d, 0);
        const new_order::key k_no_1(w, d,
                                    std::numeric_limits<int32_t>::max());
        new_order_scan_callback new_order_c;

        varstr *no_start_key = arena->next(::Size(k_no_0));
        if (!no_start_key) {
          arena = new ermia::str_arena(ermia::config::arena_size_mb);
          no_start_key = arena->next(::Size(k_no_0));
        }
        varstr *no_end_key = arena->next(::Size(k_no_1));
        if (!no_end_key) {
          arena = new ermia::str_arena(ermia::config::arena_size_mb);
          no_end_key = arena->next(::Size(k_no_1));
        }

        {
          r = district_index
                        ->Scan(t, Encode(*no_start_key, k_no_0),
                               &Encode(*no_end_key, k_no_1), new_order_c);

          if (r._val != RC_TRUE) {
            printf("DDL scan new order false\n");
            return {RC_ABORT_INTERNAL};
          }
        }

        const new_order::key *k_no = new_order_c.get_key();
        if (unlikely(!k_no)) continue;

        // test_map[k_no->no_o_id] = 0;

        const order_line::key k_ol_0(w, d, k_no->no_o_id, 0);
        const order_line::key k_ol_1(w, d, k_no->no_o_id,
  std::numeric_limits<int32_t>::max()); varstr *start_key =
  arena->next(::Size(k_ol_0)); if (!start_key) { arena = new
  ermia::str_arena(ermia::config::arena_size_mb); start_key =
  arena->next(::Size(k_ol_0));
        }
        varstr *end_key = arena->next(::Size(k_ol_1));
        if (!end_key) {
          arena = new ermia::str_arena(ermia::config::arena_size_mb);
          end_key = arena->next(::Size(k_ol_1));
        }

        ddl_add_column_scan_callback c_add_column(this, t, schema_version,
  arena, op); r = index->Scan(t, Encode(*start_key, k_ol_0), &Encode(*end_key,
  k_ol_1), c_add_column, arena); if (c_add_column.n < 5 || c_add_column.n > 15)
  { printf("c_add_column.n: %zu\n", c_add_column.n);
        }
        if (r._val != RC_TRUE) {
          printf("DDL scan false\n");
          return {RC_ABORT_INTERNAL};
        }
      //}
    }
  }*/

  /*r = index->Scan(t, *start_key, nullptr, c_add_column, arena);
  if (r._val != RC_TRUE) {
    printf("DDL scan false\n");
    return {RC_ABORT_INTERNAL};
  }*/
#endif

  if (r._val != RC_TRUE) {
    printf("DDL scan false\n");
    return {RC_ABORT_INTERNAL};
  }

  // printf("scan invoke status: %hu\n", c_add_column.invoke_status._val);

  return {RC_TRUE};
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

#if defined(COPYDDL) && !defined(LAZYDDL)
bool ConcurrentMasstreeIndex::changed_data_capture(
    transaction *t, uint64_t begin_csn, uint64_t end_csn, uint32_t thread_id,
    uint32_t begin_log, uint32_t end_log) {
  // printf("cdc begins\n");
  bool ddl_end_tmp = true, ddl_end_flag = false;
  ;
  FID fid = t->old_td->GetTupleFid();
  // printf("cdc on fid: %u\n", fid);
  ermia::str_arena *arena = new ermia::str_arena(ermia::config::arena_size_mb);
  std::vector<ermia::str_arena *> arenas;
  arenas.emplace_back(arena);
  ermia::ConcurrentMasstreeIndex *table_secondary_index = nullptr;
  if (t->new_td->GetSecIndexes().size()) {
    table_secondary_index =
              (ermia::ConcurrentMasstreeIndex *) (*(t->new_td->GetSecIndexes().begin()));
    ALWAYS_ASSERT(table_secondary_index);
  }
  uint index = begin_log;
  for (uint i = begin_log; i <= end_log; i++) {
    dlog::tls_log *tlog = dlog::tlogs[i];
    uint64_t csn = volatile_read(pcommit::_tls_durable_csn[i]);
    if (tlog && i != thread::MyId() && i != thread_id && csn) {
      // printf("i: %d, index: %u\n", i, index);
      tlog->last_flush();
      std::vector<dlog::segment> *segments = tlog->get_segments();
      // printf("log %u cdc, seg size: %lu\n", tlog->get_id(), segments->size());
      if (segments->size() > 1)
        printf("> 1 files at log %u\n", tlog->get_id());
      bool stop_scan = false;
      uint64_t offset_in_seg = t->cdc_offsets[index];
      uint64_t offset_increment = 0;
      uint64_t last_csn = 0;
      // printf("offset_in_seg: %lu\n", offset_in_seg);
      for (std::vector<dlog::segment>::reverse_iterator seg =
               segments->rbegin();
           seg != segments->rend(); seg++) {
        uint64_t data_sz = seg->size;
        // printf("seg size: %lu\n", data_sz);
        char *data_buf = (char *)malloc(data_sz - offset_in_seg);
        t->get_bufs().emplace_back(data_buf);
        size_t m = os_pread(seg->fd, (char *)data_buf, data_sz - offset_in_seg,
                            offset_in_seg);
        uint64_t block_sz = sizeof(dlog::log_block),
                 logrec_sz = sizeof(dlog::log_record),
                 tuple_sc = sizeof(dbtuple);

	int insert_total = 0, update_total = 0;
	int insert_fail = 0, update_fail = 0;
        while (offset_increment < data_sz - offset_in_seg) {
          dlog::log_block *header =
              (dlog::log_block *)(data_buf + offset_increment);
          if ((end_csn && begin_csn <= header->csn && header->csn <= end_csn) ||
              (!end_csn && begin_csn <= header->csn)) {
            last_csn = header->csn;
            uint64_t offset_in_block = 0;
            varstr *insert_key, *update_key, *insert_key_idx;
            while (offset_in_block < header->payload_size) {
              dlog::log_record *logrec =
                  (dlog::log_record *)(data_buf + offset_increment + block_sz +
                                       offset_in_block);
              ALWAYS_ASSERT(logrec->oid);
              ALWAYS_ASSERT(logrec->fid);
              // printf("logrec->fid: %u, logrec->rec_size: %u\n", logrec->fid, logrec->rec_size);
              ALWAYS_ASSERT(logrec->rec_size);
              ALWAYS_ASSERT(logrec->data);
              // varstr valptr;
              // rc_t rc = rc_t{RC_INVALID};
              // OID oid = INVALID_OID;
              // ConcurrentMasstree::versioned_node_t sinfo;

	      if (logrec->fid != fid) {
	        offset_in_block += logrec->rec_size;
	        continue;
	      }

              if (logrec->type == dlog::log_record::logrec_type::INSERT) {
                // this->GetRecord(t, rc, *insert_key, valptr);
                // if (rc._val == RC_FALSE) {
                // bool found = AWAIT masstree_.search(*insert_key, oid,
                // t->xc->begin_epoch, &sinfo); if (!found) {
                dbtuple *tuple = (dbtuple *)(logrec->data);
                // printf("cdc insert, tuple->size: %u\n", tuple->size);
		OID oid = 0;
                varstr value(tuple->get_value_start(), tuple->size);
                
		order_line::value v_ol_temp;
                const order_line::value *v_ol = Decode(value, v_ol_temp);

                order_line_1::value v_ol_1;
                v_ol_1.ol_i_id = v_ol->ol_i_id;
                // printf("cdc insert, v_ol->ol_i_id: %u\n", v_ol->ol_i_id);
		v_ol_1.ol_delivery_d = v_ol->ol_delivery_d;
                v_ol_1.ol_amount = v_ol->ol_amount;
                v_ol_1.ol_supply_w_id = v_ol->ol_supply_w_id;
                v_ol_1.ol_quantity = v_ol->ol_quantity;
                v_ol_1.v = v_ol->v + 1;
                // printf("cdc insert, v_ol->v: %lu\n", v_ol->v);
		v_ol_1.ol_tax = 0;
		
		order_line_1::key k_ol_temp;
		const order_line_1::key *k_ol = Decode(*insert_key, k_ol_temp);

                // printf("cdc insert, ol_w_id: %u, ol_d_id: %u, ol_o_id: %u\n", k_ol->ol_w_id, k_ol->ol_d_id, k_ol->ol_o_id);

                // test_map[k_ol->ol_o_id] = 1;

                const size_t order_line_sz = ::Size(v_ol_1);
                varstr *d_v = arena->next(order_line_sz);

                if (!d_v) {
                  arena = new ermia::str_arena(ermia::config::arena_size_mb);
                  arenas.emplace_back(arena);
                  // arena->reset();
                  d_v = arena->next(order_line_sz);
                }
                d_v = &Encode(*d_v, v_ol_1);
		
		insert_total++;
                while (this->InsertRecord(t, *insert_key, *d_v, &oid)._val !=
                       RC_TRUE) {
                }
                if (table_secondary_index) {
		//   table_secondary_index->InsertOID(t, *insert_key_idx, oid);
		}
                //}
              } else if (logrec->type ==
                         dlog::log_record::logrec_type::UPDATE) {
                // this->GetRecord(t, rc, *update_key, valptr);
                // if (rc._val == RC_FALSE) {
                // bool found = AWAIT masstree_.search(*update_key, oid,
                // t->xc->begin_epoch, &sinfo); if (!found) {
                dbtuple *tuple = (dbtuple *)(logrec->data);
                // printf("cdc update, tuple->size: %u\n", tuple->size);
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
                if (!d_v) {
                  arena = new ermia::str_arena(ermia::config::arena_size_mb);
                  // arenas.emplace_back(arena);
                  d_v = arena->next(sizeof(str2));
                }
                d_v->copy_from(str2, sizeof(str2));
#else
                order_line::value v_ol_temp;
                const order_line::value *v_ol = Decode(value, v_ol_temp);

                order_line_1::value v_ol_1;
                v_ol_1.ol_i_id = v_ol->ol_i_id;
		// printf("cdc update, v_ol->ol_i_id: %u\n", v_ol->ol_i_id);
                v_ol_1.ol_delivery_d = v_ol->ol_delivery_d;
                v_ol_1.ol_amount = v_ol->ol_amount;
                v_ol_1.ol_supply_w_id = v_ol->ol_supply_w_id;
                v_ol_1.ol_quantity = v_ol->ol_quantity;
                v_ol_1.v = v_ol->v + 1;
                // printf("cdc update, v_ol->v: %lu\n", v_ol->v);
                v_ol_1.ol_tax = 0;

		order_line_1::key k_ol_temp;
                const order_line_1::key *k_ol = Decode(*insert_key, k_ol_temp);

                // printf("cdc update, ol_w_id: %u, ol_d_id: %u, ol_o_id: %u\n", k_ol->ol_w_id, k_ol->ol_d_id, k_ol->ol_o_id);

                // test_map[k_ol->ol_o_id] = 2;

                const size_t order_line_sz = ::Size(v_ol_1);
                d_v = arena->next(order_line_sz);

                if (!d_v) {
                  arena = new ermia::str_arena(ermia::config::arena_size_mb);
                  arenas.emplace_back(arena);
                  // arena->reset();
                  d_v = arena->next(order_line_sz);
                }
                d_v = &Encode(*d_v, v_ol_1);
#endif

                update_total++;
                if (this->UpdateRecord(t, *update_key, *d_v)._val != RC_TRUE) {
                  // update_fail++;
                }

              } else if (logrec->type ==
                         dlog::log_record::logrec_type::INSERT_KEY) {
#ifdef MICROBENCH
                insert_key = new varstr(logrec->data + sizeof(varstr), 8);
#else
                insert_key = new varstr(logrec->data + sizeof(varstr), 16);
#endif
              } else if (logrec->type ==
                         dlog::log_record::logrec_type::UPDATE_KEY) {
#ifdef MICROBENCH
                update_key = new varstr(logrec->data + sizeof(varstr), 8);
#else
                update_key = new varstr(logrec->data + sizeof(varstr), 16);
#endif
              } else if (logrec->type ==
                         dlog::log_record::logrec_type::INVALID) {
                printf("Invalid type\n");
              } else {
                printf("unknown type\n");
              }

              /*
              Object* obj = new (MM::allocate(tuple->size))
              Object(logrec->data, NULL_PTR, 0, config::eager_warm_up());
              obj->SetClsn(logrec->data);
              ASSERT(obj->GetClsn().asi_type() == fat_ptr::ASI_LOG);
              */

              offset_in_block += logrec->rec_size;
            }
          } else {
            // stop_scan = true;
            if (end_csn && header->csn > end_csn) {
              // printf("header->csn > end_csn\n");
              ddl_end_flag = true;
              stop_scan = true;
              break;
            }
          }
          offset_increment += header->payload_size + block_sz;
          // printf("payload_size: %u\n", header->payload_size);
        }
        // free(data_buf);
        /*for (std::vector<ermia::str_arena *>::iterator arena = arenas.begin();
        arena != arenas.end(); arena++) { free((*arena)->get_str());
        }*/
        if (insert_total != 0 || update_total != 0)
          ddl_end_tmp = false;
        // printf("insert total: %d, insert fail: %d, update total: %d, update fail: %d\n", insert_total, insert_fail, update_total, update_fail);
        if (update_fail > 0)
          printf("update_fail, CDC conflicts with copy, %d\n", update_fail);

        if (insert_fail > 0)
          printf("insert_fail, CDC conflicts with copy, %d\n", insert_fail);

        // printf("offset_in_seg: %lu\n", offset_in_seg);

        // printf("csn: %lu\n", last_csn);

        if (stop_scan)
          break;
      }

      uint64_t &offset = t->cdc_offsets.at(index);
      offset = offset_in_seg + offset_increment;
      index++;
      // printf("index: %u\n", index);
      if (index > end_log || index >= t->cdc_offsets.size() - 1)
        break;
    }
  }
  // if (ddl_end_tmp) printf("all log captured\n");
  // printf("cdc finished\n");
  return ddl_end_flag || ddl_end_tmp;
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
}

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
          // printf("retry with total: %d\n", total);
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

  ASSERT((char *)key.data() == (char *)&key + sizeof(varstr));
  t->ensure_active();

  OID oid = 0;

#if defined(COPYDDL) && !defined(LAZYDDL)
  if (t->is_ddl()) {
    rc_t rc = {RC_INVALID};
    AWAIT GetOID(key, rc, t->xc, oid);

    if (rc._val == RC_TRUE) {
      if (out_oid) {
        *out_oid = oid;
      }

      RETURN rc_t{RC_TRUE};
    }
  }
#endif

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
  oid = t->Insert(table_descriptor, &key, &value, &tuple);

  // Done with table record, now set up index
  ASSERT((char *)key.data() == (char *)&key + sizeof(varstr));
  if (!AWAIT InsertOID(t, key, oid)) {
    if (config::enable_chkpt) {
      volatile_write(table_descriptor->GetKeyArray()->get(oid)->_ptr, 0);
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
    auto *key_array = table_descriptor->GetKeyArray();
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

  // Search for OID
  OID oid = 0;
  rc_t rc = {RC_INVALID};
  AWAIT GetOID(key, rc, t->xc, oid);

#ifdef LAZYDDL
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
        // printf("retry with total: %d\n", total);
        old_table_descriptor = old_table_descriptors[total - 1];
        ALWAYS_ASSERT(old_table_descriptor != nullptr);
        goto retry;
      }
      RETURN rc_t{RC_ABORT_INTERNAL};
    }
  }
#endif

  if (rc._val == RC_TRUE) {
    RETURN t->Update(table_descriptor, oid, &key, &value);
  } else {
    /*#if defined(COPYDDL) && !defined(LAZYDDL)
        if (t->is_ddl()) {
          RETURN InsertRecord(t, key, value);
        }
    #endif*/
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

#ifdef LAZYDDL
  if (rc._val != RC_TRUE && old_table_descriptor) {
    return rc_t{RC_TRUE};
  }
#endif

  if (rc._val == RC_TRUE) {
    // Allocate an empty record version as the "new" version
    varstr *null_val = t->string_allocator().next(0);
    RETURN t->Update(table_descriptor, oid, &key, null_val);
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
rc_t Table::Insert(transaction &t, varstr *k, varstr *value, OID *out_oid) {
  t.ensure_active();
  OID oid = t.Insert(td, k, value);
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

rc_t Table::Update(transaction &t, OID oid, varstr &value) {
  return t.Update(td, oid, &value);
}

rc_t Table::Remove(transaction &t, OID oid) {
  return t.Update(td, oid, nullptr);
}

////////////////// End of Table interfaces //////////

OrderedIndex::OrderedIndex(std::string table_name, bool is_primary)
    : is_primary(is_primary) {
  table_descriptor = Catalog::GetTable(table_name);
  self_fid = oidmgr->create_file(true);
}

} // namespace ermia

