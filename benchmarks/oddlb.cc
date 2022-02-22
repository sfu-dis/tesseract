/*
 * An Online DDL Benchmark.
 */
#include "oddlb.h"

#include <sstream>

#include "bench.h"

class oddlb_sequential_worker : public oddlb_base_worker {
 public:
  oddlb_sequential_worker(
      unsigned int worker_id, unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      spin_barrier *barrier_a, spin_barrier *barrier_b)
      : oddlb_base_worker(worker_id, seed, db, open_tables, barrier_a,
                          barrier_b) {}

  double read_ratio = 0.2, write_ratio = 0.8;

  virtual workload_desc_vec get_workload() const {
    workload_desc_vec w;

    if (read_ratio) w.push_back(workload_desc("Read", read_ratio, TxnRead));
    if (write_ratio) w.push_back(workload_desc("RMW", write_ratio, TxnRMW));

    return w;
  }

  virtual ddl_workload_desc_vec get_ddl_workload() const {
    ddl_workload_desc_vec ddl_w;
    ddl_w.push_back(ddl_workload_desc("DDL", 0, TxnDDL));
    return ddl_w;
  }

  static rc_t TxnDDL(bench_worker *w) {
    return static_cast<oddlb_sequential_worker *>(w)->txn_ddl();
  }
  static rc_t TxnRead(bench_worker *w) {
    return static_cast<oddlb_sequential_worker *>(w)->txn_read();
  }
  static rc_t TxnRMW(bench_worker *w) {
    return static_cast<oddlb_sequential_worker *>(w)->txn_rmw();
  }

  rc_t txn_ddl() {
#if SIDDL
  retry:
#endif
    ermia::transaction *txn =
        db->NewTransaction(ermia::transaction::TXN_FLAG_DDL, *arena, txn_buf());

    ermia::varstr schema_value;
    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = ermia::INVALID_OID;
    schema_index->ReadSchemaRecord(txn, rc, *table_key, schema_value, &oid);
    TryVerifyRelaxed(rc);

#ifdef COPYDDL
    struct ermia::schema_record schema;
    ermia::schema_record *old_schema =
        (ermia::schema_record *)schema_value.data();

    uint64_t schema_version = old_schema->v + 1;
    DLOG(INFO) << "Change to a new schema, version: " << schema_version;
    schema.v = schema_version;
    schema.old_v = old_schema->v;
    schema.old_td = old_schema->td;
    schema.state = ermia::ddl::schema_state_type::READY;
    schema.ddl_type = ermia::ddl::ddl_type_map(ermia::config::ddl_type);
    schema.show_index = true;
    schema.reformat_idx = old_schema->reformat_idx;
    schema.constraint_idx = old_schema->constraint_idx;

    rc = rc_t{RC_INVALID};

    if (schema.ddl_type == ermia::ddl::ddl_type::COPY_ONLY ||
        schema.ddl_type == ermia::ddl::ddl_type::COPY_VERIFICATION) {
      char table_name[20];
      snprintf(table_name, 20, "USERTABLE_%lu", schema_version);

      db->CreateTable(table_name);

      schema.td = ermia::Catalog::GetTable(table_name);
      schema.state = ermia::ddl::schema_state_type::NOT_READY;
#ifdef LAZYDDL
      schema.old_index = old_schema->index;
      schema.old_tds[old_schema->v] = old_schema->td;
#endif

#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
      auto *new_table_index =
          new ermia::ConcurrentMasstreeIndex(table_name, true);
      new_table_index->SetArrays(true);
      schema.td->SetPrimaryIndex(new_table_index);
      schema.index = new_table_index;
#else
      ermia::Catalog::GetTable(table_name)
          ->SetPrimaryIndex(old_schema->index, table_name);
      schema.index = ermia::Catalog::GetTable(table_name)->GetPrimaryIndex();
      ALWAYS_ASSERT(old_schema->index == schema.index);
#endif

      txn->set_old_td(old_schema->td);
      txn->add_new_td_map(schema.td);
      txn->add_old_td_map(old_schema->td);
    } else {
      if (schema.ddl_type == ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
        schema_version =
            old_schema->v + ermia::config::no_copy_verification_version_add;
        schema.v = schema_version;
      }
      schema.td = old_schema->td;
      schema.index = old_schema->index;
      schema.state = ermia::ddl::schema_state_type::NOT_READY;

      txn->set_old_td(old_schema->td);
      txn->add_old_td_map(old_schema->td);
    }
    ermia::varstr &v2 = str(sizeof(ermia::schema_record));
    v2.copy_from((char *)&schema, sizeof(ermia::schema_record));

    rc = rc_t{RC_INVALID};
    schema_index->WriteSchemaTable(txn, rc, *table_key, v2);
    TryCatch(rc);

    // New a ddl executor
    ermia::ddl::ddl_executor *ddl_exe = new ermia::ddl::ddl_executor();
    ddl_exe->add_ddl_executor_paras(schema.v, schema.old_v, schema.ddl_type,
                                    schema.reformat_idx, schema.constraint_idx,
                                    schema.td, schema.old_td, schema.index,
                                    schema.state);
    txn->set_ddl_executor(ddl_exe);

    if (schema.ddl_type != ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
#if !defined(LAZYDDL)
      rc = rc_t{RC_INVALID};
      rc = ddl_exe->scan(txn, arena);
      TryCatch(rc);
#endif
    }
#elif defined(BLOCKDDL)
    struct ermia::schema_record schema;
    memcpy(&schema, (char *)schema_value.data(), sizeof(schema));

    uint64_t schema_version = schema.v + 1;
    DLOG(INFO) << "change to new schema: " << schema_version;
    schema.v = schema_version;
    ermia::varstr &v1 = str(sizeof(ermia::schema_record));
    v1.copy_from((char *)&schema, sizeof(ermia::schema_record));

    TryCatch(schema_index->WriteSchemaTable(txn, rc, *table_key, v1));

    // New a ddl executor
    ermia::ddl::ddl_executor *ddl_exe = new ermia::ddl::ddl_executor();
    ddl_exe->add_ddl_executor_paras(schema.v, -1, schema.ddl_type,
                                    schema.reformat_idx, schema.constraint_idx,
                                    schema.td, schema.td, schema.index,
                                    ermia::ddl::schema_state_type::READY);

    txn->set_old_td(schema.td);
    txn->add_old_td_map(schema.td);
    txn->add_new_td_map(schema.td);

    TryCatch(ddl_exe->scan(txn, arena));
#elif SIDDL
    struct ermia::schema_record schema;
    memcpy(&schema, (char *)schema_value.data(), sizeof(schema));

    uint64_t schema_version = schema.v + 1;
    DLOG(INFO) << "change to new schema: " << schema_version;
    schema.v = schema_version;
    ermia::varstr &v = str(sizeof(ermia::schema_record));
    v.copy_from((char *)&schema, sizeof(ermia::schema_record));

    txn->set_old_td(schema.td);

    rc = rc_t{RC_INVALID};
    rc = schema_index->WriteSchemaTable(txn, rc, *table_key, v);
    TryCatch(rc);

    // New a ddl executor
    ermia::ddl::ddl_executor *ddl_exe = new ermia::ddl::ddl_executor();
    ddl_exe->add_ddl_executor_paras(schema.v, -1, schema.ddl_type,
                                    schema.reformat_idx, schema.constraint_idx,
                                    schema.td, schema.td, schema.index,
                                    ermia::ddl::schema_state_type::READY);

    rc = rc_t{RC_INVALID};
    rc = ddl_exe->scan(txn, arena);
    if (rc._val != RC_TRUE) {
      std::cerr << "SI DDL aborts" << std::endl;
      db->Abort(txn);
      goto retry;
    }
    TryCatch(rc);
#endif
    TryCatch(db->Commit(txn));
    DLOG(INFO) << "DDL commit OK";
    return {RC_TRUE};
  }

  rc_t txn_read() {
#ifdef SIDDL
  retry:
#endif
#ifdef LAZYDDL
    ermia::transaction *txn =
        db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
#else
    ermia::transaction *txn = db->NewTransaction(
        ermia::transaction::TXN_FLAG_READ_ONLY, *arena, txn_buf());
#endif

    for (uint i = 0; i < oddl_reps_per_tx; ++i) {
      uint64_t a =
          r.next() % oddl_initial_table_size;  // 0 ~ oddl_initial_table_size-1

      ermia::varstr v1;
      rc_t rc = rc_t{RC_INVALID};
      ermia::OID oid = ermia::INVALID_OID;
      schema_index->ReadSchemaRecord(txn, rc, *table_key, v1, &oid);
      TryCatch(rc);

      ermia::schema_record *schema = (ermia::schema_record *)v1.data();
      uint64_t schema_version = schema->v;

      const oddlb_kv_1::key k2(a);

      ermia::varstr v2;

      rc = rc_t{RC_INVALID};
      oid = ermia::INVALID_OID;

#ifdef COPYDDL
      ermia::ConcurrentMasstreeIndex *table_index =
          (ermia::ConcurrentMasstreeIndex *)schema->index;
#endif

      table_index->GetRecord(txn, rc, Encode(str(Size(k2)), k2), v2, &oid,
                             schema);
      if (rc._val != RC_TRUE) {
        TryCatch(rc_t{RC_ABORT_USER});
      }

      ermia::schema_base *record_test = (ermia::schema_base *)v2.data();

#ifdef SIDDL
      if (record_test->v != schema_version) {
        TryCatch(rc_t{RC_ABORT_USER});
      }
#endif

      if (schema->ddl_type != ermia::ddl::ddl_type::NO_COPY_VERIFICATION &&
          schema->ddl_type != ermia::ddl::ddl_type::VERIFICATION_ONLY &&
          record_test->v != schema_version) {
#ifdef BLOCKDDL
        TryCatch(rc_t{RC_ABORT_USER});
#else
        LOG(FATAL) << "Read: It should get " << schema_version << " ,but get "
                   << record_test->v;
#endif
      }

      if (schema_version == 0) {
        oddlb_kv_1::value *record1_test = (oddlb_kv_1::value *)v2.data();

        ALWAYS_ASSERT(record1_test->o_value_a == a);
        ALWAYS_ASSERT(record1_test->o_value_b == a ||
                      record1_test->o_value_b == 20000000);
      } else {
        if (schema->ddl_type == ermia::ddl::ddl_type::COPY_VERIFICATION ||
            schema->ddl_type == ermia::ddl::ddl_type::COPY_ONLY) {
          oddlb_kv_2::value *record2_test = (oddlb_kv_2::value *)v2.data();

          ALWAYS_ASSERT(record2_test->o_value_a == a);
          ALWAYS_ASSERT(record2_test->o_value_b == schema_version);
          ALWAYS_ASSERT(record2_test->o_value_c == schema_version);
        } else if (schema->ddl_type ==
                   ermia::ddl::ddl_type::VERIFICATION_ONLY) {
          oddlb_kv_1::value *record1_test = (oddlb_kv_1::value *)v2.data();

          ALWAYS_ASSERT(record1_test->o_value_a == a);
          ALWAYS_ASSERT(record1_test->o_value_b == a ||
                        record1_test->o_value_b == 20000000);
        } else if (schema->ddl_type ==
                   ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
          oddlb_kv_2::value *record2_test = (oddlb_kv_2::value *)v2.data();

          ALWAYS_ASSERT(record2_test->o_value_a == a);
          ALWAYS_ASSERT(record2_test->o_value_b == schema_version);
          ALWAYS_ASSERT(record2_test->o_value_c == schema_version);
        }
      }
    }

    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

  rc_t txn_rmw() {
    ermia::transaction *txn =
        db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());

    for (uint i = 0; i < oddl_reps_per_tx; ++i) {
      uint64_t a =
          r.next() % oddl_initial_table_size;  // 0 ~ oddl_initial_table_size-1

#ifdef SIDDL
    retry:
#endif
      ermia::varstr v;
      rc_t rc = rc_t{RC_INVALID};
      ermia::OID oid = ermia::INVALID_OID;
      schema_index->ReadSchemaRecord(txn, rc, *table_key, v, &oid);
      TryCatch(rc);

      ermia::schema_record *schema = (ermia::schema_record *)v.data();
      uint64_t schema_version = schema->v;

#ifdef COPYDDL
      ermia::ConcurrentMasstreeIndex *table_index =
          (ermia::ConcurrentMasstreeIndex *)schema->index;
#endif

      const oddlb_kv_1::key k1(a);

      ermia::varstr v1;

      rc = rc_t{RC_INVALID};
      oid = ermia::INVALID_OID;

      ermia::varstr v2;
      if (schema_version == 0) {
        oddlb_kv_1::value record1;
        record1.o_value_version = schema_version;
        record1.o_value_a = a;
        if (unlikely(ermia::cdc_test)) {
          record1.o_value_b = 20000000;
        } else {
          record1.o_value_b = a;
        }

        v2 = Encode(str(Size(record1)), record1);
      } else {
        if (schema->ddl_type == ermia::ddl::ddl_type::COPY_VERIFICATION ||
            schema->ddl_type == ermia::ddl::ddl_type::COPY_ONLY) {
          oddlb_kv_2::value record2;

          record2.o_value_version = schema_version;
          record2.o_value_a = a;
          record2.o_value_b = schema_version;
          record2.o_value_c = schema_version;

          v2 = Encode(str(Size(record2)), record2);
        } else if (schema->ddl_type ==
                   ermia::ddl::ddl_type::VERIFICATION_ONLY) {
          oddlb_kv_1::value record1;
          record1.o_value_version = schema_version;
          record1.o_value_a = a;
          record1.o_value_b = a;

          v2 = Encode(str(Size(record1)), record1);
        } else if (schema->ddl_type ==
                   ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
          oddlb_kv_2::value record2;

          record2.o_value_version = schema_version;
          record2.o_value_a = a;
          record2.o_value_b = schema_version;
          record2.o_value_c = schema_version;

          v2 = Encode(str(Size(record2)), record2);
        }
      }

      TryCatch(table_index->UpdateRecord(txn, Encode(str(Size(k1)), k1), v2,
                                         schema));
    }

    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }
};

void oddlb_do_test(ermia::Engine *db, int argc, char **argv) {
  oddlb_parse_options(argc, argv);
  oddlb_bench_runner<oddlb_sequential_worker> r(db);
  r.run();
}
