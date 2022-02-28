/*
 * An Online DDL Benchmark.
 */
#include "oddlb.h"

#include <sstream>

#include "bench.h"

extern OddlbWorkload oddlb_workload;

class oddlb_sequential_worker : public oddlb_base_worker {
 public:
  oddlb_sequential_worker(
      unsigned int worker_id, unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      spin_barrier *barrier_a, spin_barrier *barrier_b)
      : oddlb_base_worker(worker_id, seed, db, open_tables, barrier_a,
                          barrier_b) {}

  virtual workload_desc_vec get_workload() const {
    workload_desc_vec w;

    if (oddlb_workload.read_percent()) {
      w.push_back(workload_desc(
          "Read", double(oddlb_workload.read_percent()) / 100.0, TxnRead));
    }
    if (oddlb_workload.update_percent()) {
      w.push_back(workload_desc(
          "RMW", double(oddlb_workload.update_percent()) / 100.0, TxnRMW));
    }

    return w;
  }

  virtual ddl_workload_desc_vec get_ddl_workload() const {
    ddl_workload_desc_vec ddl_w;
    for (int i = 0; i < ermia::config::ddl_total; i++) {
      ddl_w.push_back(ddl_workload_desc("DDL", 0, TxnDDL, ddl_examples[i]));
    }
    return ddl_w;
  }

  static rc_t TxnDDL(bench_worker *w, uint32_t ddl_example) {
    return static_cast<oddlb_sequential_worker *>(w)->txn_ddl(ddl_example);
  }
  static rc_t TxnRead(bench_worker *w) {
    return static_cast<oddlb_sequential_worker *>(w)->txn_read();
  }
  static rc_t TxnRMW(bench_worker *w) {
    return static_cast<oddlb_sequential_worker *>(w)->txn_rmw();
  }

  rc_t txn_ddl(uint32_t ddl_example) {
#if SIDDL
  retry:
#endif
    ermia::transaction *txn =
        db->NewTransaction(ermia::transaction::TXN_FLAG_DDL, *arena, txn_buf());

    ermia::varstr valptr;
    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = ermia::INVALID_OID;
    schema_index->ReadSchemaRecord(txn, rc, *table_key, valptr, &oid);

    struct ermia::schema_record schema;
    schema_kv::value schema_value_temp;
    const schema_kv::value *old_schema_value =
        Decode(valptr, schema_value_temp);
    schema.value_to_record(old_schema_value);
    schema.ddl_type = get_example_ddl_type(ddl_example);

#ifdef COPYDDL
    schema.old_v = schema.v;
    uint64_t schema_version = schema.old_v + 1;
    DLOG(INFO) << "Change to a new schema, version: " << schema_version;
    schema.v = schema_version;
    schema.old_td = schema.td;
    schema.state = ermia::ddl::schema_state_type::NOT_READY;
    schema.show_index = true;

    rc = rc_t{RC_INVALID};

    if (schema.ddl_type == ermia::ddl::ddl_type::COPY_ONLY ||
        schema.ddl_type == ermia::ddl::ddl_type::COPY_VERIFICATION) {
      char table_name[20];
      snprintf(table_name, 20, "USERTABLE_%lu", schema_version);

      db->CreateTable(table_name);

      schema.td = ermia::Catalog::GetTable(table_name);
      schema.old_index = schema.index;
#ifdef LAZYDDL
      schema.old_tds[schema.old_v] = schema.old_td;
      schema.old_tds_total = schema.v;
#endif

#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
      auto *new_table_index =
          new ermia::ConcurrentMasstreeIndex(table_name, true);
      new_table_index->SetArrays(true);
      schema.td->SetPrimaryIndex(new_table_index);
      schema.index = new_table_index;
#else
      ermia::Catalog::GetTable(table_name)
          ->SetPrimaryIndex(schema.old_index, table_name);
      schema.index = ermia::Catalog::GetTable(table_name)->GetPrimaryIndex();
#endif

      txn->set_old_td(schema.old_td);
      txn->add_new_td_map(schema.td);
      txn->add_old_td_map(schema.old_td);
    } else {
      if (schema.ddl_type == ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
        schema_version =
            schema.old_v + ermia::config::no_copy_verification_version_add;
        schema.v = schema_version;
        schema.reformats_total =
            ermia::config::no_copy_verification_version_add;
      }

      txn->set_old_td(schema.td);
      txn->add_old_td_map(schema.td);
    }

    schema_kv::value new_schema_value;
    schema.record_to_value(new_schema_value);

    rc = rc_t{RC_INVALID};
    schema_index->WriteSchemaTable(
        txn, rc, *table_key,
        Encode(str(Size(new_schema_value)), new_schema_value));
    TryCatch(rc);

    // New a ddl executor
    ermia::ddl::ddl_executor *ddl_exe =
        new ermia::ddl::ddl_executor(schema.ddl_type);
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
#else
    uint64_t schema_version = schema.v + 1;
    DLOG(INFO) << "change to new schema: " << schema_version;
    schema.v = schema_version;

    schema_kv::value new_schema_value;
    schema.record_to_value(new_schema_value);

    TryCatch(schema_index->WriteSchemaTable(
        txn, rc, *table_key,
        Encode(str(Size(new_schema_value)), new_schema_value)));

    // New a ddl executor
    ermia::ddl::ddl_executor *ddl_exe =
        new ermia::ddl::ddl_executor(schema.ddl_type);
    ddl_exe->add_ddl_executor_paras(schema.v, -1, schema.ddl_type,
                                    schema.reformat_idx, schema.constraint_idx,
                                    schema.td, schema.td, schema.index,
                                    ermia::ddl::schema_state_type::READY);

    txn->set_ddl_executor(ddl_exe);
    txn->set_old_td(schema.td);
    txn->add_old_td_map(schema.td);
    txn->add_new_td_map(schema.td);

#ifdef BLOCKDDL    
    TryCatch(ddl_exe->scan(txn, arena));
#elif SIDDL
    rc = rc_t{RC_INVALID};
    rc = ddl_exe->scan(txn, arena);
    if (rc._val != RC_TRUE) {
      std::cerr << "SI DDL aborts" << std::endl;
      db->Abort(txn);
      goto retry;
    }
    TryCatch(rc);
#endif
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

    ermia::varstr v1;
    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = ermia::INVALID_OID;
    schema_index->ReadSchemaRecord(txn, rc, *table_key, v1, &oid);
    TryCatch(rc);

    schema_kv::value schema_value_temp;
    const schema_kv::value *schema_value = Decode(v1, schema_value_temp);
    ermia::schema_record schema;
    schema.value_to_record(schema_value);
    uint64_t schema_version = schema.v;

    for (uint i = 0; i < oddlb_reps_per_tx; ++i) {
      uint64_t a = r.next() %
                   oddlb_initial_table_size;  // 0 ~ oddlb_initial_table_size-1

      const oddlb_kv_1::key k2(a);

      ermia::varstr v2;

      rc = rc_t{RC_INVALID};
      oid = ermia::INVALID_OID;

#ifdef COPYDDL
      ermia::ConcurrentMasstreeIndex *table_index =
          (ermia::ConcurrentMasstreeIndex *)schema.index;
#endif

      table_index->GetRecord(txn, rc, Encode(str(Size(k2)), k2), v2, &oid,
                             &schema);
      if (rc.IsAbort()) {
        TryCatch(rc);
      } else if (rc._val == RC_FALSE) {
        TryCatch(rc_t{RC_ABORT_USER});
      }

      oddlb_kv_1::value *record_test = (oddlb_kv_1::value *)v2.data();

#ifdef SIDDL
      if (record_test->o_value_version != schema_version) {
        TryCatch(rc_t{RC_ABORT_USER});
      }
#endif

      if (schema.ddl_type != ermia::ddl::ddl_type::VERIFICATION_ONLY &&
          record_test->o_value_version != schema_version) {
        LOG(FATAL) << "Read: It should get " << schema_version << " ,but get "
                   << record_test->o_value_version;
      }

      if (schema_version == 0) {
        oddlb_kv_1::value *record1_test = (oddlb_kv_1::value *)v2.data();

        ALWAYS_ASSERT(record1_test->o_value_a == a);
        ALWAYS_ASSERT(record1_test->o_value_b == a ||
                      record1_test->o_value_b == 20000000);
      } else {
        if (schema.ddl_type == ermia::ddl::ddl_type::COPY_VERIFICATION ||
            schema.ddl_type == ermia::ddl::ddl_type::COPY_ONLY) {
          oddlb_kv_2::value *record2_test = (oddlb_kv_2::value *)v2.data();

          ALWAYS_ASSERT(record2_test->o_value_a == a);
          ALWAYS_ASSERT(record2_test->o_value_b == schema_version);
          ALWAYS_ASSERT(record2_test->o_value_c == schema_version);
        } else if (schema.ddl_type == ermia::ddl::ddl_type::VERIFICATION_ONLY) {
          oddlb_kv_1::value *record1_test = (oddlb_kv_1::value *)v2.data();

          ALWAYS_ASSERT(record1_test->o_value_a == a);
          ALWAYS_ASSERT(record1_test->o_value_b == a ||
                        record1_test->o_value_b == 20000000);
        } else if (schema.ddl_type ==
                   ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
          oddlb_kv_6::value *record2_test = (oddlb_kv_6::value *)v2.data();

          ALWAYS_ASSERT(record2_test->o_value_a == a);
          ALWAYS_ASSERT(record2_test->o_value_b == schema_version);
          ALWAYS_ASSERT(record2_test->o_value_c == schema_version);
          if (schema.v == 2) {
            ALWAYS_ASSERT(record2_test->o_value_d == schema_version);
          }
          if (schema.v == 3) {
            ALWAYS_ASSERT(record2_test->o_value_e == schema_version);
          }
          if (schema.v == 4) {
            ALWAYS_ASSERT(record2_test->o_value_f == schema_version);
          }
          if (schema.v == 5) {
            ALWAYS_ASSERT(record2_test->o_value_g == schema_version);
          }
        }
      }
    }

    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

  rc_t txn_rmw() {
    ermia::transaction *txn =
        db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());

    ermia::varstr v;
    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = ermia::INVALID_OID;
    schema_index->ReadSchemaRecord(txn, rc, *table_key, v, &oid);
    TryCatch(rc);

    schema_kv::value schema_value_temp;
    const schema_kv::value *schema_value = Decode(v, schema_value_temp);
    ermia::schema_record schema;
    schema.value_to_record(schema_value);
    uint64_t schema_version = schema.v;

    for (uint i = 0; i < oddlb_reps_per_tx; ++i) {
      uint64_t a = r.next() %
                   oddlb_initial_table_size;  // 0 ~ oddlb_initial_table_size-1

#ifdef SIDDL
    retry:
#endif
#ifdef COPYDDL
      ermia::ConcurrentMasstreeIndex *table_index =
          (ermia::ConcurrentMasstreeIndex *)schema.index;
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
        if (unlikely(ermia::ddl::cdc_test)) {
          record1.o_value_b = 20000000;
        } else {
          record1.o_value_b = a;
        }

        v2 = Encode(str(Size(record1)), record1);
      } else {
        if (schema.ddl_type == ermia::ddl::ddl_type::COPY_VERIFICATION ||
            schema.ddl_type == ermia::ddl::ddl_type::COPY_ONLY) {
          oddlb_kv_2::value record2;

          record2.o_value_version = schema_version;
          record2.o_value_a = a;
          record2.o_value_b = schema_version;
          record2.o_value_c = schema_version;

          v2 = Encode(str(Size(record2)), record2);
        } else if (schema.ddl_type == ermia::ddl::ddl_type::VERIFICATION_ONLY) {
          oddlb_kv_1::value record1;
          record1.o_value_version = schema_version;
          record1.o_value_a = a;
          record1.o_value_b = a;

          v2 = Encode(str(Size(record1)), record1);
        } else if (schema.ddl_type ==
                   ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
          v2 = GenerateValue(a, &schema);
        }
      }

      TryCatch(table_index->UpdateRecord(txn, Encode(str(Size(k1)), k1), v2,
                                         &schema));
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
