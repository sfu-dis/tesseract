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
      : oddlb_base_worker(worker_id, seed, db, open_tables, barrier_a, barrier_b) {}

  virtual workload_desc_vec get_workload() const {
    workload_desc_vec w;

    if (oddlb_workload.read_percent()) {
      w.push_back(workload_desc("Read", double(oddlb_workload.read_percent()) / 100.0, TxnRead));
    }
    if (oddlb_workload.update_percent()) {
      w.push_back(workload_desc("RMW", double(oddlb_workload.update_percent()) / 100.0, TxnRMW));
    }

    return w;
  }

  static const std::string get_example_name(uint32_t ddl_example) {
    switch (ddl_example) {
      case 0:
        return "DDL_ADD_COLUMN";
      case 1:
        return "DDL_ADD_COLUMN_NO_COPY";
      case 2:
        return "DDL_ADD_CONSTRAINT";
      case 3:
        return "DDL_ADD_COLUMN_AND_ADD_CONSTRAINT";
      default:
        LOG(FATAL) << "Not supported";
    }
  }

  virtual ddl_workload_desc_vec get_ddl_workload() const {
    ddl_workload_desc_vec ddl_w;
    for (int i = 0; i < ermia::config::ddl_total; i++) {
      ddl_w.push_back(ddl_workload_desc(get_example_name(ddl_examples[i]), 0, TxnDDL, ddl_examples[i]));
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

#ifdef DDL
  inline uint64_t oddlb_read_schema(ermia::transaction *txn, ermia::schema_record &out_schema) {
    ermia::varstr v1;
    ermia::OID oid = ermia::INVALID_OID;
    ermia::catalog::read_schema(txn, schema_index, table_index, *table_key, v1, &oid);

    schema_kv::value schema_value_temp;
    const schema_kv::value *schema_value = Decode(v1, schema_value_temp);
    out_schema.value_to_record(schema_value);
    return out_schema.v;
  }
#endif

  rc_t txn_ddl(uint32_t ddl_example) {
#ifdef DDL

#ifdef SIDDL
  retry:
#endif
    ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_DDL, *arena, txn_buf());
    struct ermia::schema_record schema;
    oddlb_read_schema(txn, schema);
    schema.ddl_type = get_example_ddl_type(ddl_example);
    ermia::ddl::ddl_executor *ddl_exe = txn->get_ddl_executor();
    ddl_exe->set_ddl_type(schema.ddl_type);

#ifdef COPYDDL
    schema.old_v = schema.v;
    uint64_t schema_version = schema.old_v + 1;
    DLOG(INFO) << "Change to a new schema, version: " << schema_version;
    schema.v = schema_version;
    schema.old_td = schema.td;
    schema.state = ermia::ddl::schema_state_type::NOT_READY;
    schema.show_index = true;

    if (schema.ddl_type == ermia::ddl::ddl_type::COPY_ONLY || schema.ddl_type == ermia::ddl::ddl_type::COPY_VERIFICATION) {
      char table_name[20];
      snprintf(table_name, 20, "USERTABLE_%lu", schema_version);

      db->CreateTable(table_name, false);

      schema.td = ermia::Catalog::GetTable(table_name);
      schema.old_index = schema.index;
#ifdef LAZYDDL
      schema.old_tds[schema.old_v] = schema.old_td;
      schema.old_tds_total = schema.v;
#endif

#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
      db->CreateMasstreePrimaryIndex(table_name, std::string(table_name));
#else
      schema.td->SetPrimaryIndex(schema.old_index, table_name);
#endif
      schema.index = schema.td->GetPrimaryIndex();

      ddl_exe->set_old_td(schema.old_td);
      ddl_exe->add_new_td_map(schema.td);
      ddl_exe->add_old_td_map(schema.old_td);
    } else {
      if (schema.ddl_type == ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
        schema_version = schema.old_v + ermia::config::no_copy_verification_version_add;
        schema.v = schema_version;
        schema.reformats_total = ermia::config::no_copy_verification_version_add;
      }

      ddl_exe->set_old_td(schema.td);
      ddl_exe->add_old_td_map(schema.td);
    }

    schema_kv::value new_schema_value;
    schema.record_to_value(new_schema_value);

    auto rc = ermia::catalog::write_schema(txn, schema_index, *table_key,
              Encode(str(Size(new_schema_value)), new_schema_value), oid);
    TryCatch(rc);

    ddl_exe->add_ddl_executor_paras(schema.v, schema.old_v, schema.ddl_type,
                                    schema.reformat_idx, schema.constraint_idx,
                                    schema.td, schema.old_td, schema.index,
                                    schema.state);

#ifndef LAZYDDL
    if (schema.ddl_type != ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
      rc = rc_t{RC_INVALID};
      rc = ddl_exe->scan(txn, arena);
      TryCatch(rc);
    }
#endif
#else  // COPY_DDL
    uint64_t schema_version = schema.v + 1;
    DLOG(INFO) << "Changing to new schema: version " << schema_version;
    schema.v = schema_version;

    schema_kv::value new_schema_value;
    schema.record_to_value(new_schema_value);

    auto rc = ermia::catalog::write_schema(
        txn, schema_index, *table_key,
        Encode(str(Size(new_schema_value)), new_schema_value), nullptr);
    TryCatch(rc);

    ddl_exe->add_ddl_executor_paras(schema.v, -1, schema.ddl_type,
                                    schema.reformat_idx, schema.constraint_idx,
                                    schema.td, schema.td, schema.index,
                                    ermia::ddl::schema_state_type::READY);

    ddl_exe->set_old_td(schema.td);
    ddl_exe->add_old_td_map(schema.td);
    ddl_exe->add_new_td_map(schema.td);

#ifdef BLOCKDDL
    TryCatch(ddl_exe->scan(txn, arena));
#elif SIDDL
    rc = rc_t{RC_INVALID};
    rc = ddl_exe->scan(txn, arena);
    if (rc._val != RC_TRUE && running) {
      db->Abort(txn);
      goto retry;
    }
    TryCatch(rc);
#endif  // BLOCKDDL
#endif  // COPYDDL
    TryCatch(db->Commit(txn));
#endif  // DDL
    return {RC_TRUE};
  }

  rc_t txn_read() {
#ifdef LAZYDDL
    ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
#else
    ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_READ_ONLY, *arena, txn_buf());
#endif

    for (uint i = 0; i < oddlb_reps_per_tx; ++i) {
      uint64_t schema_version = 0;
      ermia::schema_record *schema_ptr = nullptr;
#ifdef DDL
      // Read schema for each record first
      struct ermia::schema_record schema_rec;
      schema_version = oddlb_read_schema(txn, schema);
      schema_ptr = &schema;

#ifdef COPYDDL
      ermia::ConcurrentMasstreeIndex *table_index = (ermia::ConcurrentMasstreeIndex *)schema.index;
#endif

#endif  // DDL

      uint64_t a = r.next() % oddlb_initial_table_size;  // 0 ~ oddlb_initial_table_size-1
      const oddlb_kv_1::key k2(a);
      ermia::varstr v2;

      rc_t rc = rc_t{RC_INVALID};
      table_index->GetRecord(txn, rc, Encode(str(Size(k2)), k2), v2, nullptr, schema_ptr);

      if (unlikely(rc._val == RC_FALSE)) {
        // May happen when enable_late_scan_join is set to true,
        // some new tuple have not been filled with values
        rc._val = RC_ABORT_USER;
      }

      oddlb_kv_1::value record_temp;
      const oddlb_kv_1::value *record_test = Decode(v2, record_temp);

#ifdef DDL
      LOG_IF(FATAL,
             schema.ddl_type != ermia::ddl::ddl_type::VERIFICATION_ONLY &&
                 record_test->o_value_version != schema_version)
          << "Read: It should get " << schema_version << " ,but get "
          << record_test->o_value_version;
#endif

      if (schema_version == 0) {
        oddlb_kv_1::value record1_temp;
        const oddlb_kv_1::value *record1_test = Decode(v2, record1_temp);

        ALWAYS_ASSERT(record1_test->o_value_a == a);
        ALWAYS_ASSERT(record1_test->o_value_b == a || record1_test->o_value_b == 20000000);
      } else {
#ifdef DDL
        switch (schema.ddl_type) {
          case ermia::ddl::ddl_type::COPY_VERIFICATION:
          case ermia::ddl::ddl_type::COPY_ONLY:
          {
            oddlb_kv_2::value record2_temp;
            const oddlb_kv_2::value *record2_test = Decode(v2, record2_temp);

            ALWAYS_ASSERT(record2_test->o_value_a == a);
            ALWAYS_ASSERT(record2_test->o_value_b == schema_version);
            ALWAYS_ASSERT(record2_test->o_value_c == schema_version);
            break;
          }
          case ermia::ddl::ddl_type::VERIFICATION_ONLY:
          {
            oddlb_kv_1::value record1_temp;
            const oddlb_kv_1::value *record1_test = Decode(v2, record1_temp);

            ALWAYS_ASSERT(record1_test->o_value_a == a);
            ALWAYS_ASSERT(record1_test->o_value_b == a || record1_test->o_value_b == 20000000);
            break;
          }
          case ermia::ddl::ddl_type::NO_COPY_VERIFICATION:
          {
            oddlb_kv_6::value record6_temp;
            const oddlb_kv_6::value *record2_test = Decode(v2, record6_temp);

            ALWAYS_ASSERT(record2_test->o_value_a == a);
            ALWAYS_ASSERT(record2_test->o_value_b == schema_version);
            ALWAYS_ASSERT(record2_test->o_value_c == schema_version);

            switch (schema.v) {
              case 2:
                ALWAYS_ASSERT(record2_test->o_value_d == schema_version);
                break;
              case 3:
                ALWAYS_ASSERT(record2_test->o_value_e == schema_version);
                break;
              case 4:
                ALWAYS_ASSERT(record2_test->o_value_f == schema_version);
                break;
              case 5:
                ALWAYS_ASSERT(record2_test->o_value_g == schema_version);
                break;
              defautl:
                LOG(FATAL);
            }
            break;
          }
          default:
            LOG(FATAL);
        }
#else
        LOG(FATAL) << "No other schema version possible without DDL support";
#endif  // DDL
      }
    }

    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

  rc_t txn_rmw() {
    ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());

    for (uint i = 0; i < oddlb_reps_per_tx; ++i) {
      uint64_t schema_version = 0;
      ermia::schema_record *schema_ptr = nullptr;
#ifdef DDL
      // Read schema for each record first
      struct ermia::schema_record schema;
      schema_version = oddlb_read_schema(txn, schema);
      schema_ptr = &schema;
#endif

#ifdef COPYDDL
      ermia::ConcurrentMasstreeIndex *table_index = (ermia::ConcurrentMasstreeIndex *)schema.index;
#endif

      uint64_t a = r.next() % oddlb_initial_table_size;  // 0 ~ oddlb_initial_table_size-1
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
#ifdef DDL
        switch (schema.ddl_type) {
          case ermia::ddl::ddl_type::COPY_VERIFICATION:
          case ermia::ddl::ddl_type::COPY_ONLY:
          {
            oddlb_kv_2::value record2;

            record2.o_value_version = schema_version;
            record2.o_value_a = a;
            record2.o_value_b = schema_version;
            record2.o_value_c = schema_version;

            v2 = Encode(str(Size(record2)), record2);
            break;
          }
          case ermia::ddl::ddl_type::VERIFICATION_ONLY:
          {
            oddlb_kv_1::value record1;
            record1.o_value_version = schema_version;
            record1.o_value_a = a;
            record1.o_value_b = a;

            v2 = Encode(str(Size(record1)), record1);
            break;
          }
          case ermia::ddl::ddl_type::NO_COPY_VERIFICATION:
            v2 = GenerateValue(a, &schema);
            break;
          default:
            LOG(FATAL);
        }
#else
      LOG(FATAL) << "No other schema version possible without DDL support";
#endif  // DDL
      }

      const oddlb_kv_1::key k1(a);
      TryCatch(table_index->UpdateRecord(txn, Encode(str(Size(k1)), k1), v2, schema_ptr));
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
