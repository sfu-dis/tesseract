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

  void no_copy_verification_op(ermia::Schema_record *schema,
                               ermia::varstr &value, ermia::str_arena *arena) {
    uint64_t latest_version = schema->v;
    uint64_t current_version = 1;
    ermia::varstr *new_value;
    for (; current_version <= latest_version; current_version++) {
      new_value = ermia::ddl::reformats[schema->reformats[current_version - 1]](
          nullptr, value, arena, current_version, -1, -1);
    }

    ermia::Schema6 *record2_test = (ermia::Schema6 *)new_value->data();

    ALWAYS_ASSERT(record2_test->b == latest_version);
    ALWAYS_ASSERT(record2_test->c == latest_version);
    ALWAYS_ASSERT(record2_test->d == latest_version);
    ALWAYS_ASSERT(record2_test->e == latest_version);
    ALWAYS_ASSERT(record2_test->f == latest_version);
    ALWAYS_ASSERT(record2_test->g == latest_version);
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
    struct ermia::Schema_record schema;
    ermia::Schema_record *old_schema =
        (ermia::Schema_record *)schema_value.data();

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
      schema.state = schema.ddl_type == ermia::ddl::ddl_type::VERIFICATION_ONLY
                         ? ermia::ddl::schema_state_type::NOT_READY
                         : ermia::ddl::schema_state_type::READY;
      txn->set_old_td(old_schema->td);
      txn->add_old_td_map(old_schema->td);
    }
    ermia::varstr &v2 = str(sizeof(ermia::Schema_record));
    v2.copy_from((char *)&schema, sizeof(ermia::Schema_record));

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
    struct ermia::Schema_record schema;
    memcpy(&schema, (char *)schema_value.data(), sizeof(schema));

    uint64_t schema_version = schema.v + 1;
    DLOG(INFO) << "change to new schema: " << schema_version;
    schema.v = schema_version;
    ermia::varstr &v1 = str(sizeof(ermia::Schema_record));
    v1.copy_from((char *)&schema, sizeof(ermia::Schema_record));

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
    struct ermia::Schema_record schema;
    memcpy(&schema, (char *)schema_value.data(), sizeof(schema));

    uint64_t schema_version = schema.v + 1;
    DLOG(INFO) << "change to new schema: " << schema_version;
    schema.v = schema_version;
    ermia::varstr &v = str(sizeof(ermia::Schema_record));
    v.copy_from((char *)&schema, sizeof(ermia::Schema_record));

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
    uint64_t a =
        r.next() % oddl_initial_table_size;  // 0 ~ oddl_initial_table_size-1

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

    ermia::Schema_record *schema = (ermia::Schema_record *)v1.data();
    uint64_t schema_version = schema->v;

    ermia::varstr &k2 = str(sizeof(uint64_t));
    k2.copy_from((char *)&a, sizeof(uint64_t));

    ermia::varstr v2;

    rc = rc_t{RC_INVALID};
    oid = ermia::INVALID_OID;

#ifdef COPYDDL
    ermia::ConcurrentMasstreeIndex *table_index =
        (ermia::ConcurrentMasstreeIndex *)schema->index;
#endif

    table_index->GetRecord(txn, rc, k2, v2, &oid, schema);
    if (rc._val != RC_TRUE) TryCatch(rc_t{RC_ABORT_USER});

    ermia::Schema_base *record_test = (ermia::Schema_base *)v2.data();

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
      ermia::Schema1 *record1_test = (ermia::Schema1 *)v2.data();

      ALWAYS_ASSERT(record1_test->a == a);
      ALWAYS_ASSERT(record1_test->b == a || record1_test->b == 20000000);
    } else {
      if (schema->ddl_type == ermia::ddl::ddl_type::COPY_VERIFICATION ||
          schema->ddl_type == ermia::ddl::ddl_type::COPY_ONLY) {
        ermia::Schema2 *record2_test = (ermia::Schema2 *)v2.data();

        ALWAYS_ASSERT(record2_test->a == a);
        ALWAYS_ASSERT(record2_test->b == schema_version);
        ALWAYS_ASSERT(record2_test->c == schema_version);
      } else if (schema->ddl_type == ermia::ddl::ddl_type::VERIFICATION_ONLY) {
        ermia::Schema1 *record1_test = (ermia::Schema1 *)v2.data();

        ALWAYS_ASSERT(record1_test->a == a);
        ALWAYS_ASSERT(record1_test->b == a || record1_test->b == 20000000);
      } else if (schema->ddl_type ==
                 ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
        if (record_test->v != schema_version) {
#ifdef COPYDDL
          no_copy_verification_op(schema, v2, &(txn->string_allocator()));
#endif
        } else {
          ermia::Schema6 *record2_test = (ermia::Schema6 *)v2.data();

          ALWAYS_ASSERT(record2_test->a == a);
          ALWAYS_ASSERT(record2_test->b == schema_version);
          ALWAYS_ASSERT(record2_test->c == schema_version);
          ALWAYS_ASSERT(record2_test->d == schema_version);
          ALWAYS_ASSERT(record2_test->e == schema_version);
          ALWAYS_ASSERT(record2_test->f == schema_version);
          ALWAYS_ASSERT(record2_test->g == schema_version);
        }
      }
    }

    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

  rc_t txn_rmw() {
    uint64_t a =
        r.next() % oddl_initial_table_size;  // 0 ~ oddl_initial_table_size-1

    ermia::transaction *txn =
        db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());

#ifdef SIDDL
  retry:
#endif
    ermia::varstr v;
    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = ermia::INVALID_OID;
    schema_index->ReadSchemaRecord(txn, rc, *table_key, v, &oid);
    TryCatch(rc);

    ermia::Schema_record *schema = (ermia::Schema_record *)v.data();
    uint64_t schema_version = schema->v;

#ifdef COPYDDL
    ermia::ConcurrentMasstreeIndex *table_index =
        (ermia::ConcurrentMasstreeIndex *)schema->index;
#endif

    ermia::varstr &k1 = str(sizeof(uint64_t));
    k1.copy_from((char *)&a, sizeof(uint64_t));

    ermia::varstr v1;

    rc = rc_t{RC_INVALID};
    oid = ermia::INVALID_OID;

    ermia::varstr v2;
    if (schema_version == 0) {
      struct ermia::Schema1 record1;
      record1.v = schema_version;
      record1.a = a;
      if (unlikely(ermia::cdc_test)) {
        record1.b = 20000000;
      } else {
        record1.b = a;
      }

      v2 = str(sizeof(ermia::Schema1));
      v2.copy_from((char *)&record1, sizeof(ermia::Schema1));
    } else {
      if (schema->ddl_type == ermia::ddl::ddl_type::COPY_VERIFICATION ||
          schema->ddl_type == ermia::ddl::ddl_type::COPY_ONLY) {
        struct ermia::Schema2 record2;

        record2.v = schema_version;
        record2.a = a;
        record2.b = schema_version;
        record2.c = schema_version;

        v2 = str(sizeof(ermia::Schema2));
        v2.copy_from((char *)&record2, sizeof(ermia::Schema2));
      } else if (schema->ddl_type == ermia::ddl::ddl_type::VERIFICATION_ONLY) {
        struct ermia::Schema1 record1;
        record1.v = schema_version;
        record1.a = a;
        record1.b = a;

        v2 = str(sizeof(ermia::Schema1));
        v2.copy_from((char *)&record1, sizeof(ermia::Schema1));
      } else if (schema->ddl_type ==
                 ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
        struct ermia::Schema6 record2;

        record2.v = schema_version;
        record2.a = a;
        record2.b = schema_version;
        record2.c = schema_version;
        record2.d = schema_version;
        record2.e = schema_version;
        record2.f = schema_version;
        record2.g = schema_version;

        v2 = str(sizeof(ermia::Schema6));
        v2.copy_from((char *)&record2, sizeof(ermia::Schema6));
      }
    }

    TryCatch(table_index->UpdateRecord(txn, k1, v2, schema));

    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }
};

void oddlb_do_test(ermia::Engine *db, int argc, char **argv) {
  oddlb_parse_options(argc, argv);
  oddlb_bench_runner<oddlb_sequential_worker> r(db);
  r.run();
}
