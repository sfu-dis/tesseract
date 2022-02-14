/*
 * An Online DDL Benchmark.
 */
#include "oddlb.h"
#include "bench.h"
#include <sstream>

class oddlb_sequential_worker : public oddlb_base_worker {
public:
  oddlb_sequential_worker(
      unsigned int worker_id, unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      spin_barrier *barrier_a, spin_barrier *barrier_b)
      : oddlb_base_worker(worker_id, seed, db, open_tables, barrier_a,
                          barrier_b) {
  }

  double read_ratio = 0.2, write_ratio = 0.8;

  virtual workload_desc_vec get_workload() const {
    workload_desc_vec w;

    if (read_ratio)
      w.push_back(workload_desc("Read", read_ratio, TxnRead));
    if (write_ratio)
      w.push_back(workload_desc("RMW", write_ratio, TxnRMW));

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

    struct ermia::Schema6 record2_test;
    memcpy(&record2_test, (char *)new_value->data(), sizeof(record2_test));

    ALWAYS_ASSERT(record2_test.b == latest_version);
    ALWAYS_ASSERT(record2_test.c == latest_version);
    ALWAYS_ASSERT(record2_test.d == latest_version);
    ALWAYS_ASSERT(record2_test.e == latest_version);
    ALWAYS_ASSERT(record2_test.f == latest_version);
    ALWAYS_ASSERT(record2_test.g == latest_version);
  }

  rc_t txn_ddl() {
#ifdef COPYDDL
    ermia::transaction *txn =
        db->NewTransaction(ermia::transaction::TXN_FLAG_DDL, *arena, txn_buf());

    char str1[] = "USERTABLE", str2[sizeof(ermia::Schema_record)];
    ermia::varstr &k1 = str(sizeof(str1));
    k1.copy_from(str1, sizeof(str1));

    ermia::varstr v1;
    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = ermia::INVALID_OID;
    schema_index->ReadSchemaTable(txn, rc, k1, v1, &oid);
    TryVerifyRelaxed(rc);

    struct ermia::Schema_record schema;
    memcpy(&schema, (char *)v1.data(), sizeof(schema));

    uint64_t old_schema_version = schema.v;
    ermia::ConcurrentMasstreeIndex *old_table_index =
        (ermia::ConcurrentMasstreeIndex *)schema.index;
    ermia::TableDescriptor *old_td = schema.td;

    uint64_t schema_version = old_schema_version + 1;
    DLOG(INFO) << "Change to a new schema, version: " << schema_version;
    schema.v = schema_version;
    schema.old_v = old_schema_version;
    schema.old_td = old_td;
    schema.state = ermia::ddl::schema_state_type::READY;
    schema.ddl_type = ermia::ddl::ddl_type_map(ermia::config::ddl_type);

    rc = rc_t{RC_INVALID};

    if (ermia::config::ddl_type == 1 || ermia::config::ddl_type == 3) {
      std::stringstream ss;
      ss << schema_version;

      std::string str3 = std::string(str1);
      str3 += ss.str();

      db->CreateTable(str3.c_str());

      schema.td = ermia::Catalog::GetTable(str3.c_str());
      schema.state = ermia::ddl::schema_state_type::NOT_READY;
#ifdef LAZYDDL
      schema.old_index = old_table_index;
      schema.old_tds[old_schema_version] = old_td;
#endif

#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
      auto *new_table_index =
          new ermia::ConcurrentMasstreeIndex(str3.c_str(), true);
      new_table_index->SetArrays(true);
      schema.td->SetPrimaryIndex(new_table_index);
      schema.index = new_table_index;
#else
      ermia::Catalog::GetTable(str3.c_str())
          ->SetPrimaryIndex(old_table_index, std::string(str1));
      schema.index = ermia::Catalog::GetTable(str3.c_str())->GetPrimaryIndex();
      ALWAYS_ASSERT(old_table_index == schema.index);
#endif

      txn->set_old_td(old_td);
      txn->add_new_td_map(schema.td);
      txn->add_old_td_map(old_td);
    } else {
      if (ermia::config::ddl_type == 4) {
        schema_version = old_schema_version +
                         ermia::config::no_copy_verification_version_add;
        schema.v = schema_version;
      }
      schema.state = ermia::config::ddl_type == 2 ? 2 : 0;
      txn->set_old_td(old_td);
      txn->add_old_td_map(old_td);
    }
    memcpy(str2, &schema, sizeof(str2));
    ermia::varstr &v2 = str(sizeof(str2));
    v2.copy_from(str2, sizeof(str2));

    rc = rc_t{RC_INVALID};
    schema_index->WriteSchemaTable(txn, rc, k1, v2);
    TryCatch(rc);

    // New a ddl executor
    ermia::ddl::ddl_executor *ddl_exe = new ermia::ddl::ddl_executor();
    ddl_exe->add_ddl_executor_paras(schema.v, schema.old_v, schema.ddl_type,
                                    schema.reformat_idx, schema.constraint_idx,
                                    schema.td, schema.old_td, schema.index,
                                    schema.state);
    txn->set_ddl_executor(ddl_exe);

    if (ermia::config::ddl_type != 4) {
#if !defined(LAZYDDL)
      rc = rc_t{RC_INVALID};
      rc = ddl_exe->scan(txn, arena, v2);
      TryCatch(rc);
#endif
    }

    TryCatch(db->Commit(txn));
#elif defined(BLOCKDDL)
    ermia::transaction *txn =
        db->NewTransaction(ermia::transaction::TXN_FLAG_DDL, *arena, txn_buf());
    txn->register_locked_tables(schema_fid,
                                ermia::transaction::lock_type::EXCLUSIVE);

    char str1[] = "USERTABLE", str2[sizeof(ermia::Schema_base)];
    ermia::varstr &k = str(sizeof(str1));
    k.copy_from(str1, sizeof(str1));
    ermia::varstr v;

    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = ermia::INVALID_OID;
    schema_index->ReadSchemaTable(txn, rc, k, v, &oid);
    TryVerifyRelaxed(rc);

    struct ermia::Schema_base schema;
    memcpy(&schema, (char *)v.data(), sizeof(schema));

    uint64_t schema_version = schema.v + 1;
    DLOG(INFO) << "change to new schema: " << schema_version;
    schema.v = schema_version;
    memcpy(str2, &schema, sizeof(str2));
    ermia::varstr &v1 = str(sizeof(str2));
    v1.copy_from(str2, sizeof(str2));

    TryCatch(schema_index->WriteSchemaTable(txn, rc, k, v1));

    // New a ddl executor
    ermia::ddl::ddl_executor *ddl_exe = new ermia::ddl::ddl_executor();
    ddl_exe->add_ddl_executor_paras(schema.v, -1, schema.ddl_type,
                                    schema.reformat_idx, schema.constraint_idx,
                                    schema.td, schema.td, schema.index, -1);

    txn->set_old_td(schema.td);
    txn->add_old_td_map(schema.td);
    txn->add_new_td_map(schema.td);

    TryCatch(ddl_exe->scan(txn, arena, v1));

    TryCatch(db->Commit(txn));
#elif SIDDL
    DLOG(INFO) << "SI DDL begins";
    int count = 0;
  retry:
    ermia::transaction *txn =
        db->NewTransaction(ermia::transaction::TXN_FLAG_DDL, *arena, txn_buf());

    char str1[] = "USERTABLE", str2[sizeof(ermia::Schema_base)];
    ermia::varstr &k = str(sizeof(str1));
    k.copy_from(str1, sizeof(str1));

    ermia::varstr v1;
    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = ermia::INVALID_OID;
    schema_index->ReadSchemaTable(txn, rc, k, v1, &oid);
    TryVerifyRelaxed(rc);

    struct ermia::Schema_base schema;
    memcpy(&schema, (char *)v1.data(), sizeof(schema));

    uint64_t schema_version = schema.v + 1;
    DLOG(INFO) << "change to new schema: " << schema_version;
    schema.v = schema_version;
    memcpy(str2, &schema, sizeof(str2));
    ermia::varstr &v = str(sizeof(str2));
    v.copy_from(str2, sizeof(str2));

    txn->set_old_td(schema.td);

    rc = rc_t{RC_INVALID};
    rc = schema_index->WriteSchemaTable(txn, rc, k, v);
    TryCatch(rc);

    // New a ddl executor
    ermia::ddl::ddl_executor *ddl_exe = new ermia::ddl::ddl_executor();
    ddl_exe->add_ddl_executor_paras(schema.v, -1, schema.ddl_type,
                                    schema.reformat_idx, schema.constraint_idx,
                                    schema.td, schema.td, schema.index, -1);

    rc = rc_t{RC_INVALID};
    rc = ddl_exe->scan(txn, arena, v);
    if (rc._val != RC_TRUE) {
      std::cerr << "SI DDL aborts" << std::endl;
      count++;
      db->Abort(txn);
      goto retry;
    }
    TryCatch(rc);

    TryCatch(db->Commit(txn));
    DLOG(INFO) << count << " attempts";
#endif
    DLOG(INFO) << "DDL commit OK";
    return {RC_TRUE};
  }

  rc_t txn_read() {
    uint64_t a =
        r.next() % oddl_initial_table_size; // 0 ~ oddl_initial_table_size-1

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
#ifdef BLOCKDDL
    txn->register_locked_tables(schema_fid,
                                ermia::transaction::lock_type::SHARED);
#endif

    char str1[] = "USERTABLE";
    ermia::varstr &k1 = str(sizeof(str1));
    k1.copy_from(str1, sizeof(str1));
    ermia::varstr v1;

    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = ermia::INVALID_OID;
    schema_index->ReadSchemaTable(txn, rc, k1, v1, &oid);
    TryCatch(rc);

#ifdef COPYDDL
    struct ermia::Schema_record schema;
#else
    struct ermia::Schema_base schema;
#endif
    memcpy(&schema, (char *)v1.data(), sizeof(schema));
    uint64_t schema_version = schema.v;

    char str2[sizeof(uint64_t)];
    memcpy(str2, &a, sizeof(str2));
    ermia::varstr &k2 = str(sizeof(uint64_t));
    k2.copy_from(str2, sizeof(str2));

    ermia::varstr v2;

    rc = rc_t{RC_INVALID};
    oid = ermia::INVALID_OID;

#ifdef COPYDDL
    ermia::ConcurrentMasstreeIndex *table_index =
        (ermia::ConcurrentMasstreeIndex *)schema.index;
#endif

#ifdef COPYDDL
    table_index->GetRecord(txn, rc, k2, v2, &oid, &schema);
#else
    table_index->GetRecord(txn, rc, k2, v2, &oid);
#endif
    if (rc._val != RC_TRUE)
      TryCatch(rc_t{RC_ABORT_USER});

    struct ermia::Schema_base record_test;
    memcpy(&record_test, (char *)v2.data(), sizeof(record_test));

#ifdef SIDDL
    if (record_test.v != schema_version) {
      TryCatch(rc_t{RC_ABORT_USER});
    }
#endif

    if (schema.ddl_type != ermia::ddl::ddl_type::NO_COPY_VERIFICATION &&
        schema.ddl_type != ermia::ddl::ddl_type::VERIFICATION_ONLY &&
        record_test.v != schema_version) {
#ifdef BLOCKDDL
      TryCatch(rc_t{RC_ABORT_USER});
#else
      LOG(FATAL) << "Read: It should get " << schema_version << " ,but get "
                 << record_test.v;
#endif
    }

    if (schema_version == 0) {
      struct ermia::Schema1 record1_test;
      memcpy(&record1_test, (char *)v2.data(), sizeof(record1_test));

      ALWAYS_ASSERT(record1_test.a == a);
      ALWAYS_ASSERT(record1_test.b == a || record1_test.b == 20000000);
    } else {
      if (schema.ddl_type == ermia::ddl::ddl_type::COPY_VERIFICATION ||
          schema.ddl_type == ermia::ddl::ddl_type::COPY_ONLY) {
        struct ermia::Schema2 record2_test;
        memcpy(&record2_test, (char *)v2.data(), sizeof(record2_test));

        ALWAYS_ASSERT(record2_test.a == a);
        ALWAYS_ASSERT(record2_test.b == schema_version);
        ALWAYS_ASSERT(record2_test.c == schema_version);
      } else if (schema.ddl_type == ermia::ddl::ddl_type::VERIFICATION_ONLY) {
        struct ermia::Schema1 record1_test;
        memcpy(&record1_test, (char *)v2.data(), sizeof(record1_test));

        ALWAYS_ASSERT(record1_test.a == a);
        ALWAYS_ASSERT(record1_test.b == a || record1_test.b == 20000000);
      } else if (schema.ddl_type ==
                 ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
        if (record_test.v != schema_version) {
#ifdef COPYDDL
          no_copy_verification_op(&schema, v2, &(txn->string_allocator()));
#endif
        } else {
          struct ermia::Schema6 record2_test;
          memcpy(&record2_test, (char *)v2.data(), sizeof(record2_test));

          ALWAYS_ASSERT(record2_test.a == a);
          ALWAYS_ASSERT(record2_test.b == schema_version);
          ALWAYS_ASSERT(record2_test.c == schema_version);
          ALWAYS_ASSERT(record2_test.d == schema_version);
          ALWAYS_ASSERT(record2_test.e == schema_version);
          ALWAYS_ASSERT(record2_test.f == schema_version);
          ALWAYS_ASSERT(record2_test.g == schema_version);
        }
      }
    }

    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

  rc_t txn_rmw() {
    uint64_t a =
        r.next() % oddl_initial_table_size; // 0 ~ oddl_initial_table_size-1

    ermia::transaction *txn =
        db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
#ifdef BLOCKDDL
    txn->register_locked_tables(schema_fid,
                                ermia::transaction::lock_type::SHARED);
#endif

#ifdef SIDDL
  retry:
#endif
    char str0[] = "USERTABLE";
    ermia::varstr &k = str(sizeof(str0));
    k.copy_from(str0, sizeof(str0));
    ermia::varstr v;

    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = ermia::INVALID_OID;
    schema_index->ReadSchemaTable(txn, rc, k, v, &oid);
    TryCatch(rc);

#ifdef COPYDDL
    struct ermia::Schema_record schema;
#else
    struct ermia::Schema_base schema;
#endif
    memcpy(&schema, (char *)v.data(), sizeof(schema));
    uint64_t schema_version = schema.v;

#ifdef COPYDDL
    ermia::ConcurrentMasstreeIndex *table_index =
        (ermia::ConcurrentMasstreeIndex *)schema.index;
#endif

    char str1[sizeof(uint64_t)];
    memcpy(str1, &a, sizeof(str1));
    ermia::varstr &k1 = str(sizeof(uint64_t));
    k1.copy_from(str1, sizeof(str1));

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

      char str2[sizeof(ermia::Schema1)];
      memcpy(str2, &record1, sizeof(str2));
      v2 = str(sizeof(str2));
      v2.copy_from(str2, sizeof(str2));
    } else {
      if (schema.ddl_type == ermia::ddl::ddl_type::COPY_VERIFICATION ||
          schema.ddl_type == ermia::ddl::ddl_type::COPY_ONLY) {
        struct ermia::Schema2 record2;

        record2.v = schema_version;
        record2.a = a;
        record2.b = schema_version;
        record2.c = schema_version;

        char str2[sizeof(ermia::Schema2)];
        memcpy(str2, &record2, sizeof(str2));
        v2 = str(sizeof(str2));
        v2.copy_from(str2, sizeof(str2));
      } else if (schema.ddl_type == ermia::ddl::ddl_type::VERIFICATION_ONLY) {
        struct ermia::Schema1 record1;
        record1.v = schema_version;
        record1.a = a;
        record1.b = a;

        char str2[sizeof(ermia::Schema1)];
        memcpy(str2, &record1, sizeof(str2));
        v2 = str(sizeof(str2));
        v2.copy_from(str2, sizeof(str2));
      } else if (schema.ddl_type ==
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

        char str2[sizeof(ermia::Schema6)];
        memcpy(str2, &record2, sizeof(str2));
        v2 = str(sizeof(str2));
        v2.copy_from(str2, sizeof(str2));
      }
    }

#ifdef COPYDDL
    TryCatch(table_index->UpdateRecord(txn, k1, v2, &schema));
#else
    TryCatch(table_index->UpdateRecord(txn, k1, v2));
#endif

    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }
};

void oddlb_do_test(ermia::Engine *db, int argc, char **argv) {
  oddlb_parse_options(argc, argv);
  oddlb_bench_runner<oddlb_sequential_worker> r(db);
  r.run();
}
