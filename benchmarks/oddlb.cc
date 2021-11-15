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
    // std::cerr << "Read/Write = " << read_ratio << "/" << write_ratio
    //           << std::endl;
  }

  double read_ratio = 0.2, write_ratio = 0.7999985;

  virtual workload_desc_vec get_workload() const {
    workload_desc_vec w;

    // w.push_back(workload_desc("DDL", 0.0000015, TxnDDL));
    // w.push_back(workload_desc("Read", 0.9499985, TxnRead));
    // w.push_back(workload_desc("RMW", 0.05, TxnRMW));

    w.push_back(workload_desc("Read", read_ratio, TxnRead));
    w.push_back(workload_desc("RMW", write_ratio, TxnRMW));
    w.push_back(workload_desc("DDL", 0.0000015, TxnDDL));

    return w;
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

  static bool constraint_verification(uint64_t x) { return x < 0; }

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
    std::cerr << "Change to a new schema, version: " << schema_version
              << std::endl;
    schema.v = schema_version;

    rc = rc_t{RC_INVALID};

    std::stringstream ss;
    ss << schema_version;

    std::string str3 = std::string(str1);
    str3 += ss.str();

    db->CreateTable(str3.c_str());
#ifdef LAZYDDL
    // For create index DDL
    db->CreateMasstreePrimaryIndex(str3.c_str(), str3);
    schema.index = ermia::Catalog::GetTable(str3.c_str())->GetPrimaryIndex();
#else
    ermia::Catalog::GetTable(str3.c_str())
        ->SetPrimaryIndex(old_table_index, std::string(str1));
    schema.index = ermia::Catalog::GetTable(str1)->GetPrimaryIndex();
#endif
    schema.td = ermia::Catalog::GetTable(str3.c_str());
    schema.state = 0;
#ifdef LAZYDDL
    schema.old_index = old_table_index;
    schema.old_td = old_td;
    schema.old_tds[old_schema_version] = old_td;
#elif DCOPYDDL
    schema.old_td = old_td;
    schema.state = 1;
#else
    schema.old_td = old_td;
    schema.state = 2;
#endif
    memcpy(str2, &schema, sizeof(str2));
    ermia::varstr &v2 = str(sizeof(str2));
    v2.copy_from(str2, sizeof(str2));

    txn->set_table_descriptors(schema.td, old_td);

    rc = rc_t{RC_INVALID};
    schema_index->WriteSchemaTable(txn, rc, k1, v2);
    TryCatch(rc);

    // New a ddl executor
    ermia::ddl::ddl_executor *ddl_exe = new ermia::ddl::ddl_executor(
        schema.v, schema.old_v, schema.reformat_idx, schema.td, schema.old_td,
        schema.index, schema.state);
    txn->set_ddl_executor(ddl_exe);

#if !defined(LAZYDDL)
    ermia::ConcurrentMasstreeIndex *table_index =
        (ermia::ConcurrentMasstreeIndex *)schema.index;
    rc = rc_t{RC_INVALID};
    rc = ddl_exe->scan_copy(txn, arena, v2);
    TryCatch(rc);

#ifdef DCOPYDDL
    schema.state = 0;
    memcpy(str2, &schema, sizeof(str2));
    v2 = Encode_(str(sizeof(str2)), str2);

    schema_index->WriteSchemaTable(txn, rc, k1, v2);
    TryCatch(rc);
#endif
#endif

    TryCatch(db->Commit(txn));
#elif defined(BLOCKDDL)
    db->WriteLock(std::string("SCHEMA"));
    ermia::transaction *txn =
        db->NewTransaction(ermia::transaction::TXN_FLAG_DDL, *arena, txn_buf());

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
    std::cerr << "change to new schema: " << schema_version << std::endl;
    schema.v = schema_version;
    memcpy(str2, &schema, sizeof(str2));
    ermia::varstr &v1 = str(sizeof(str2));
    v1.copy_from(str2, sizeof(str2));

    rc = rc_t{RC_INVALID};
    rc = schema_index->WriteSchemaTable(txn, rc, k, v1);
    TryCatchUnblock(rc);

    // New a ddl executor
    ermia::ddl::ddl_executor *ddl_exe = new ermia::ddl::ddl_executor(
        schema.v, -1, schema.reformat_idx, nullptr, nullptr, nullptr, -1);

    rc = rc_t{RC_INVALID};
    rc = ddl_exe->scan_copy(txn, arena, v1);
    TryCatchUnblock(rc);

    TryCatchUnblock(db->Commit(txn));
    db->WriteUnlock(std::string("SCHEMA"));
#elif SIDDL
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
    std::cerr << "Change to a new schema, version: " << schema_version
              << std::endl;
    schema.v = schema_version;
    memcpy(str2, &schema, sizeof(str2));
    ermia::varstr &v = str(sizeof(str2));
    v.copy_from(str2, sizeof(str2));

    rc = rc_t{RC_INVALID};
    rc = schema_index->WriteSchemaTable(txn, rc, k, v);
    TryCatch(rc);

    // New a ddl executor
    ermia::ddl::ddl_executor *ddl_exe = new ermia::ddl::ddl_executor(
        schema.v, -1, schema.reformat_idx, nullptr, nullptr, nullptr, -1);

    rc = rc_t{RC_INVALID};
    rc = ddl_exe->scan_copy(txn, arena, v);
    if (rc._val != RC_TRUE) {
      count++;
      db->Abort(txn);
      goto retry;
    }

    TryCatch(db->Commit(txn));
    printf("%d attempts\n", count);
#else
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
    std::cerr << "Change to a new schema, version: " << schema_version
              << std::endl;
    schema.v = schema_version;
    memcpy(str2, &schema, sizeof(str2));
    ermia::varstr &v = str(sizeof(str2));
    v.copy_from(str2, sizeof(str2));

    rc = rc_t{RC_INVALID};
    rc = schema_index->WriteSchemaTable(txn, rc, k, v);
    TryCatch(rc);

    TryCatch(db->Commit(txn));
#endif
    printf("DDL commit OK\n");
    return {RC_TRUE};
  }

  rc_t txn_read() {
#ifdef BLOCKDDL
    db->ReadLock(std::string("SCHEMA"));
#endif
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

#ifdef LAZYDDL
    if (schema_version == 0) {
      table_index->GetRecord(txn, rc, k2, v2, &oid);
    } else {
      table_index->GetRecord(txn, rc, k2, v2, &oid);
      if (rc._val != RC_TRUE) {
        ALWAYS_ASSERT(schema.old_td != nullptr);
        table_index->GetRecord(txn, rc, k2, v2, &oid, schema.old_td,
                               schema.old_tds, schema_version);
      }
    }
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

#if !defined(NONEDDL)
    if (record_test.v != schema_version) {
      LOG(FATAL) << "Read: It should get " << schema_version << " ,but get "
                 << record_test.v;
    }
#endif

    if (schema_version == 0) {
      struct ermia::Schema1 record1_test;
      memcpy(&record1_test, (char *)v2.data(), sizeof(record1_test));

      ALWAYS_ASSERT(record1_test.a == a);
      ALWAYS_ASSERT(record1_test.b == a);
    } else {
      struct ermia::Schema2 record2_test;
#ifdef NONEDDL
      struct ermia::Schema1 record1_test;
      memcpy(&record1_test, (char *)v2.data(), sizeof(record1_test));

      record2_test.a = a;
      record2_test.b = schema_version;
      record2_test.c = schema_version;
#else
      memcpy(&record2_test, (char *)v2.data(), sizeof(record2_test));
#endif

      if (record2_test.a != a)
        printf("here 1, real a: %lu, a: %lu, b: %lu, c: %lu\n", a,
               record2_test.a, record2_test.b, record2_test.c);
      ALWAYS_ASSERT(record2_test.a == a);
      ALWAYS_ASSERT(record2_test.b == schema_version);
      ALWAYS_ASSERT(record2_test.c == schema_version);
    }

    TryCatch(db->Commit(txn));
#ifdef BLOCKDDL
    db->ReadUnlock(std::string("SCHEMA"));
#endif
    return {RC_TRUE};
  }

  rc_t txn_rmw() {
#ifdef BLOCKDDL
    db->ReadLock(std::string("SCHEMA"));
#endif
    uint64_t a =
        r.next() % oddl_initial_table_size; // 0 ~ oddl_initial_table_size-1

    ermia::transaction *txn =
        db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());

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

    /*table_index->GetRecord(txn, rc, k1, v1, &oid);
    TryVerifyRelaxed(rc);

    struct ermia::Schema_base record_test;
    memcpy(&record_test, (char *)v1.data(), sizeof(record_test));

#ifdef SIDDL
    if (record_test.v != schema_version) {
      goto retry;
    }
#endif

#if !defined(NONEDDL)
    if (schema_version != record_test.v) {
      LOG(FATAL) << "Write: It should get " << schema_version << " ,but get " <<
record_test.v;
    }
#endif*/

    if (schema_version == 0) {
      struct ermia::Schema1 record1;
      record1.v = schema_version;
      record1.a = a;
      record1.b = a;

      char str2[sizeof(ermia::Schema1)];
      memcpy(str2, &record1, sizeof(str2));
      ermia::varstr &v2 = str(sizeof(str2));
      v2.copy_from(str2, sizeof(str2));

      TryCatch(table_index->UpdateRecord(txn, k1, v2));
    } else {
      struct ermia::Schema2 record2;

      record2.v = schema_version;
      record2.a = a;
      record2.b = schema_version;
      record2.c = schema_version;

      char str2[sizeof(ermia::Schema2)];
      memcpy(str2, &record2, sizeof(str2));
      ermia::varstr &v2 = str(sizeof(str2));
      v2.copy_from(str2, sizeof(str2));

#ifdef LAZYDDL
      if (table_index->UpdateRecord(txn, k1, v2)._val != RC_TRUE) {
        ALWAYS_ASSERT(schema.old_td != nullptr);
        TryCatch(table_index->UpdateRecord(txn, k1, v2, schema.old_td,
                                           schema.old_tds, schema_version));
      }
#else
      TryCatch(table_index->UpdateRecord(txn, k1, v2));
#endif
    }

    TryCatch(db->Commit(txn));
#ifdef BLOCKDDL
    db->ReadUnlock(std::string("SCHEMA"));
#endif
    return {RC_TRUE};
  }
};

void oddlb_do_test(ermia::Engine *db, int argc, char **argv) {
  std::cerr << RAND_MAX << std::endl;
  oddlb_parse_options(argc, argv);
  oddlb_bench_runner<oddlb_sequential_worker> r(db);
  r.run();
}
