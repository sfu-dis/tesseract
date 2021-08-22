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
                          barrier_b) {}

  virtual workload_desc_vec get_workload() const {
    workload_desc_vec w;

    // w.push_back(workload_desc("DDL", 0.0000015, TxnDDL));
    // w.push_back(workload_desc("Read", 0.9499985, TxnRead));
    // w.push_back(workload_desc("RMW", 0.05, TxnRMW));

    w.push_back(workload_desc("Read", 0.6999985, TxnRead));
    w.push_back(workload_desc("RMW", 0.3, TxnRMW));
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

    char str1[] = "USERTABLE", str2[sizeof(Schema_record)];
    ermia::varstr &k1 = str(sizeof(str1));
    k1.copy_from(str1, sizeof(str1));

    ermia::varstr v1;
    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = ermia::INVALID_OID;
    schema_index->ReadSchemaTable(txn, rc, k1, v1, &oid);
    TryVerifyRelaxed(rc);

    struct Schema_record schema;
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

    if (!db->BuildIndexMap(str3.c_str())) {
      // printf("Duplicate table creation\n");
      db->Abort(txn);
      return {RC_ABORT_USER};
    }

    db->WriteLock(str3.c_str());

    db->CreateTable(str3.c_str());
    db->CreateMasstreePrimaryIndex(str3.c_str(), str3);

    // std::cerr << "Create a new table: " << str3 << std::endl;

    schema.index = ermia::Catalog::GetTable(str3.c_str())->GetPrimaryIndex();
    schema.td = ermia::Catalog::GetTable(str3.c_str());
#ifdef LAZYDDL
    schema.old_index = old_table_index;
    schema.old_td = old_td;
    schema.old_tds[old_schema_version] = old_td;
    /*if (schema_version == 1) schema.old_tds[0] = old_td;
    if (schema_version == 2) schema.old_tds[1] = old_td;
    if (schema_version == 2) ALWAYS_ASSERT(schema.old_tds[0] != nullptr);
    */
#endif
    memcpy(str2, &schema, sizeof(str2));
    ermia::varstr &v2 = str(sizeof(str2));
    v2.copy_from(str2, sizeof(str2));

    db->WriteUnlock(str3.c_str());

    txn->set_table_descriptors(schema.td, old_td);

    rc = rc_t{RC_INVALID};
    schema_index->WriteSchemaTable(txn, rc, k1, v2);
    TryCatch(rc);

#if !defined(LAZYDDL)
    ermia::ConcurrentMasstreeIndex *table_index =
        (ermia::ConcurrentMasstreeIndex *)schema.index;
    rc = rc_t{RC_INVALID};
    rc = table_index->WriteNormalTable(arena, old_table_index, txn, v2, NULL);
    TryCatch(rc);
#endif

    TryCatch(db->Commit(txn));
#elif defined(BLOCKDDL)
    // CRITICAL_SECTION(cs, lock);
    db->WriteLock(std::string("SCHEMA"));
    // db->WriteLock(std::string("USERTABLE"));
    ermia::transaction *txn =
        db->NewTransaction(ermia::transaction::TXN_FLAG_DDL, *arena, txn_buf());

    char str1[] = "USERTABLE", str2[sizeof(Schema_base)];
    ermia::varstr &k = str(sizeof(str1));
    k.copy_from(str1, sizeof(str1));
    ermia::varstr v;

    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = ermia::INVALID_OID;
    schema_index->ReadSchemaTable(txn, rc, k, v, &oid);
    TryVerifyRelaxed(rc);

    struct Schema_base schema;
    memcpy(&schema, (char *)v.data(), sizeof(schema));

    uint64_t schema_version = schema.v + 1;
    std::cerr << "change to new schema: " << schema_version << std::endl;
    schema.v = schema_version;
    // schema.op = constraint_verification;
    memcpy(str2, &schema, sizeof(str2));
    ermia::varstr &v1 = str(sizeof(str2));
    v1.copy_from(str2, sizeof(str2));

    rc = rc_t{RC_INVALID};
    rc = schema_index->WriteSchemaTable(txn, rc, k, v1);
    if (rc._val != RC_TRUE)
      TryCatchUnblock(rc);

    rc = rc_t{RC_INVALID};
    rc = table_index->WriteNormalTable(arena, table_index, txn, v1, NULL);
    // rc = table_index->CheckNormalTable(arena, table_index, txn, NULL);
    if (rc._val != RC_TRUE)
      TryCatchUnblock(rc);

    rc = rc_t{RC_INVALID};
    rc = db->Commit(txn);
    if (rc._val != RC_TRUE)
      TryCatchUnblock(rc);
    // db->WriteUnlock(std::string("USERTABLE"));
    db->WriteUnlock(std::string("SCHEMA"));
#elif SIDDL
    int count = 0;
  retry:
    ermia::transaction *txn =
        db->NewTransaction(ermia::transaction::TXN_FLAG_DDL, *arena, txn_buf());

    char str1[] = "USERTABLE", str2[sizeof(Schema_base)];
    ermia::varstr &k = str(sizeof(str1));
    k.copy_from(str1, sizeof(str1));

    ermia::varstr v1;
    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = ermia::INVALID_OID;
    schema_index->ReadSchemaTable(txn, rc, k, v1, &oid);
    TryVerifyRelaxed(rc);

    struct Schema_base schema;
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

    rc = rc_t{RC_INVALID};
    rc = table_index->WriteNormalTable(arena, table_index, txn, v, NULL);
    if (rc._val != RC_TRUE) {
      count++;
      db->Abort(txn);
      goto retry;
    }
    // TryCatch(rc);

    TryCatch(db->Commit(txn));
    printf("%d attempts\n", count);
#else
    ermia::transaction *txn =
        db->NewTransaction(ermia::transaction::TXN_FLAG_DDL, *arena, txn_buf());

    char str1[] = "USERTABLE", str2[sizeof(Schema_base)];
    ermia::varstr &k = str(sizeof(str1));
    k.copy_from(str1, sizeof(str1));

    ermia::varstr v1;
    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = ermia::INVALID_OID;
    schema_index->ReadSchemaTable(txn, rc, k, v1, &oid);
    TryVerifyRelaxed(rc);

    struct Schema_base schema;
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
    printf("ddl commit ok\n");
    return {RC_TRUE};
  }

  rc_t txn_read() {
#ifdef BLOCKDDL
    // CRITICAL_SECTION(cs, lock);
    db->ReadLock(std::string("SCHEMA"));
    // db->ReadLock(std::string("USERTABLE"));
#endif
    uint64_t a =
        rand() % oddl_initial_table_size; // 0 ~ oddl_initial_table_size-1

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
    struct Schema_record schema;
#else
    struct Schema_base schema;
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

    struct Schema_base record_test;
    memcpy(&record_test, (char *)v2.data(), sizeof(record_test));

#ifdef SIDDL
    if (record_test.v != schema_version) {
      TryCatch(rc_t{RC_ABORT_USER});
      // db->Abort(txn);
      // goto retry;
    }
#endif

#if !defined(NONEDDL)
    if (record_test.v != schema_version) {
      printf("Read: It should get %lu, but get %lu\n", schema_version,
             record_test.v);
      ALWAYS_ASSERT(record_test.v == schema_version);
    }
#endif

    if (schema_version == 0) {
      struct Schema1 record1_test;
      memcpy(&record1_test, (char *)v2.data(), sizeof(record1_test));

      ALWAYS_ASSERT(record1_test.a == a);
      ALWAYS_ASSERT(record1_test.b == a);

    } else {
      struct Schema2 record2_test;
#ifdef NONEDDL
      struct Schema1 record1_test;
      memcpy(&record1_test, (char *)v2.data(), sizeof(record1_test));

      record2_test.a = schema_version;
      record2_test.b = schema_version;
      record2_test.c = schema_version;
#else
      memcpy(&record2_test, (char *)v2.data(), sizeof(record2_test));
#endif

      ALWAYS_ASSERT(record2_test.a == schema_version);
      ALWAYS_ASSERT(record2_test.b == schema_version);
      ALWAYS_ASSERT(record2_test.c == schema_version);
    }

    TryCatch(db->Commit(txn));
#ifdef BLOCKDDL
    // db->ReadUnlock(std::string("USERTABLE"));
    db->ReadUnlock(std::string("SCHEMA"));
#endif
    return {RC_TRUE};
  }

  rc_t txn_rmw() {
#ifdef BLOCKDDL
    // CRITICAL_SECTION(cs, lock);
    db->ReadLock(std::string("SCHEMA"));
    // db->ReadLock(std::string("USERTABLE"));
#endif
    uint64_t a =
        rand() % oddl_initial_table_size; // 0 ~ oddl_initial_table_size-1

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
    struct Schema_record schema;
#else
    struct Schema_base schema;
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
    //TryVerifyRelaxed(rc);
    if (rc._val != RC_TRUE) TryCatch(rc_t{RC_ABORT_USER});

    struct Schema_base record_test;
    memcpy(&record_test, (char *)v1.data(), sizeof(record_test));

#ifdef SIDDL
    if (record_test.v != schema_version) {
      //TryCatch(rc_t{RC_ABORT_USER});
      //db->Abort(txn);
      goto retry;
    }
#endif

#if !defined(NONEDDL)
    if (schema_version != record_test.v) {
      printf("Write: It should get %lu, but get %lu\n", schema_version,
record_test.v); ALWAYS_ASSERT(record_test.v == schema_version);
    }
#endif*/

    if (schema_version == 0) {
      struct Schema1 record1;
      record1.v = schema_version;
      record1.a = a;
      record1.b = a;

      char str2[sizeof(Schema1)];
      memcpy(str2, &record1, sizeof(str2));
      ermia::varstr &v2 = str(sizeof(str2));
      v2.copy_from(str2, sizeof(str2));

      TryCatch(table_index->UpdateRecord(txn, k1, v2));

    } else {
      struct Schema2 record2;
#ifdef NONEDDL
      /*struct Schema1 record1;
      memcpy(&record1, (char *)v1.data(), sizeof(record1));

      record2.a = record1.a;
      record2.b = record1.b;
      */
#else
      // memcpy(&record2, (char *)v1.data(), sizeof(record2));
#endif

      record2.v = schema_version;
      record2.a = schema_version;
      record2.b = schema_version;
      record2.c = schema_version;

      char str2[sizeof(Schema2)];
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
    // db->ReadUnlock(std::string("USERTABLE"));
    db->ReadUnlock(std::string("SCHEMA"));
#endif
    return {RC_TRUE};
  }
};

void oddlb_do_test(ermia::Engine *db, int argc, char **argv) {
  oddlb_parse_options(argc, argv);
  oddlb_bench_runner<oddlb_sequential_worker> r(db);
  r.run();
}
