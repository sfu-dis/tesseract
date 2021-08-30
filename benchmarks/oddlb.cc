/*
 * An Online DDL Benchmark.
 */
#include "oddlb.h"
#include "bench.h"
#include <sstream>

volatile bool ddl_running_1 = true;
volatile bool ddl_running_2 = false;
std::atomic<uint64_t> ddl_end(0);

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

    w.push_back(workload_desc("Read", 0.2, TxnRead));
    w.push_back(workload_desc("RMW", 0.7999985, TxnRMW));
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
#endif
    memcpy(str2, &schema, sizeof(str2));
    ermia::varstr &v2 = str(sizeof(str2));
    v2.copy_from(str2, sizeof(str2));

    db->WriteUnlock(str3.c_str());

    txn->set_table_descriptors(schema.td, old_td);

#if !defined(LAZYDDL)
    uint32_t cdc_threads = ermia::config::cdc_threads;
    uint logs_per_cdc_thread =
        (ermia::config::worker_threads - 1) / cdc_threads;
    std::vector<ermia::thread::Thread *> cdc_workers;
    for (uint i = 0; i < cdc_threads; i++) {
      ermia::thread::Thread *thread = ermia::thread::GetThread(true);
      ALWAYS_ASSERT(thread);
      cdc_workers.push_back(thread);

      uint32_t thread_id = ermia::thread::MyId();
      auto parallel_changed_data_capture = [=](char *) {
        // printf("thread_id: %u, my_thread_id: %u\n", thread_id,
        // ermia::thread::MyId());
        uint32_t begin_log = i * logs_per_cdc_thread;
        uint32_t end_log = (i + 1) * logs_per_cdc_thread - 1;
        ;
        if (i == cdc_threads - 1)
          end_log = ermia::config::MAX_THREADS;
        bool ddl_end_local = false;
        while (ddl_running_1 || !ddl_end_local) {
          ddl_end_local = schema.td->GetPrimaryIndex()->changed_data_capture(
              txn, txn->GetXIDContext()->begin, txn->GetXIDContext()->end,
              thread_id, begin_log, end_log);
          // usleep(1000);
        }
      };

      thread->StartTask(parallel_changed_data_capture);
    }
#endif

    rc = rc_t{RC_INVALID};
    schema_index->WriteSchemaTable(txn, rc, k1, v2);
    TryCatch(rc);

#if !defined(LAZYDDL)
    ermia::ConcurrentMasstreeIndex *table_index =
        (ermia::ConcurrentMasstreeIndex *)schema.index;
    rc = rc_t{RC_INVALID};
    rc = table_index->WriteNormalTable(arena, old_table_index, txn, v2, NULL);
    if (rc._val != RC_TRUE) {
      ddl_running_1 = false;
      for (std::vector<ermia::thread::Thread *>::const_iterator it =
               cdc_workers.begin();
           it != cdc_workers.end(); ++it) {
        (*it)->Join();
        ermia::thread::PutThread(*it);
        // printf("thread joined\n");
      }
    }
    TryCatch(rc);
#endif

#if !defined(LAZYDDL)
    ddl_running_1 = false;
    for (std::vector<ermia::thread::Thread *>::const_iterator it =
             cdc_workers.begin();
         it != cdc_workers.end(); ++it) {
      (*it)->Join();
      ermia::thread::PutThread(*it);
      // printf("thread joined\n");
    }
#endif

    TryCatch(db->Commit(txn));

    /*thread->Join();
    ermia::thread::PutThread(thread);
    printf("thread joined\n");
    */
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
    // rc = table_index->WriteNormalTable(arena, table_index, txn, v, NULL);
    rc = table_index->CheckNormalTable(arena, table_index, txn, NULL);
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

      record2_test.a = a;
      record2_test.b = schema_version;
      record2_test.c = schema_version;
#else
      memcpy(&record2_test, (char *)v2.data(), sizeof(record2_test));
#endif

      ALWAYS_ASSERT(record2_test.a == a);
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

      record2.v = schema_version;
      record2.a = a;
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
