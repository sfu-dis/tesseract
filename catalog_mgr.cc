#include "catalog_mgr.h"

void schematable_loader::load() {
  ermia::OrderedIndex *tbl = open_tables.at("SCHEMA");
  ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());

  char str1[] = "order_line", str2[] = "oorder";
  ermia::varstr &k1 = str(sizeof(str1)), &k2 = str(sizeof(str2));
  k1.copy_from(str1, sizeof(str1));
  k2.copy_from(str2, sizeof(str2));

#ifdef COPYDDL
  struct ermia::Schema_record order_line_schema;
  order_line_schema.index = ermia::Catalog::GetTable("order_line")->GetPrimaryIndex();
  order_line_schema.td = ermia::Catalog::GetTable("order_line");
  order_line_schema.state = 0;
#ifdef LAZYDDL
  order_line_schema.old_index = nullptr;
  order_line_schema.old_td = nullptr;
#elif DCOPYDDL
  order_line_schema.old_td = nullptr;
  order_line_schema.old_v = -1;
#endif

  struct ermia::Schema_record oorder_schema;
  oorder_schema.index = ermia::Catalog::GetTable("oorder")->GetPrimaryIndex();
  oorder_schema.td = ermia::Catalog::GetTable("oorder");
  oorder_schema.state = 0;
#ifdef LAZYDDL
  oorder_schema.old_index = nullptr;
  oorder_schema.old_td = nullptr;
#elif DCOPYDDL
  oorder_schema.old_td = nullptr;
  oorder_schema.old_v = -1;
#endif

  char str3[sizeof(ermia::Schema_record)], str4[sizeof(ermia::Schema_record)];
#else
  struct ermia::Schema_base order_line_schema, oorder_schema;
  char str3[sizeof(ermia::Schema_base)], str4[sizeof(ermia::Schema_record)];
  ;
#endif
  order_line_schema.v = 0;
  memcpy(str3, &order_line_schema, sizeof(str3));
  ermia::varstr &v1 = str(sizeof(str3));
  v1.copy_from(str3, sizeof(str3));

  oorder_schema.v = 0;
  memcpy(str4, &oorder_schema, sizeof(str4));
  ermia::varstr &v2 = str(sizeof(str4));
  v2.copy_from(str4, sizeof(str4));

  TryVerifyStrict(tbl->InsertRecord(txn, k1, v1));
  TryVerifyStrict(tbl->InsertRecord(txn, k2, v2));
  TryVerifyStrict(db->Commit(txn));

  if (ermia::config::verbose) {
    std::cerr << "[INFO] schema table loaded" << std::endl;
  };
}

static bool constraint_verification(uint64_t x) { return true; }

void microbenchmark_schematable_loader::load() {
  ermia::OrderedIndex *tbl = open_tables.at("SCHEMA");
  ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());

  char str1[] = "USERTABLE";
  ermia::varstr &k1 = str(sizeof(str1));
  k1.copy_from(str1, sizeof(str1));

#ifdef COPYDDL
  struct ermia::Schema_record usertable_schema;
  usertable_schema.index =
      ermia::Catalog::GetTable("USERTABLE")->GetPrimaryIndex();
  usertable_schema.td = ermia::Catalog::GetTable("USERTABLE");
  usertable_schema.state = 0;
#ifdef LAZYDDL
  usertable_schema.old_index = nullptr;
  usertable_schema.old_td = nullptr;
#elif DCOPYDDL
  usertable_schema.old_td = nullptr;
  usertable_schema.old_v = -1;
#endif

  char str2[sizeof(ermia::Schema_record)];
#else
  struct ermia::Schema_base usertable_schema;
  char str2[sizeof(ermia::Schema_base)];
  // usertable_schema.op = constraint_verification;
#endif
  usertable_schema.v = 0;
  memcpy(str2, &usertable_schema, sizeof(str2));
  ermia::varstr &v1 = str(sizeof(str2));
  v1.copy_from(str2, sizeof(str2));

  TryVerifyStrict(tbl->InsertRecord(txn, k1, v1));
  TryVerifyStrict(db->Commit(txn));

  if (ermia::config::verbose) {
    std::cerr << "[INFO] schema table loaded" << std::endl;
  };
}

void create_schema_table(ermia::Engine *db, const char *name) {
  ermia::thread::Thread *thread = ermia::thread::GetThread(true);
  ALWAYS_ASSERT(thread);

  auto create_table = [=](char *) {
    db->CreateTable(name);
    db->CreateMasstreePrimaryIndex(name, std::string(name));
#ifdef COPYDDL
    ermia::schema_td = ermia::Catalog::GetTable(name);
#endif
    db->BuildIndexMap(std::string(name));
  };

  thread->StartTask(create_table);
  thread->Join();
  ermia::thread::PutThread(thread);
}

