#include "catalog_mgr.h"

void schematable_loader::load() {
  ermia::OrderedIndex *tbl = open_tables.at("SCHEMA");
  ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());

  char str1[] = "order_line", str2[] = "oorder";
  ermia::varstr &k1 = str(sizeof(str1)), &k2 = str(sizeof(str2));
  k1.copy_from(str1, sizeof(str1));
  k2.copy_from(str2, sizeof(str2));

#ifdef COPYDDL
  struct Schema_record order_line_schema;
  order_line_schema.index = ermia::Catalog::GetTable("order_line")->GetPrimaryIndex();
  order_line_schema.td = ermia::Catalog::GetTable("order_line");
#ifdef LAZYDDL
  order_line_schema.old_index = nullptr;
  order_line_schema.old_td = nullptr;
#endif

  struct Schema_record oorder_schema;
  oorder_schema.index = ermia::Catalog::GetTable("oorder")->GetPrimaryIndex();
  oorder_schema.td = ermia::Catalog::GetTable("oorder");
#ifdef LAZYDDL
  oorder_schema.old_index = nullptr;
  oorder_schema.old_td = nullptr;
#endif

  char str3[sizeof(Schema_record)], str4[sizeof(Schema_record)];
#else
  struct Schema_base order_line_schema, oorder_schema;
  char str3[sizeof(Schema_base)], str4[sizeof(Schema_record)];;
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

