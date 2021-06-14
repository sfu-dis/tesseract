#include "catalog_mgr.h"

void schematable_loader::load() {
  ermia::OrderedIndex *tbl = open_tables.at("SCHEMA");
  ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());

  char str1[] = "order_line";
  ermia::varstr &k = str(sizeof(str1));
  k.copy_from(str1, sizeof(str1));

#ifdef COPYDDL
  struct Schema_record schema;
  schema.index = ermia::Catalog::GetTable("order_line")->GetPrimaryIndex();
  schema.td = ermia::Catalog::GetTable("order_line");
  char str2[sizeof(Schema_record)];
#else
  struct Schema_base schema;
  char str2[sizeof(Schema_base)];
#endif
  schema.v = 0;
  memcpy(str2, &schema, sizeof(str2));
  ermia::varstr &v = str(sizeof(str2));
  v.copy_from(str2, sizeof(str2));

  TryVerifyStrict(tbl->InsertRecord(txn, k, v));
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

