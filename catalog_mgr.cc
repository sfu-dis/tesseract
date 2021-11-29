#include "catalog_mgr.h"
#include "benchmarks/oddlb-schemas.h"
#include "benchmarks/tpcc.h"

void schematable_loader::load() {
  ermia::OrderedIndex *tbl = open_tables.at("SCHEMA");
  ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());

  auto add_column = [=](ermia::varstr &value, ermia::str_arena *arena,
                        uint64_t schema_version) {
    order_line::value v_ol_temp;
    const order_line::value *v_ol = Decode(value, v_ol_temp);

    order_line_1::value v_ol_1;
    v_ol_1.ol_i_id = v_ol->ol_i_id;
    v_ol_1.ol_delivery_d = v_ol->ol_delivery_d;
    v_ol_1.ol_amount = v_ol->ol_amount;
    v_ol_1.ol_supply_w_id = v_ol->ol_supply_w_id;
    v_ol_1.ol_quantity = v_ol->ol_quantity;
    v_ol_1.v = schema_version;
    v_ol_1.ol_tax = 0.1;

    const size_t order_line_sz = ::Size(v_ol_1);
    ermia::varstr *new_value = arena->next(order_line_sz);
    new_value = &Encode(*new_value, v_ol_1);

    return new_value;
  };

  char str1[] = "order_line", str2[] = "oorder";
  ermia::varstr &k1 = str(sizeof(str1)), &k2 = str(sizeof(str2));
  k1.copy_from(str1, sizeof(str1));
  k2.copy_from(str2, sizeof(str2));

#ifdef COPYDDL
  struct ermia::Schema_record order_line_schema;
  order_line_schema.state = 0;
  order_line_schema.old_td = nullptr;
#ifdef LAZYDDL
  order_line_schema.old_index = nullptr;
#elif DCOPYDDL
  order_line_schema.old_v = -1;
#endif

  struct ermia::Schema_record oorder_schema;
  oorder_schema.state = 0;
  oorder_schema.old_td = nullptr;
#ifdef LAZYDDL
  oorder_schema.old_index = nullptr;
#elif DCOPYDDL
  oorder_schema.old_v = -1;
#endif

  char str3[sizeof(ermia::Schema_record)], str4[sizeof(ermia::Schema_record)];
#else
  struct ermia::Schema_base order_line_schema, oorder_schema;
  char str3[sizeof(ermia::Schema_base)], str4[sizeof(ermia::Schema_record)];
#endif
  order_line_schema.v = 0;
  order_line_schema.reformat_idx = ermia::ddl::reformats.size();
  ermia::ddl::reformats.push_back(add_column);
  order_line_schema.constraint_idx = -1;
  order_line_schema.index =
      ermia::Catalog::GetTable("order_line")->GetPrimaryIndex();
  order_line_schema.td = ermia::Catalog::GetTable("order_line");
  memcpy(str3, &order_line_schema, sizeof(str3));
  ermia::varstr &v1 = str(sizeof(str3));
  v1.copy_from(str3, sizeof(str3));

  oorder_schema.v = 0;
  oorder_schema.index = ermia::Catalog::GetTable("oorder")->GetPrimaryIndex();
  oorder_schema.td = ermia::Catalog::GetTable("oorder");
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

void microbenchmark_schematable_loader::load() {
  ermia::OrderedIndex *tbl = open_tables.at("SCHEMA");
  ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());

  auto add_column = [=](ermia::varstr &value, ermia::str_arena *arena,
                        uint64_t schema_version) {
    uint64_t a = 0;
    if (schema_version == 1) {
      struct ermia::Schema1 record;
      memcpy(&record, (char *)value.data(), sizeof(record));
      a = record.a;
    } else {
      struct ermia::Schema2 record;
      memcpy(&record, (char *)value.data(), sizeof(record));
      a = record.a;
    }

    char str2[sizeof(ermia::Schema2)];
    struct ermia::Schema2 record2;
    record2.v = schema_version;
    record2.a = a;
    record2.b = schema_version;
    record2.c = schema_version;
    memcpy(str2, &record2, sizeof(str2));
    ermia::varstr *new_value = arena->next(sizeof(str2));
    new_value->copy_from(str2, sizeof(str2));
    return new_value;
  };

  auto add_column_1 = [=](ermia::varstr &value, ermia::str_arena *arena,
                          uint64_t schema_version) {
    struct ermia::Schema2 record;
    memcpy(&record, (char *)value.data(), sizeof(record));
    uint64_t a = record.a;

    char str2[sizeof(ermia::Schema3)];
    struct ermia::Schema3 record2;
    record2.v = schema_version;
    record2.a = a;
    record2.b = schema_version;
    record2.c = schema_version;
    record2.d = schema_version;
    memcpy(str2, &record2, sizeof(str2));
    ermia::varstr *new_value = arena->next(sizeof(str2));
    new_value->copy_from(str2, sizeof(str2));
    return new_value;
  };

  auto add_column_2 = [=](ermia::varstr &value, ermia::str_arena *arena,
                          uint64_t schema_version) {
    struct ermia::Schema3 record;
    memcpy(&record, (char *)value.data(), sizeof(record));
    uint64_t a = record.a;

    char str2[sizeof(ermia::Schema4)];
    struct ermia::Schema4 record2;
    record2.v = schema_version;
    record2.a = a;
    record2.b = schema_version;
    record2.c = schema_version;
    record2.d = schema_version;
    record2.e = schema_version;
    memcpy(str2, &record2, sizeof(str2));
    ermia::varstr *new_value = arena->next(sizeof(str2));
    new_value->copy_from(str2, sizeof(str2));
    return new_value;
  };

  auto add_column_3 = [=](ermia::varstr &value, ermia::str_arena *arena,
                          uint64_t schema_version) {
    struct ermia::Schema4 record;
    memcpy(&record, (char *)value.data(), sizeof(record));
    uint64_t a = record.a;

    char str2[sizeof(ermia::Schema5)];
    struct ermia::Schema5 record2;
    record2.v = schema_version;
    record2.a = a;
    record2.b = schema_version;
    record2.c = schema_version;
    record2.d = schema_version;
    record2.e = schema_version;
    record2.f = schema_version;
    memcpy(str2, &record2, sizeof(str2));
    ermia::varstr *new_value = arena->next(sizeof(str2));
    new_value->copy_from(str2, sizeof(str2));
    return new_value;
  };

  auto add_column_4 = [=](ermia::varstr &value, ermia::str_arena *arena,
                          uint64_t schema_version) {
    struct ermia::Schema5 record;
    memcpy(&record, (char *)value.data(), sizeof(record));
    uint64_t a = record.a;

    char str2[sizeof(ermia::Schema6)];
    struct ermia::Schema6 record2;
    record2.v = schema_version;
    record2.a = a;
    record2.b = schema_version;
    record2.c = schema_version;
    record2.d = schema_version;
    record2.e = schema_version;
    record2.f = schema_version;
    record2.g = schema_version;
    memcpy(str2, &record2, sizeof(str2));
    ermia::varstr *new_value = arena->next(sizeof(str2));
    new_value->copy_from(str2, sizeof(str2));
    return new_value;
  };

  auto column_verification = [=](ermia::varstr &value,
                                 uint64_t schema_version) {
    if (schema_version == 1) {
      struct ermia::Schema1 record;
      memcpy(&record, (char *)value.data(), sizeof(record));
      return record.b < 10000000;
    } else {
      struct ermia::Schema2 record;
      memcpy(&record, (char *)value.data(), sizeof(record));
      return record.b < 10000000;
    }
  };

  char str1[] = "USERTABLE";
  ermia::varstr &k1 = str(sizeof(str1));
  k1.copy_from(str1, sizeof(str1));

#ifdef COPYDDL
  struct ermia::Schema_record usertable_schema;
  usertable_schema.state = 0;
  usertable_schema.old_td = nullptr;
  if (ermia::config::ddl_type == 4) {
    int i = 0;
    usertable_schema.reformats[i++] = ermia::ddl::reformats.size();
    ermia::ddl::reformats.push_back(add_column);
    usertable_schema.reformats[i++] = ermia::ddl::reformats.size();
    ermia::ddl::reformats.push_back(add_column_1);
    usertable_schema.reformats[i++] = ermia::ddl::reformats.size();
    ermia::ddl::reformats.push_back(add_column_2);
    usertable_schema.reformats[i++] = ermia::ddl::reformats.size();
    ermia::ddl::reformats.push_back(add_column_3);
    usertable_schema.reformats[i++] = ermia::ddl::reformats.size();
    ermia::ddl::reformats.push_back(add_column_4);
  }
#ifdef LAZYDDL
  usertable_schema.old_index = nullptr;
#elif DCOPYDDL
  usertable_schema.old_v = -1;
#endif

  char str2[sizeof(ermia::Schema_record)];
#else
  struct ermia::Schema_base usertable_schema;
  char str2[sizeof(ermia::Schema_base)];
#endif
  usertable_schema.v = 0;
  if (ermia::config::ddl_type != 4) {
    usertable_schema.reformat_idx = ermia::ddl::reformats.size();
    ermia::ddl::reformats.push_back(add_column);
  }
  usertable_schema.constraint_idx = ermia::ddl::constraints.size();
  ermia::ddl::constraints.push_back(column_verification);
  usertable_schema.index =
      ermia::Catalog::GetTable("USERTABLE")->GetPrimaryIndex();
  usertable_schema.td = ermia::Catalog::GetTable("USERTABLE");
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
    ermia::schema_td = ermia::Catalog::GetTable(name);
#ifdef BLOCKDDL
    db->BuildLockMap(ermia::Catalog::GetTable(name)->GetTupleFid());
#endif
  };

  thread->StartTask(create_table);
  thread->Join();
  ermia::thread::PutThread(thread);
}

