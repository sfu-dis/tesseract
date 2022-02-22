#include "catalog_mgr.h"

#include "benchmarks/oddlb-schemas.h"
#include "benchmarks/tpcc.h"

void schematable_loader::load() {
  ermia::OrderedIndex *tbl = open_tables.at("SCHEMA");
  ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());

  auto add_column = [=](ermia::varstr *key, ermia::varstr &value,
                        ermia::str_arena *arena, uint64_t schema_version,
                        ermia::FID fid, ermia::OID oid) {
    order_line::value v_ol_temp;
    const order_line::value *v_ol = Decode(value, v_ol_temp);

    order_line_1::value v_ol_1;
    v_ol_1.ol_i_id = v_ol->ol_i_id;
    v_ol_1.ol_delivery_d = v_ol->ol_delivery_d;
    v_ol_1.ol_amount = v_ol->ol_amount;
    v_ol_1.ol_supply_w_id = v_ol->ol_supply_w_id;
    v_ol_1.ol_quantity = v_ol->ol_quantity;
    v_ol_1.ol_tax = (float)0.1;

    const size_t order_line_sz = ::Size(v_ol_1);
    ermia::varstr *new_value = arena->next(order_line_sz);
    new_value = &Encode(*new_value, v_ol_1);

    return new_value;
  };

  char str1[] = "order_line", str2[] = "oorder", str3[] = "customer";
  ermia::varstr &k1 = str(sizeof(str1)), &k2 = str(sizeof(str2)),
                &k3 = str(sizeof(str3));
  k1.copy_from(str1, sizeof(str1));
  k2.copy_from(str2, sizeof(str2));
  k3.copy_from(str3, sizeof(str3));

  struct ermia::schema_record order_line_schema;
  order_line_schema.state = ermia::ddl::schema_state_type::READY;
  order_line_schema.old_td = nullptr;
  order_line_schema.old_index = nullptr;

  struct ermia::schema_record oorder_schema;
  oorder_schema.state = ermia::ddl::schema_state_type::READY;
  oorder_schema.old_td = nullptr;
  oorder_schema.old_index = nullptr;

  struct ermia::schema_record customer_schema;
  customer_schema.state = ermia::ddl::schema_state_type::READY;
  customer_schema.old_td = nullptr;
  customer_schema.old_index = nullptr;

  order_line_schema.v = 0;
  order_line_schema.csn = 0;
  order_line_schema.reformat_idx = ermia::ddl::reformats.size();
  order_line_schema.secondary_index_key_create_idx = -1;
  ermia::ddl::reformats.push_back(add_column);
  order_line_schema.constraint_idx = -1;
  order_line_schema.index =
      ermia::Catalog::GetTable("order_line")->GetPrimaryIndex();
  order_line_schema.td = ermia::Catalog::GetTable("order_line");
  order_line_schema.show_index = ermia::config::ddl_example == 3 ? false : true;
  ermia::varstr &v1 = str(sizeof(order_line_schema));
  v1.copy_from((char *)&order_line_schema, sizeof(order_line_schema));

  oorder_schema.v = 0;
  oorder_schema.csn = 0;
  oorder_schema.secondary_index_key_create_idx = -1;
  oorder_schema.index = ermia::Catalog::GetTable("oorder")->GetPrimaryIndex();
  oorder_schema.td = ermia::Catalog::GetTable("oorder");
  oorder_schema.show_index = true;
  ermia::varstr &v2 = str(sizeof(oorder_schema));
  v2.copy_from((char *)&oorder_schema, sizeof(oorder_schema));

  customer_schema.v = 0;
  customer_schema.csn = 0;
  customer_schema.reformat_idx = -1;
  customer_schema.constraint_idx = -1;
  customer_schema.secondary_index_key_create_idx = -1;
  customer_schema.index =
      ermia::Catalog::GetTable("customer")->GetPrimaryIndex();
  customer_schema.td = ermia::Catalog::GetTable("customer");
  customer_schema.show_index = true;
  ermia::varstr &v3 = str(sizeof(customer_schema));
  v3.copy_from((char *)&customer_schema, sizeof(customer_schema));

  TryVerifyStrict(tbl->InsertRecord(txn, k1, v1));
  TryVerifyStrict(tbl->InsertRecord(txn, k2, v2));
  TryVerifyStrict(tbl->InsertRecord(txn, k3, v3));
  TryVerifyStrict(db->Commit(txn));

  if (ermia::config::verbose) {
    std::cerr << "[INFO] schema table loaded" << std::endl;
  };
}

void microbenchmark_schematable_loader::load() {
  ermia::OrderedIndex *tbl = open_tables.at("SCHEMA");
  ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());

  auto add_column = [=](ermia::varstr *key, ermia::varstr &value,
                        ermia::str_arena *arena, uint64_t schema_version,
                        ermia::FID fid, ermia::OID oid) {
    uint64_t a = 0;
    if (schema_version == 1) {
      oddlb_kv_1::value *record = (oddlb_kv_1::value *)value.data();
      a = record->o_value_a;
    } else {
      oddlb_kv_2::value *record = (oddlb_kv_2::value *)value.data();
      a = record->o_value_a;
    }

    oddlb_kv_2::value record2;
    record2.o_value_version = schema_version;
    record2.o_value_a = a;
    record2.o_value_b = schema_version;
    record2.o_value_c = schema_version;
    ermia::varstr *new_value = arena->next(sizeof(record2));
    new_value->copy_from((char *)&record2, sizeof(record2));
    return new_value;
  };

  auto add_column_1 = [=](ermia::varstr *key, ermia::varstr &value,
                          ermia::str_arena *arena, uint64_t schema_version,
                          ermia::FID fid, ermia::OID oid) {
    oddlb_kv_2::value *record = (oddlb_kv_2::value *)value.data();

    oddlb_kv_3::value record2;
    record2.o_value_version = schema_version;
    record2.o_value_a = record->o_value_a;
    record2.o_value_b = schema_version;
    record2.o_value_c = schema_version;
    record2.o_value_d = schema_version;
    ermia::varstr *new_value = arena->next(sizeof(record2));
    new_value->copy_from((char *)&record2, sizeof(record2));
    return new_value;
  };

  auto add_column_2 = [=](ermia::varstr *key, ermia::varstr &value,
                          ermia::str_arena *arena, uint64_t schema_version,
                          ermia::FID fid, ermia::OID oid) {
    oddlb_kv_3::value *record = (oddlb_kv_3::value *)value.data();

    oddlb_kv_4::value record2;
    record2.o_value_version = schema_version;
    record2.o_value_a = record->o_value_a;
    record2.o_value_b = schema_version;
    record2.o_value_c = schema_version;
    record2.o_value_d = schema_version;
    record2.o_value_e = schema_version;
    ermia::varstr *new_value = arena->next(sizeof(record2));
    new_value->copy_from((char *)&record2, sizeof(record2));
    return new_value;
  };

  auto add_column_3 = [=](ermia::varstr *key, ermia::varstr &value,
                          ermia::str_arena *arena, uint64_t schema_version,
                          ermia::FID fid, ermia::OID oid) {
    oddlb_kv_4::value *record = (oddlb_kv_4::value *)value.data();

    oddlb_kv_5::value record2;
    record2.o_value_version = schema_version;
    record2.o_value_a = record->o_value_a;
    record2.o_value_b = schema_version;
    record2.o_value_c = schema_version;
    record2.o_value_d = schema_version;
    record2.o_value_e = schema_version;
    record2.o_value_f = schema_version;
    ermia::varstr *new_value = arena->next(sizeof(record2));
    new_value->copy_from((char *)&record2, sizeof(record2));
    return new_value;
  };

  auto add_column_4 = [=](ermia::varstr *key, ermia::varstr &value,
                          ermia::str_arena *arena, uint64_t schema_version,
                          ermia::FID fid, ermia::OID oid) {
    oddlb_kv_5::value *record = (oddlb_kv_5::value *)value.data();

    oddlb_kv_6::value record2;
    record2.o_value_version = schema_version;
    record2.o_value_a = record->o_value_a;
    record2.o_value_b = schema_version;
    record2.o_value_c = schema_version;
    record2.o_value_d = schema_version;
    record2.o_value_e = schema_version;
    record2.o_value_f = schema_version;
    record2.o_value_g = schema_version;
    ermia::varstr *new_value = arena->next(sizeof(record2));
    new_value->copy_from((char *)&record2, sizeof(record2));
    return new_value;
  };

  auto column_verification = [=](ermia::varstr &value,
                                 uint64_t schema_version) {
    if (schema_version == 1) {
      oddlb_kv_1::value *record = (oddlb_kv_1::value *)value.data();
      return record->o_value_b < 10000000;
    } else {
      oddlb_kv_2::value *record = (oddlb_kv_2::value *)value.data();
      return record->o_value_b < 10000000;
    }
  };

  char str1[] = "USERTABLE";
  ermia::varstr &k1 = str(sizeof(str1));
  k1.copy_from(str1, sizeof(str1));

  struct ermia::schema_record usertable_schema;
  usertable_schema.state = ermia::ddl::schema_state_type::READY;
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
  usertable_schema.old_index = nullptr;
  usertable_schema.v = 0;
  usertable_schema.csn = 0;
  if (ermia::config::ddl_type != 4) {
    usertable_schema.reformat_idx = ermia::ddl::reformats.size();
    ermia::ddl::reformats.push_back(add_column);
  }
  usertable_schema.constraint_idx = ermia::ddl::constraints.size();
  usertable_schema.secondary_index_key_create_idx = -1;
  ermia::ddl::constraints.push_back(column_verification);
  usertable_schema.index =
      ermia::Catalog::GetTable("USERTABLE")->GetPrimaryIndex();
  usertable_schema.td = ermia::Catalog::GetTable("USERTABLE");
  usertable_schema.show_index = true;
  ermia::varstr &v1 = str(sizeof(usertable_schema));
  v1.copy_from((char *)&usertable_schema, sizeof(usertable_schema));

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
  };

  thread->StartTask(create_table);
  thread->Join();
  ermia::thread::PutThread(thread);
}
