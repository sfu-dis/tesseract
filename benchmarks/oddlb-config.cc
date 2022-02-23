#include <getopt.h>

#include "../engine.h"
#include "bench.h"
#include "oddlb.h"

uint oddl_reps_per_tx = 10;
uint oddl_initial_table_size = 10000000;

void oddlb_create_db(ermia::Engine *db) {
  ermia::thread::Thread *thread = ermia::thread::GetThread(true);
  ALWAYS_ASSERT(thread);

  auto create_table = [=](char *) {
    db->CreateTable("USERTABLE");
    db->CreateMasstreePrimaryIndex("USERTABLE", std::string("USERTABLE"));
  };

  thread->StartTask(create_table);
  thread->Join();
  ermia::thread::PutThread(thread);
}

void oddlb_schematable_loader::load() {
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
    usertable_schema.reformat_idx = i;
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

void oddlb_usertable_loader::load() {
  ermia::OrderedIndex *tbl = open_tables.at("USERTABLE");
  uint32_t nloaders = std::thread::hardware_concurrency() /
                      (numa_max_node() + 1) / 2 * ermia::config::numa_nodes;
  int64_t to_insert = oddl_initial_table_size / nloaders;
  uint64_t start_key = loader_id * to_insert;

  for (uint64_t i = 0; i < to_insert; ++i) {
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());

    const oddlb_kv_1::key k(start_key + i);

    oddlb_kv_1::value record1;
    record1.o_value_version = 0;
    record1.o_value_a = start_key + i;  // a is key
    record1.o_value_b = start_key + i;

    TryVerifyStrict(tbl->InsertRecord(txn, Encode(str(Size(k)), k),
                                      Encode(str(Size(record1)), record1)));
    TryVerifyStrict(db->Commit(txn));
  }

  // Verify inserted values
  for (uint64_t i = 0; i < to_insert; ++i) {
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = 0;

    const oddlb_kv_1::key k(start_key + i);

    ermia::varstr v;

    tbl->GetRecord(txn, rc, Encode(str(Size(k)), k), v, &oid);

    oddlb_kv_1::value *record1_test = (oddlb_kv_1::value *)v.data();
    ALWAYS_ASSERT(record1_test->o_value_version == 0);
    ALWAYS_ASSERT(record1_test->o_value_a == start_key + i);
    ALWAYS_ASSERT(record1_test->o_value_b == start_key + i);
    TryVerifyStrict(rc);
    TryVerifyStrict(db->Commit(txn));
  }

  if (ermia::config::verbose) {
    std::cerr << "[INFO] loader " << loader_id << " loaded " << to_insert
              << " keys in USERTABLE" << std::endl;
  }
}

void oddlb_parse_options(int argc, char **argv) {
  // parse options
  optind = 1;
  while (1) {
    static struct option long_options[] = {
        {"reps-per-tx", required_argument, 0, 'r'},
        {"initial-table-size", required_argument, 0, 's'},
        {0, 0, 0, 0}};

    int option_index = 0;
    int c = getopt_long(argc, argv, "r:s:", long_options, &option_index);
    if (c == -1) break;
    switch (c) {
      case 0:
        if (long_options[option_index].flag != 0) break;
        abort();
        break;

      case 'r':
        oddl_reps_per_tx = strtoul(optarg, NULL, 10);
        break;

      case 's':
        oddl_initial_table_size = strtoul(optarg, NULL, 10);
        break;

      case '?':
        /* getopt_long already printed an error message. */
        exit(1);

      default:
        abort();
    }
  }

  ALWAYS_ASSERT(oddl_initial_table_size);

  if (ermia::config::verbose) {
    std::cerr << "oddlb settings:" << std::endl
              << "  initial user table size:    " << oddl_initial_table_size
              << std::endl
              << "  operations per transaction: " << oddl_reps_per_tx
              << std::endl;
  }
}
