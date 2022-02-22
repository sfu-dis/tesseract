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

void oddlb_usertable_loader::load() {
  ermia::OrderedIndex *tbl = open_tables.at("USERTABLE");
  uint32_t nloaders = std::thread::hardware_concurrency() /
                      (numa_max_node() + 1) / 2 * ermia::config::numa_nodes;
  int64_t to_insert = oddl_initial_table_size / nloaders;
  uint64_t start_key = loader_id * to_insert;

  for (uint64_t i = 0; i < to_insert; ++i) {
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());

    struct ermia::Schema1 record1;
    record1.v = 0;
    record1.a = start_key + i;  // a is key
    record1.b = start_key + i;

    char str1[sizeof(record1.a)], str2[sizeof(record1)];
    memcpy(str1, &record1.a, sizeof(str1));
    memcpy(str2, &record1, sizeof(str2));

    ermia::varstr &k = str(sizeof(str1));
    k.copy_from(str1, sizeof(str1));

    ermia::varstr &v = str(sizeof(str2));
    v.copy_from(str2, sizeof(str2));

    TryVerifyStrict(tbl->InsertRecord(txn, k, v));
    TryVerifyStrict(db->Commit(txn));
  }

  // Verify inserted values
  for (uint64_t i = 0; i < to_insert; ++i) {
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
    rc_t rc = rc_t{RC_INVALID};
    ermia::OID oid = 0;

    char str1[sizeof(uint64_t)];
    uint64_t a = start_key + i;
    memcpy(str1, &a, sizeof(str1));

    ermia::varstr &k = str(sizeof(str1));
    k.copy_from(str1, sizeof(str1));

    ermia::varstr &v = str(0);

    tbl->GetRecord(txn, rc, k, v, &oid);

    struct ermia::Schema1 record1_test;
    memcpy(&record1_test, (char *)v.data(), sizeof(record1_test));
    ALWAYS_ASSERT(record1_test.v == 0);
    ALWAYS_ASSERT(record1_test.a == start_key + i);
    ALWAYS_ASSERT(record1_test.b == start_key + i);
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
