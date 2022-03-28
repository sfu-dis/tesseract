#pragma once

#include <string>
#include <utility>
#include <vector>

#include "../catalog_mgr.h"
#include "../macros.h"
#include "../third-party/foedus/zipfian_random.hpp"
#include "bench.h"
#include "oddlb-schemas.h"
#include "record/encoder.h"
#include "record/inline_str.h"

extern uint oddlb_reps_per_tx;
extern uint oddlb_initial_table_size;

struct OddlbWorkload {
  OddlbWorkload(char desc, int16_t read_percent, int16_t update_percent)
      : desc(desc),
        read_percent_(read_percent),
        update_percent_(update_percent) {}

  OddlbWorkload() {}
  int16_t read_percent() const { return read_percent_; }
  int16_t update_percent() const {
    return update_percent_ == 0 ? 0 : update_percent_ - read_percent_;
  }

  char desc;
  // Cumulative percentage of r/u. From read to update the percentages
  // accumulates, e.g., r=5, u=12 => we'll have 12-5=7% of updates in total.
  int16_t read_percent_;
  int16_t update_percent_;
};

class oddlb_schematable_loader : public ermia::catalog::schematable_loader {
 public:
  oddlb_schematable_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables)
      : ermia::catalog::schematable_loader(seed, db, open_tables) {}

 protected:
  void load();
};

class oddlb_usertable_loader : public bench_loader {
 public:
  oddlb_usertable_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      uint32_t loader_id)
      : bench_loader(seed, db, open_tables), loader_id(loader_id) {}

 private:
  uint32_t loader_id;

 protected:
  void load();
};

void oddlb_create_db(ermia::Engine *db);
void oddlb_parse_options(int argc, char **argv);

template <class WorkerType>
class oddlb_bench_runner : public bench_runner {
 public:
  oddlb_bench_runner(ermia::Engine *db) : bench_runner(db) {
    oddlb_create_db(db);
    ermia::catalog::create_schema_table(db, "SCHEMA");
  }

  virtual void prepare(char *) {
    open_tables["USERTABLE"] = ermia::Catalog::GetPrimaryIndex("USERTABLE");
    open_tables["SCHEMA"] = ermia::Catalog::GetPrimaryIndex("SCHEMA");
  }

 protected:
  virtual std::vector<bench_loader *> make_loaders() {
    uint64_t requested = oddlb_initial_table_size;
    uint32_t nloaders = std::thread::hardware_concurrency() /
                        (numa_max_node() + 1) / 2 * ermia::config::numa_nodes;
    uint64_t records_per_thread =
        std::max<uint64_t>(1, oddlb_initial_table_size / nloaders);
    oddlb_initial_table_size = records_per_thread * nloaders;

    std::cerr << "nloaders: " << nloaders
              << ", records_per_thread: " << records_per_thread << std::endl;

    std::cerr << "[INFO] requested for " << requested << ", will load "
              << oddlb_initial_table_size << std::endl;

    std::vector<bench_loader *> ret;

    ret.push_back(new oddlb_schematable_loader(0, db, open_tables));

    for (uint32_t i = 0; i < nloaders; ++i) {
      ret.push_back(new oddlb_usertable_loader(0, db, open_tables, i));
    }

    return ret;
  }

  virtual std::vector<bench_worker *> make_workers() {
    util::fast_random r(8544290);
    std::vector<bench_worker *> ret;
    for (size_t i = 0; i < ermia::config::worker_threads; i++) {
      auto seed = r.next();
      LOG(INFO) << "RND SEED: " << seed;
      ret.push_back(
          new WorkerType(i, seed, db, open_tables, &barrier_a, &barrier_b));
    }
    return ret;
  }
};

class oddlb_base_worker : public bench_worker {
 public:
  oddlb_base_worker(
      unsigned int worker_id, unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      spin_barrier *barrier_a, spin_barrier *barrier_b)
      : bench_worker(worker_id, true, seed, db, open_tables, barrier_a,
                     barrier_b),
        schema_index(
            (ermia::ConcurrentMasstreeIndex *)open_tables.at("SCHEMA")),
        schema_fid(
            open_tables.at("SCHEMA")->GetTableDescriptor()->GetTupleFid()),
        table_fid(
            open_tables.at("USERTABLE")->GetTableDescriptor()->GetTupleFid())
#if defined(SIDDL) || defined(BLOCKDDL)
        ,
        table_index(
            (ermia::ConcurrentMasstreeIndex *)open_tables.at("USERTABLE"))
#endif
  {
    const schema_kv::key k(table_fid);
    table_key =
        (ermia::varstr *)ermia::MM::allocate(sizeof(ermia::varstr) + sizeof(k));
    new (table_key)
        ermia::varstr((char *)table_key + sizeof(ermia::varstr), sizeof(k));
    Encode(*table_key, k);
  }

 protected:
  ALWAYS_INLINE ermia::varstr &str(uint64_t size) { return *arena->next(size); }
  ALWAYS_INLINE ermia::varstr &str(ermia::str_arena &a, uint64_t size) {
    return *a.next(size);
  }

  ermia::varstr &GenerateValue(uint32_t a, ermia::schema_record *schema) {
    uint32_t schema_version = schema->v;
    switch (schema_version) {
      case 1: {
        oddlb_kv_2::value record2;
        record2.o_value_version = schema_version;
        record2.o_value_a = a;
        record2.o_value_b = schema_version;
        record2.o_value_c = schema_version;
        return Encode(str(Size(record2)), record2);
      }
      case 2: {
        oddlb_kv_3::value record3;
        record3.o_value_version = schema_version;
        record3.o_value_a = a;
        record3.o_value_b = schema_version;
        record3.o_value_c = schema_version;
        record3.o_value_d = schema_version;
        return Encode(str(Size(record3)), record3);
      }
      case 3: {
        oddlb_kv_4::value record4;
        record4.o_value_version = schema_version;
        record4.o_value_a = a;
        record4.o_value_b = schema_version;
        record4.o_value_c = schema_version;
        record4.o_value_d = schema_version;
        record4.o_value_e = schema_version;
        return Encode(str(Size(record4)), record4);
      }
      case 4: {
        oddlb_kv_5::value record5;
        record5.o_value_version = schema_version;
        record5.o_value_a = a;
        record5.o_value_b = schema_version;
        record5.o_value_c = schema_version;
        record5.o_value_d = schema_version;
        record5.o_value_e = schema_version;
        record5.o_value_f = schema_version;
        return Encode(str(Size(record5)), record5);
      }
      case 5: {
        oddlb_kv_6::value record6;
        record6.o_value_version = schema_version;
        record6.o_value_a = a;
        record6.o_value_b = schema_version;
        record6.o_value_c = schema_version;
        record6.o_value_d = schema_version;
        record6.o_value_e = schema_version;
        record6.o_value_f = schema_version;
        record6.o_value_g = schema_version;
        return Encode(str(Size(record6)), record6);
      }
      default: {
        LOG(FATAL) << "Not supported";
      }
    }
    return str(0);
  }

  // 0: add column
  // 1: add column (no-copy-no-verification)
  // 2: add constraint
  // 3: add column & add constraint
  static ermia::ddl::ddl_type get_example_ddl_type(uint32_t ddl_example) {
    switch (ddl_example) {
      case 0:
        return ermia::ddl::ddl_type::COPY_ONLY;
      case 1:
        return ermia::ddl::ddl_type::NO_COPY_VERIFICATION;
      case 2:
        return ermia::ddl::ddl_type::VERIFICATION_ONLY;
      case 3:
        return ermia::ddl::ddl_type::COPY_VERIFICATION;
      default:
        LOG(FATAL) << "Not supported";
        return ermia::ddl::ddl_type::INVALID;
    }
  }

  ermia::ConcurrentMasstreeIndex *schema_index;
  ermia::varstr *table_key;
  ermia::FID schema_fid;
  ermia::FID table_fid;
#if defined(SIDDL) || defined(BLOCKDDL)
  ermia::ConcurrentMasstreeIndex *table_index;
#endif
};
