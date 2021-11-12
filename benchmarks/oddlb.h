#pragma once

#include <string>
#include <utility>
#include <vector>

#include "../catalog_mgr.h"
#include "../macros.h"
#include "../third-party/foedus/zipfian_random.hpp"
#include "bench.h"
#include "../schema.h"
#include "record/encoder.h"
#include "record/inline_str.h"

extern uint oddl_initial_table_size;

struct OddlbWorkload {
  OddlbWorkload(char desc, int16_t insert_percent, int16_t read_percent,
                int16_t update_percent, int16_t scan_percent,
                int16_t rmw_percent)
      : desc(desc), insert_percent_(insert_percent),
        read_percent_(read_percent), update_percent_(update_percent),
        scan_percent_(scan_percent), rmw_percent_(rmw_percent),
        rmw_additional_reads_(0), reps_per_tx_(1), distinct_keys_(true) {}

  OddlbWorkload() {}
  int16_t insert_percent() const { return insert_percent_; }
  int16_t read_percent() const {
    return read_percent_ == 0 ? 0 : read_percent_ - insert_percent_;
  }
  int16_t update_percent() const {
    return update_percent_ == 0 ? 0 : update_percent_ - read_percent_;
  }
  int16_t scan_percent() const {
    return scan_percent_ == 0 ? 0 : scan_percent_ - update_percent_;
  }
  int16_t rmw_percent() const {
    return rmw_percent_ == 0 ? 0 : rmw_percent_ - scan_percent_;
  }

  char desc;
  // Cumulative percentage of i/r/u/s/rmw. From insert...rmw the percentages
  // accumulates, e.g., i=5, r=12 => we'll have 12-5=7% of reads in total.
  int16_t insert_percent_;
  int16_t read_percent_;
  int16_t update_percent_;
  int16_t scan_percent_;
  int16_t rmw_percent_;
  int32_t rmw_additional_reads_;
  int32_t reps_per_tx_;
  bool distinct_keys_;
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

template <class WorkerType> class oddlb_bench_runner : public bench_runner {
public:
  oddlb_bench_runner(ermia::Engine *db) : bench_runner(db) {
    oddlb_create_db(db);
    create_schema_table(db, "SCHEMA");
  }

  virtual void prepare(char *) {
    open_tables["USERTABLE"] = ermia::Catalog::GetPrimaryIndex("USERTABLE");
    open_tables["SCHEMA"] = ermia::Catalog::GetPrimaryIndex("SCHEMA");
  }

protected:
  virtual std::vector<bench_loader *> make_loaders() {
    uint64_t requested = oddl_initial_table_size;
    uint32_t nloaders = std::thread::hardware_concurrency() /
                        (numa_max_node() + 1) / 2 * ermia::config::numa_nodes;
    uint64_t records_per_thread =
        std::max<uint64_t>(1, oddl_initial_table_size / nloaders);
    oddl_initial_table_size = records_per_thread * nloaders;

    std::cerr << "nloaders: " << nloaders
              << ", records_per_thread: " << records_per_thread << std::endl;

    // if (ermia::config::verbose) {
    std::cerr << "[INFO] requested for " << requested << ", will load "
              << oddl_initial_table_size << std::endl;
    //}

    std::vector<bench_loader *> ret;

    ret.push_back(new microbenchmark_schematable_loader(0, db, open_tables));

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
        schema_index((ermia::ConcurrentMasstreeIndex *)open_tables.at("SCHEMA"))
#if defined(SIDDL) || defined(BLOCKDDL) || defined(NONEDDL)
        ,
        table_index(
            (ermia::ConcurrentMasstreeIndex *)open_tables.at("USERTABLE"))
#endif
  {
  }

protected:
  ALWAYS_INLINE ermia::varstr &str(uint64_t size) { return *arena->next(size); }
  ALWAYS_INLINE ermia::varstr &str(ermia::str_arena &a, uint64_t size) {
    return *a.next(size);
  }

  ermia::ConcurrentMasstreeIndex *schema_index;
#if defined(SIDDL) || defined(BLOCKDDL) || defined(NONEDDL)
  ermia::ConcurrentMasstreeIndex *table_index;
#endif

  mcs_lock lock;
};
