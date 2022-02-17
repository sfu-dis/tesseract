#pragma once

#include "benchmarks/bench.h"
#include "engine.h"

class schematable_loader : public bench_loader {
 public:
  schematable_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables)
      : bench_loader(seed, db, open_tables) {}

 protected:
  void load();
};

class microbenchmark_schematable_loader : public bench_loader {
 public:
  microbenchmark_schematable_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables)
      : bench_loader(seed, db, open_tables) {}

 protected:
  void load();
};

void create_schema_table(ermia::Engine *db, const char *name);
