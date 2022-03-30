#pragma once

#include "benchmarks/bench.h"
#include "engine.h"

namespace ermia {

namespace catalog {

class schematable_loader : public bench_loader {
 public:
  schematable_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables)
      : bench_loader(seed, db, open_tables) {}

 protected:
  virtual void load() = 0;
};

void create_schema_table(ermia::Engine *db, const char *name);

void read_schema(transaction *t, ConcurrentMasstreeIndex *schema_table_index,
                 ConcurrentMasstreeIndex *target_table_index,
                 const varstr &table_name, varstr &out_schema_value, OID *out_schema_oid);

rc_t write_schema(transaction *t, ConcurrentMasstreeIndex *schema_table_index,
                  const varstr &table_name, varstr &schema_value,
                  OID *out_schema_oid, ddl::ddl_executor *ddl_exe,
                  bool is_insert = false);

}  // namespace catalog

}  // namespace ermia
