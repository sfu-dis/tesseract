#pragma once

#include "../engine.h"

namespace ermia {
   
struct Schema_base_ {
  uint64_t v;
};

struct Schema_base {
  uint64_t v;
  uint64_t reformat_idx;
  uint64_t constraint_idx;
  uint64_t secondary_index_key_create_idx;
  ddl::ddl_type ddl_type;
  OrderedIndex *index;
  TableDescriptor *td;
  bool show_index; // simulate no index
};

struct Schema_record : public Schema_base {
  ermia::TableDescriptor *old_td;
  ermia::TableDescriptor *td;
  ermia::OrderedIndex *index;
  uint64_t state;
  uint64_t old_v;
  OrderedIndex *old_index;
  TableDescriptor *old_tds[16];
  uint64_t reformats[16];
#ifdef LAZYDDL
  ermia::OrderedIndex *old_index;
  ermia::TableDescriptor *old_tds[16];
#endif
};

struct Schema1 : public Schema_base {
  uint64_t a;
  uint64_t b;
};

struct Schema2 : public Schema_base {
  uint64_t a;
  uint64_t b;
  uint64_t c;
};

} // namespace ermia
