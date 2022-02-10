#pragma once

#include "engine.h"
#include "sm-table.h"

namespace ermia {

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
  TableDescriptor *old_td;
  uint64_t state;
  uint64_t old_v;
  OrderedIndex *old_index;
  TableDescriptor *old_tds[16];
  uint64_t reformats[16];
};

} // namespace ermia
