#pragma once

#include "engine.h"
#include "sm-table.h"

namespace ermia {

struct Schema_base {
  uint64_t v;
  uint64_t reformat_idx;
  uint64_t constraint_idx;
  ddl::ddl_type ddl_type;
  OrderedIndex *index;
  TableDescriptor *td;
};

struct Schema_record : public Schema_base {
  TableDescriptor *old_td;
  uint64_t state;
  uint64_t old_v;
#ifdef LAZYDDL
  OrderedIndex *old_index;
  TableDescriptor *old_tds[16];
#endif
};

} // namespace ermia
