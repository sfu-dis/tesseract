#pragma once

#include "engine.h"
#include "sm-table.h"

namespace ermia {

struct Schema_base {
  uint64_t v;
};

struct Schema_record : public Schema_base {
  OrderedIndex *index;
  TableDescriptor *td;
  TableDescriptor *old_td;
  uint64_t state;
  uint64_t old_v;
#ifdef LAZYDDL
  OrderedIndex *old_index;
  TableDescriptor *old_tds[16];
#endif
};

} // namespace ermia
