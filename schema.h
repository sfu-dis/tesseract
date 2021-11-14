#pragma once

#include "engine.h"

struct Schema_base {
  uint64_t v;
};

struct Schema_record : public Schema_base {
  ermia::OrderedIndex *index;
  ermia::TableDescriptor *td;
  ermia::TableDescriptor *old_td;
  uint64_t state;
  uint64_t old_v;
#ifdef LAZYDDL
  ermia::OrderedIndex *old_index;
  ermia::TableDescriptor *old_tds[16];
#endif
};
