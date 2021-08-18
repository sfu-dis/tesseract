#pragma once

#include "../engine.h"
#include "tpcc.h"

struct Schema_base_ {
  uint64_t v;
};

struct Schema_base : public Schema_base_ {
  // uint64_t v;
  /*std::function<ermia::varstr *(
                  const char *keyp,
                  size_t keylen,
                  const ermia::varstr &value,
                  uint64_t schema_version,
                  ermia::transaction *txn,
                  ermia::str_arena *arena,
                  ermia::OrderedIndex *index)> op;
  */
  // std::function<bool(uint64_t)> op;
};

struct Schema_record : public Schema_base_ {
  ermia::OrderedIndex *index;
  ermia::TableDescriptor *td;
#ifdef LAZYDDL
  ermia::OrderedIndex *old_index;
  ermia::TableDescriptor *old_td;
#endif
  /*std::function<ermia::varstr *(
		  const char *keyp,
		  size_t keylen,
                  const ermia::varstr &value,
                  uint64_t schema_version,
		  ermia::transaction *txn,
		  ermia::str_arena *arena,
                  ermia::OrderedIndex *index)> op;
  */
};

struct Schema1 : public Schema_base_ {
  uint64_t a;
  uint64_t b;
};

struct Schema2 : public Schema_base_ {
  uint64_t a;
  uint64_t b;
  uint64_t c;
};
