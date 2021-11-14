#pragma once

namespace ermia {

struct Schema_base_ {
  uint64_t v;
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

} // namespace ermia
