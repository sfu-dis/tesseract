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

struct Schema3 : public Schema_base_ {
  uint64_t a;
  uint64_t b;
  uint64_t c;
  uint64_t d;
};

struct Schema4 : public Schema_base_ {
  uint64_t a;
  uint64_t b;
  uint64_t c;
  uint64_t d;
  uint64_t e;
};

struct Schema5 : public Schema_base_ {
  uint64_t a;
  uint64_t b;
  uint64_t c;
  uint64_t d;
  uint64_t e;
  uint64_t f;
};

struct Schema6 : public Schema_base_ {
  uint64_t a;
  uint64_t b;
  uint64_t c;
  uint64_t d;
  uint64_t e;
  uint64_t f;
  uint64_t g;
};

} // namespace ermia
