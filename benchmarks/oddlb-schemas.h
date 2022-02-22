#pragma once

#include "record/encoder.h"
#include "record/inline_str.h"

#define ODDLB_KEY_FIELDS(x, y) x(uint64_t, o_key)
#define ODDLB_VALUE_1_FIELDS(x, y) \
  x(uint64_t, o_value_version) y(uint64_t, o_value_a) y(uint64_t, o_value_b)
DO_STRUCT(oddlb_kv_1, ODDLB_KEY_FIELDS, ODDLB_VALUE_1_FIELDS);

#define ODDLB_VALUE_2_FIELDS(x, y)                                           \
  x(uint64_t, o_value_version) y(uint64_t, o_value_a) y(uint64_t, o_value_b) \
      y(uint64_t, o_value_c)
DO_STRUCT(oddlb_kv_2, ODDLB_KEY_FIELDS, ODDLB_VALUE_2_FIELDS);

#define ODDLB_VALUE_3_FIELDS(x, y)                                           \
  x(uint64_t, o_value_version) y(uint64_t, o_value_a) y(uint64_t, o_value_b) \
      y(uint64_t, o_value_c) y(uint64_t, o_value_d)
DO_STRUCT(oddlb_kv_3, ODDLB_KEY_FIELDS, ODDLB_VALUE_3_FIELDS);

#define ODDLB_VALUE_4_FIELDS(x, y)                                           \
  x(uint64_t, o_value_version) y(uint64_t, o_value_a) y(uint64_t, o_value_b) \
      y(uint64_t, o_value_c) y(uint64_t, o_value_d) y(uint64_t, o_value_e)
DO_STRUCT(oddlb_kv_4, ODDLB_KEY_FIELDS, ODDLB_VALUE_4_FIELDS);

#define ODDLB_VALUE_5_FIELDS(x, y)                                           \
  x(uint64_t, o_value_version) y(uint64_t, o_value_a) y(uint64_t, o_value_b) \
      y(uint64_t, o_value_c) y(uint64_t, o_value_d) y(uint64_t, o_value_e)   \
          y(uint64_t, o_value_f)
DO_STRUCT(oddlb_kv_5, ODDLB_KEY_FIELDS, ODDLB_VALUE_5_FIELDS);

#define ODDLB_VALUE_6_FIELDS(x, y)                                           \
  x(uint64_t, o_value_version) y(uint64_t, o_value_a) y(uint64_t, o_value_b) \
      y(uint64_t, o_value_c) y(uint64_t, o_value_d) y(uint64_t, o_value_e)   \
          y(uint64_t, o_value_f) y(uint64_t, o_value_g)
DO_STRUCT(oddlb_kv_6, ODDLB_KEY_FIELDS, ODDLB_VALUE_6_FIELDS);
