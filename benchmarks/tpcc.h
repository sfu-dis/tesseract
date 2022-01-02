#pragma once

#include "record/encoder.h"
#include "record/inline_str.h"
#include "../macros.h"

// These correspond to the their index in the workload desc vector
#define TPCC_CLID_NEW_ORDER 0
#define TPCC_CLID_PAYMENT   1
#define TPCC_CLID_DELIVERY  2

#define CUSTOMER_KEY_FIELDS(x, y) \
  x(int32_t, c_w_id) y(int32_t, c_d_id) y(int32_t, c_id)
#define CUSTOMER_VALUE_FIELDS(x, y)                                           \
  x(int32_t, c_id) y(float, c_discount) y(inline_str_fixed<2>, c_credit)      \
      y(inline_str_8<16>, c_last) y(inline_str_8<16>, c_first) y(             \
          float, c_credit_lim) y(float, c_balance) y(float, c_ytd_payment)    \
          y(int32_t, c_payment_cnt) y(int32_t, c_delivery_cnt)                \
              y(inline_str_8<20>, c_street_1) y(inline_str_8<20>, c_street_2) \
                  y(inline_str_8<20>, c_city) y(inline_str_fixed<2>, c_state) \
                      y(inline_str_fixed<9>, c_zip)                           \
                          y(inline_str_fixed<16>, c_phone)                    \
                              y(uint32_t, c_since)                            \
                                  y(inline_str_fixed<2>, c_middle)            \
                                      y(inline_str_16<500>, c_data)
DO_STRUCT(customer, CUSTOMER_KEY_FIELDS, CUSTOMER_VALUE_FIELDS)

#define CUSTOMER_PRIVATE_VALUE_FIELDS(x, y)                                    \
  x(int32_t, c_id) y(float, c_discount) y(inline_str_fixed<2>, c_credit)       \
      y(float, c_credit_lim) y(float, c_balance) y(float, c_ytd_payment)       \
          y(int32_t, c_payment_cnt) y(int32_t, c_delivery_cnt)                 \
              y(inline_str_16<500>, c_data)
DO_STRUCT(customer_private, CUSTOMER_KEY_FIELDS, CUSTOMER_PRIVATE_VALUE_FIELDS)

#define CUSTOMER_NAME_IDX_KEY_FIELDS(x, y)                              \
  x(int32_t, c_w_id) y(int32_t, c_d_id) y(inline_str_fixed<16>, c_last) \
      y(inline_str_fixed<16>, c_first)
#define CUSTOMER_NAME_IDX_VALUE_FIELDS(x, y) x(int8_t, dummy)
DO_STRUCT(customer_name_idx, CUSTOMER_NAME_IDX_KEY_FIELDS,
          CUSTOMER_NAME_IDX_VALUE_FIELDS)

#define DISTRICT_KEY_FIELDS(x, y) x(int32_t, d_w_id) y(int32_t, d_id)
#define DISTRICT_VALUE_FIELDS(x, y)                                   \
  x(float, d_ytd) y(float, d_tax) y(int32_t, d_next_o_id)             \
      y(inline_str_8<10>, d_name) y(inline_str_8<20>, d_street_1)     \
          y(inline_str_8<20>, d_street_2) y(inline_str_8<20>, d_city) \
              y(inline_str_fixed<2>, d_state) y(inline_str_fixed<9>, d_zip)
DO_STRUCT(district, DISTRICT_KEY_FIELDS, DISTRICT_VALUE_FIELDS)

#define HISTORY_KEY_FIELDS(x, y)                               \
  x(int32_t, h_c_id) y(int32_t, h_c_d_id) y(int32_t, h_c_w_id) \
      y(int32_t, h_d_id) y(int32_t, h_w_id) y(uint32_t, h_date)
#define HISTORY_VALUE_FIELDS(x, y) \
  x(float, h_amount) y(inline_str_8<24>, h_data)
DO_STRUCT(history, HISTORY_KEY_FIELDS, HISTORY_VALUE_FIELDS)

#define ITEM_KEY_FIELDS(x, y) x(int32_t, i_id)
#define ITEM_VALUE_FIELDS(x, y)                                             \
  x(inline_str_8<24>, i_name) y(float, i_price) y(inline_str_8<50>, i_data) \
      y(int32_t, i_im_id)
DO_STRUCT(item, ITEM_KEY_FIELDS, ITEM_VALUE_FIELDS)

#define NEW_ORDER_KEY_FIELDS(x, y) \
  x(int32_t, no_w_id) y(int32_t, no_d_id) y(int32_t, no_o_id)
// need dummy b/c our btree cannot have empty values.
// we also size value so that it can fit a key
#define NEW_ORDER_VALUE_FIELDS(x, y) x(inline_str_fixed<12>, no_dummy)
DO_STRUCT(new_order, NEW_ORDER_KEY_FIELDS, NEW_ORDER_VALUE_FIELDS)

#define OORDER_KEY_FIELDS(x, y) \
  x(int32_t, o_w_id) y(int32_t, o_d_id) y(int32_t, o_id)
#define OORDER_VALUE_FIELDS(x, y)                                 \
  x(int32_t, o_c_id) y(int32_t, o_carrier_id) y(int8_t, o_ol_cnt) \
      y(bool, o_all_local) y(uint32_t, o_entry_d)
DO_STRUCT(oorder, OORDER_KEY_FIELDS, OORDER_VALUE_FIELDS)

#define OORDER_VALUE_PRECOMPUTE_AGGREGATE_FIELDS(x, y)                                 \
  x(int32_t, o_c_id) y(int32_t, o_carrier_id) y(int8_t, o_ol_cnt) \
      y(bool, o_all_local) y(uint32_t, o_entry_d) y(float, o_total_amount)
DO_STRUCT(oorder_precompute_aggregate, OORDER_KEY_FIELDS, OORDER_VALUE_PRECOMPUTE_AGGREGATE_FIELDS)

#define OORDER_C_ID_IDX_KEY_FIELDS(x, y) \
  x(int32_t, o_w_id) y(int32_t, o_d_id) y(int32_t, o_c_id) y(int32_t, o_o_id)
#define OORDER_C_ID_IDX_VALUE_FIELDS(x, y) x(uint8_t, dummy)
DO_STRUCT(oorder_c_id_idx, OORDER_C_ID_IDX_KEY_FIELDS,
          OORDER_C_ID_IDX_VALUE_FIELDS)

#define ORDER_LINE_KEY_FIELDS(x, y)                           \
  x(int32_t, ol_w_id) y(int32_t, ol_d_id) y(int32_t, ol_o_id) \
      y(int32_t, ol_number)
#define ORDER_LINE_VALUE_FIELDS(x, y)                                \
  x(int32_t, ol_i_id) y(uint32_t, ol_delivery_d) y(float, ol_amount) \
      y(int32_t, ol_supply_w_id) y(int8_t, ol_quantity) y(uint64_t, v)
DO_STRUCT(order_line, ORDER_LINE_KEY_FIELDS, ORDER_LINE_VALUE_FIELDS)

#define ORDER_LINE_VALUE_1_FIELDS(x, y)                                \
  x(int32_t, ol_i_id) y(uint32_t, ol_delivery_d) y(float, ol_amount) \
      y(int32_t, ol_supply_w_id) y(int8_t, ol_quantity) y(uint64_t, v) y(float, ol_tax)
DO_STRUCT(order_line_1, ORDER_LINE_KEY_FIELDS, ORDER_LINE_VALUE_1_FIELDS)

#define STOCK_KEY_FIELDS(x, y) x(int32_t, s_w_id) y(int32_t, s_i_id)
#define STOCK_VALUE_FIELDS(x, y)                                 \
  x(int16_t, s_quantity) y(float, s_ytd) y(int32_t, s_order_cnt) \
      y(int32_t, s_remote_cnt)
DO_STRUCT(stock, STOCK_KEY_FIELDS, STOCK_VALUE_FIELDS)

#define STOCK_DATA_KEY_FIELDS(x, y) x(int32_t, s_w_id) y(int32_t, s_i_id)
#define STOCK_DATA_VALUE_FIELDS(x, y)                                       \
  x(inline_str_8<50>, s_data) y(inline_str_fixed<24>, s_dist_01)            \
      y(inline_str_fixed<24>, s_dist_02) y(inline_str_fixed<24>, s_dist_03) \
          y(inline_str_fixed<24>, s_dist_04)                                \
              y(inline_str_fixed<24>, s_dist_05)                            \
                  y(inline_str_fixed<24>, s_dist_06)                        \
                      y(inline_str_fixed<24>, s_dist_07)                    \
                          y(inline_str_fixed<24>, s_dist_08)                \
                              y(inline_str_fixed<24>, s_dist_09)            \
                                  y(inline_str_fixed<24>, s_dist_10)
DO_STRUCT(stock_data, STOCK_DATA_KEY_FIELDS, STOCK_DATA_VALUE_FIELDS)

#define WAREHOUSE_KEY_FIELDS(x, y) x(int32_t, w_id)
#define WAREHOUSE_VALUE_FIELDS(x, y)                                  \
  x(float, w_ytd) y(float, w_tax) y(inline_str_8<10>, w_name)         \
      y(inline_str_8<20>, w_street_1) y(inline_str_8<20>, w_street_2) \
          y(inline_str_8<20>, w_city) y(inline_str_fixed<2>, w_state) \
              y(inline_str_fixed<9>, w_zip)
DO_STRUCT(warehouse, WAREHOUSE_KEY_FIELDS, WAREHOUSE_VALUE_FIELDS)

#define NATION_KEY_FIELDS(x, y) x(int32_t, n_nationkey)
#define NATION_VALUE_FIELDS(x, y)                         \
  x(inline_str_fixed<25>, n_name) y(int32_t, n_regionkey) \
      y(inline_str_fixed<152>, n_comment)
DO_STRUCT(nation, NATION_KEY_FIELDS, NATION_VALUE_FIELDS)

#define REGION_KEY_FIELDS(x, y) x(int32_t, r_regionkey)
#define REGION_VALUE_FIELDS(x, y) \
  x(inline_str_fixed<25>, r_name) y(inline_str_fixed<152>, r_comment)
DO_STRUCT(region, REGION_KEY_FIELDS, REGION_VALUE_FIELDS)

#define SUPPLIER_KEY_FIELDS(x, y) x(int32_t, su_suppkey)
#define SUPPLIER_VALUE_FIELDS(x, y)                                    \
  x(inline_str_fixed<25>, su_name) y(inline_str_fixed<40>, su_address) \
      y(int32_t, su_nationkey) y(inline_str_fixed<15>, su_phone)       \
          y(int32_t, su_acctbal) y(inline_str_fixed<40>, su_comment)
DO_STRUCT(supplier, SUPPLIER_KEY_FIELDS, SUPPLIER_VALUE_FIELDS)

#define TPCC_TABLE_LIST(x)                                                     \
  x(customer) x(customer_name_idx) x(district) x(history) x(item) x(new_order) \
      x(oorder) x(oorder_c_id_idx) x(order_line) x(stock) x(stock_data)        \
          x(nation) x(region) x(supplier) x(warehouse)

struct _dummy {};  // exists so we can inherit from it, so we can use a macro in
                   // an init list...
