#pragma once

#include "../macros.h"
#include "MEESUT.h"
#include "egen/CETxnInputGenerator.h"
#include "egen/DM.h"
#include "egen/EGenLoader_stdafx.h"
#include "egen/EGenStandardTypes.h"
#include "egen/EGenTables_stdafx.h"
#include "egen/MEE.h"
#include "egen/Table_Defs.h"
#include "egen/TxnHarnessBrokerVolume.h"
#include "egen/TxnHarnessCustomerPosition.h"
#include "egen/TxnHarnessDBInterface.h"
#include "egen/TxnHarnessDataMaintenance.h"
#include "egen/TxnHarnessMarketFeed.h"
#include "egen/TxnHarnessMarketWatch.h"
#include "egen/TxnHarnessSecurityDetail.h"
#include "egen/TxnHarnessSendToMarketInterface.h"
#include "egen/TxnHarnessStructs.h"
#include "egen/TxnHarnessTradeCleanup.h"
#include "egen/TxnHarnessTradeLookup.h"
#include "egen/TxnHarnessTradeOrder.h"
#include "egen/TxnHarnessTradeResult.h"
#include "egen/TxnHarnessTradeStatus.h"
#include "egen/TxnHarnessTradeUpdate.h"
#include "egen/shore_tpce_egen.h"
#include "record/encoder.h"
#include "record/inline_str.h"

#define MIN_VAL(x) 0
#define MAX_VAL(x) numeric_limits<decltype(x)>::max()

/*
   Customer Tables
   */
#define ACCOUNT_PERMISSION_KEY_FIELDS(x, y) \
  x(int64_t, ap_ca_id) y(inline_str_fixed<20>, ap_tax_id)
#define ACCOUNT_PERMISSION_VALUE_FIELDS(x, y)                       \
  x(inline_str_fixed<4>, ap_acl) y(inline_str_fixed<25>, ap_l_name) \
      y(inline_str_fixed<20>, ap_f_name)
DO_STRUCT(account_permission, ACCOUNT_PERMISSION_KEY_FIELDS,
          ACCOUNT_PERMISSION_VALUE_FIELDS)

#define CUSTOMERS_KEY_FIELDS(x, y) x(int64_t, c_id)
#define CUSTOMERS_VALUE_FIELDS(x, y)                                           \
  x(inline_str_fixed<20>, c_tax_id) y(inline_str_fixed<4>, c_st_id) y(         \
      inline_str_fixed<25>, c_l_name) y(inline_str_fixed<20>, c_f_name)        \
      y(inline_str_fixed<1>, c_m_name) y(char, c_gndr) y(char, c_tier) y(      \
          int32_t, c_dob) y(int64_t, c_ad_id) y(inline_str_fixed<3>, c_ctry_1) \
          y(inline_str_fixed<3>, c_area_1) y(inline_str_fixed<10>, c_local_1)  \
              y(inline_str_fixed<5>, c_ext_1) y(inline_str_fixed<3>, c_ctry_2) \
                  y(inline_str_fixed<3>, c_area_2)                             \
                      y(inline_str_fixed<10>, c_local_2)                       \
                          y(inline_str_fixed<5>, c_ext_2)                      \
                              y(inline_str_fixed<3>, c_ctry_3)                 \
                                  y(inline_str_fixed<3>, c_area_3) y(          \
                                      inline_str_fixed<10>, c_local_3)         \
                                      y(inline_str_fixed<5>, c_ext_3) y(       \
                                          inline_str_fixed<50>, c_email_1)     \
                                          y(inline_str_fixed<50>, c_email_2)
DO_STRUCT(customers, CUSTOMERS_KEY_FIELDS,
          CUSTOMERS_VALUE_FIELDS)  // XXX. MUST avoid use of same name with
                                   // TPCC, otherwise one of the customer table
                                   // definitions is overwritten

#define C_TAX_ID_INDEX_KEY_FIELDS(x, y) \
  x(inline_str_fixed<20>, c_tax_id) y(int64_t, c_id)
#define C_TAX_ID_INDEX_VALUE_FIELDS(x, y) x(bool, dummy)
DO_STRUCT(c_tax_id_index, C_TAX_ID_INDEX_KEY_FIELDS,
          C_TAX_ID_INDEX_VALUE_FIELDS)

#define ASSETS_HISTORY_KEY_FIELDS(x, y) x(int64_t, ah_id)
#define ASSETS_HISTORY_VALUE_FIELDS(x, y) \
  x(int64_t, start_ca_id) y(int64_t, end_ca_id) y(double, total_assets)
DO_STRUCT(assets_history, ASSETS_HISTORY_KEY_FIELDS,
          ASSETS_HISTORY_VALUE_FIELDS)

#define CUSTOMER_ACCOUNT_KEY_FIELDS(x, y) x(int64_t, ca_id)
#define CUSTOMER_ACCOUNT_VALUE_FIELDS(x, y)                                \
  x(int64_t, ca_b_id) y(int64_t, ca_c_id) y(inline_str_fixed<50>, ca_name) \
      y(int16_t, ca_tax_st) y(double, ca_bal)
DO_STRUCT(customer_account, CUSTOMER_ACCOUNT_KEY_FIELDS,
          CUSTOMER_ACCOUNT_VALUE_FIELDS)

#define CA_ID_INDEX_KEY_FIELDS(x, y) x(int64_t, ca_c_id) y(int64_t, ca_id)
#define CA_ID_INDEX_VALUE_FIELDS(x, y) x(bool, dummy)
DO_STRUCT(ca_id_index, CA_ID_INDEX_KEY_FIELDS, CA_ID_INDEX_VALUE_FIELDS)

#define CUSTOMER_TAXRATE_KEY_FIELDS(x, y) \
  x(int64_t, cx_c_id) y(inline_str_fixed<4>, cx_tx_id)
#define CUSTOMER_TAXRATE_VALUE_FIELDS(x, y) x(bool, dummy)
DO_STRUCT(customer_taxrate, CUSTOMER_TAXRATE_KEY_FIELDS,
          CUSTOMER_TAXRATE_VALUE_FIELDS)

#define HOLDING_KEY_FIELDS(x, y)                                           \
  x(int64_t, h_ca_id) y(inline_str_fixed<15>, h_s_symb) y(uint64_t, h_dts) \
      y(uint64_t, h_t_id)
#define HOLDING_VALUE_FIELDS(x, y) \
  x(double, h_price) y(int32_t, h_qty) y(inline_str_fixed<23>, dummy)
DO_STRUCT(holding, HOLDING_KEY_FIELDS, HOLDING_VALUE_FIELDS)

#define HOLDING_HISTORY_KEY_FIELDS(x, y) \
  x(int64_t, hh_t_id) y(int64_t, hh_h_t_id)
#define HOLDING_HISTORY_VALUE_FIELDS(x, y) \
  x(int32_t, hh_before_qty) y(int32_t, hh_after_qty)
DO_STRUCT(holding_history, HOLDING_HISTORY_KEY_FIELDS,
          HOLDING_HISTORY_VALUE_FIELDS)

#define HOLDING_SUMMARY_KEY_FIELDS(x, y) \
  x(int64_t, hs_ca_id) y(inline_str_fixed<15>, hs_s_symb)
#define HOLDING_SUMMARY_VALUE_FIELDS(x, y) \
  x(int32_t, hs_qty) y(inline_str_fixed<19>, dummy)
DO_STRUCT(holding_summary, HOLDING_SUMMARY_KEY_FIELDS,
          HOLDING_SUMMARY_VALUE_FIELDS)

#define WATCH_ITEM_KEY_FIELDS(x, y) \
  x(int64_t, wi_wl_id) y(inline_str_fixed<15>, wi_s_symb)
#define WATCH_ITEM_VALUE_FIELDS(x, y) x(bool, dummy)
DO_STRUCT(watch_item, WATCH_ITEM_KEY_FIELDS, WATCH_ITEM_VALUE_FIELDS)

// added wl_c_id to PK, read-only table. ( no FK integrity issue )
#define WATCH_LIST_KEY_FIELDS(x, y) x(int64_t, wl_c_id) y(int64_t, wl_id)
#define WATCH_LIST_VALUE_FIELDS(x, y) x(bool, dummy)
DO_STRUCT(watch_list, WATCH_LIST_KEY_FIELDS, WATCH_LIST_VALUE_FIELDS)
/*
 */

/*
   Broker Tables
   */
#define BROKER_KEY_FIELDS(x, y) x(int64_t, b_id)
#define BROKER_VALUE_FIELDS(x, y)                                 \
  x(inline_str_fixed<4>, b_st_id) y(inline_str_fixed<49>, b_name) \
      y(int32_t, b_num_trades) y(double, b_comm_total)
DO_STRUCT(broker, BROKER_KEY_FIELDS, BROKER_VALUE_FIELDS)

#define B_NAME_INDEX_KEY_FIELDS(x, y) \
  x(inline_str_fixed<49>, b_name) y(int64_t, b_id)
#define B_NAME_INDEX_VALUE_FIELDS(x, y) x(bool, dummy)
DO_STRUCT(b_name_index, B_NAME_INDEX_KEY_FIELDS, B_NAME_INDEX_VALUE_FIELDS)

#define CASH_TRANSACTION_KEY_FIELDS(x, y) x(int64_t, ct_t_id)
#define CASH_TRANSACTION_VALUE_FIELDS(x, y) \
  x(uint64_t, ct_dts) y(float, ct_amt) y(inline_str_fixed<100>, ct_name)
DO_STRUCT(cash_transaction, CASH_TRANSACTION_KEY_FIELDS,
          CASH_TRANSACTION_VALUE_FIELDS)

#define CHARGE_KEY_FIELDS(x, y) \
  x(inline_str_fixed<3>, ch_tt_id) y(int32_t, ch_c_tier)
#define CHARGE_VALUE_FIELDS(x, y) x(double, ch_chrg)
DO_STRUCT(charge, CHARGE_KEY_FIELDS, CHARGE_VALUE_FIELDS)

#define COMMISSION_RATE_KEY_FIELDS(x, y)                 \
  x(int32_t, cr_c_tier) y(inline_str_fixed<3>, cr_tt_id) \
      y(inline_str_fixed<6>, cr_ex_id) y(int32_t, cr_from_qty)
#define COMMISSION_RATE_VALUE_FIELDS(x, y) \
  x(int32_t, cr_to_qty) y(double, cr_rate)
DO_STRUCT(commission_rate, COMMISSION_RATE_KEY_FIELDS,
          COMMISSION_RATE_VALUE_FIELDS)

#define SETTLEMENT_KEY_FIELDS(x, y) x(int64_t, se_t_id)
#define SETTLEMENT_VALUE_FIELDS(x, y)                                 \
  x(inline_str_fixed<40>, se_cash_type) y(uint64_t, se_cash_due_date) \
      y(double, se_amt)
DO_STRUCT(settlement, SETTLEMENT_KEY_FIELDS, SETTLEMENT_VALUE_FIELDS)

#define TRADE_KEY_FIELDS(x, y) x(int64_t, t_id)
#define TRADE_VALUE_FIELDS(x, y)                                 \
  x(uint64_t, t_dts) y(inline_str_fixed<4>, t_st_id)             \
      y(inline_str_fixed<3>, t_tt_id) y(bool, t_is_cash)         \
          y(inline_str_fixed<15>, t_s_symb) y(int32_t, t_qty)    \
              y(double, t_bid_price) y(int64_t, t_ca_id)         \
                  y(inline_str_fixed<49>, t_exec_name)           \
                      y(double, t_trade_price) y(double, t_chrg) \
                          y(double, t_comm) y(double, t_tax) y(bool, t_lifo)
DO_STRUCT(trade, TRADE_KEY_FIELDS, TRADE_VALUE_FIELDS)

#define T_CA_ID_INDEX_KEY_FIELDS(x, y) \
  x(int64_t, t_ca_id) y(uint64_t, t_dts) y(int64_t, t_id)
#define T_CA_ID_INDEX_VALUE_FIELDS(x, y)                                     \
  x(inline_str_fixed<4>, t_st_id) y(inline_str_fixed<3>, t_tt_id)            \
      y(bool, t_is_cash) y(inline_str_fixed<15>, t_s_symb) y(int32_t, t_qty) \
          y(double, t_bid_price) y(inline_str_fixed<49>, t_exec_name)        \
              y(double, t_trade_price) y(double, t_chrg)
DO_STRUCT(t_ca_id_index, T_CA_ID_INDEX_KEY_FIELDS, T_CA_ID_INDEX_VALUE_FIELDS)

#define T_S_SYMB_INDEX_KEY_FIELDS(x, y) \
  x(inline_str_fixed<15>, t_s_symb) y(uint64_t, t_dts) y(int64_t, t_id)
#define T_S_SYMB_INDEX_VALUE_FIELDS(x, y)                                  \
  x(int64_t, t_ca_id) y(inline_str_fixed<4>, t_st_id)                      \
      y(inline_str_fixed<3>, t_tt_id) y(bool, t_is_cash) y(int32_t, t_qty) \
          y(inline_str_fixed<49>, t_exec_name) y(double, t_trade_price)
DO_STRUCT(t_s_symb_index, T_S_SYMB_INDEX_KEY_FIELDS,
          T_S_SYMB_INDEX_VALUE_FIELDS)

// Added th_dts to PK, queries usually require sorting on dts column.
#define TRADE_HISTORY_KEY_FIELDS(x, y) \
  x(int64_t, th_t_id) y(inline_str_fixed<4>, th_st_id) y(uint64_t, th_dts)
#define TRADE_HISTORY_VALUE_FIELDS(x, y) x(bool, dummy)
DO_STRUCT(trade_history, TRADE_HISTORY_KEY_FIELDS, TRADE_HISTORY_VALUE_FIELDS)

#define TRADE_REQUEST_KEY_FIELDS(x, y) \
  x(inline_str_fixed<15>, tr_s_symb) y(uint64_t, tr_b_id) y(uint64_t, tr_t_id)
#define TRADE_REQUEST_VALUE_FIELDS(x, y)                                      \
  x(inline_str_fixed<3>, tr_tt_id) y(int32_t, tr_qty) y(double, tr_bid_price) \
      y(inline_str_fixed<16>, dummy)
DO_STRUCT(trade_request, TRADE_REQUEST_KEY_FIELDS, TRADE_REQUEST_VALUE_FIELDS)

#define TRADE_TYPE_KEY_FIELDS(x, y) x(inline_str_fixed<3>, tt_id)
#define TRADE_TYPE_VALUE_FIELDS(x, y) \
  x(inline_str_fixed<12>, tt_name) y(bool, tt_is_sell) y(bool, tt_is_mrkt)
DO_STRUCT(trade_type, TRADE_TYPE_KEY_FIELDS, TRADE_TYPE_VALUE_FIELDS)

/*
   Market Tables
   */
#define COMPANY_KEY_FIELDS(x, y) x(int64_t, co_id)
#define COMPANY_VALUE_FIELDS(x, y)                                        \
  x(inline_str_fixed<4>, co_st_id) y(inline_str_fixed<60>, co_name)       \
      y(inline_str_fixed<2>, co_in_id) y(inline_str_fixed<4>, co_sp_rate) \
          y(inline_str_fixed<46>, co_ceo) y(int64_t, co_ad_id)            \
              y(inline_str_fixed<150>, co_desc) y(uint64_t, co_open_date)
DO_STRUCT(company, COMPANY_KEY_FIELDS, COMPANY_VALUE_FIELDS)

#define CO_IN_ID_INDEX_KEY_FIELDS(x, y) \
  x(inline_str_fixed<2>, co_in_id) y(int64_t, co_id)
#define CO_IN_ID_INDEX_VALUE_FIELDS(x, y) x(bool, dummy)
DO_STRUCT(co_in_id_index, CO_IN_ID_INDEX_KEY_FIELDS,
          CO_IN_ID_INDEX_VALUE_FIELDS)

#define CO_NAME_INDEX_KEY_FIELDS(x, y) \
  x(inline_str_fixed<60>, co_name) y(int64_t, co_id)
#define CO_NAME_INDEX_VALUE_FIELDS(x, y) x(bool, dummy)
DO_STRUCT(co_name_index, CO_NAME_INDEX_KEY_FIELDS, CO_NAME_INDEX_VALUE_FIELDS)

#define COMPANY_COMPETITOR_KEY_FIELDS(x, y)      \
  x(int64_t, cp_co_id) y(int64_t, cp_comp_co_id) \
      y(inline_str_fixed<2>, cp_in_id)
#define COMPANY_COMPETITOR_VALUE_FIELDS(x, y) x(bool, dummy)
DO_STRUCT(company_competitor, COMPANY_COMPETITOR_KEY_FIELDS,
          COMPANY_COMPETITOR_VALUE_FIELDS)

#define DAILY_MARKET_KEY_FIELDS(x, y) \
  x(inline_str_fixed<15>, dm_s_symb) y(uint64_t, dm_date)
#define DAILY_MARKET_VALUE_FIELDS(x, y) \
  x(double, dm_close) y(double, dm_high) y(double, dm_low) y(int64_t, dm_vol)
DO_STRUCT(daily_market, DAILY_MARKET_KEY_FIELDS, DAILY_MARKET_VALUE_FIELDS)

#define EXCHANGE_KEY_FIELDS(x, y) x(inline_str_fixed<6>, ex_id)
#define EXCHANGE_VALUE_FIELDS(x, y)                         \
  x(inline_str_fixed<100>, ex_name) y(int32_t, ex_num_symb) \
      y(int32_t, ex_open) y(int32_t, ex_close)              \
          y(inline_str_fixed<150>, ex_desc) y(int64_t, ex_ad_id)
DO_STRUCT(exchange, EXCHANGE_KEY_FIELDS, EXCHANGE_VALUE_FIELDS)

#define FINANCIAL_KEY_FIELDS(x, y) \
  x(int64_t, fi_co_id) y(int32_t, fi_year) y(int32_t, fi_qtr)
#define FINANCIAL_VALUE_FIELDS(x, y)                                           \
  x(uint64_t, fi_qtr_start_date) y(double, fi_revenue) y(double, fi_net_earn)  \
      y(double, fi_basic_eps) y(double, fi_dilut_eps) y(double, fi_margin)     \
          y(double, fi_inventory) y(double, fi_assets) y(double, fi_liability) \
              y(int64_t, fi_out_basic) y(int64_t, fi_out_dilut)
DO_STRUCT(financial, FINANCIAL_KEY_FIELDS, FINANCIAL_VALUE_FIELDS)

#define INDUSTRY_KEY_FIELDS(x, y) x(inline_str_fixed<2>, in_id)
#define INDUSTRY_VALUE_FIELDS(x, y) \
  x(inline_str_fixed<50>, in_name) y(inline_str_fixed<2>, in_sc_id)
DO_STRUCT(industry, INDUSTRY_KEY_FIELDS, INDUSTRY_VALUE_FIELDS)

#define IN_NAME_INDEX_KEY_FIELDS(x, y) \
  x(inline_str_fixed<50>, in_name) y(inline_str_fixed<2>, in_id)
#define IN_NAME_INDEX_VALUE_FIELDS(x, y) x(bool, dummy)
DO_STRUCT(in_name_index, IN_NAME_INDEX_KEY_FIELDS, IN_NAME_INDEX_VALUE_FIELDS)

#define IN_SC_ID_INDEX_KEY_FIELDS(x, y) \
  x(inline_str_fixed<2>, in_sc_id) y(inline_str_fixed<2>, in_id)
#define IN_SC_ID_INDEX_VALUE_FIELDS(x, y) x(bool, dummy)
DO_STRUCT(in_sc_id_index, IN_SC_ID_INDEX_KEY_FIELDS,
          IN_SC_ID_INDEX_VALUE_FIELDS)

#define LAST_TRADE_KEY_FIELDS(x, y) x(inline_str_fixed<15>, lt_s_symb)
#define LAST_TRADE_VALUE_FIELDS(x, y)                              \
  x(uint64_t, lt_dts) y(double, lt_price) y(double, lt_open_price) \
      y(int64_t, lt_vol)
DO_STRUCT(last_trade, LAST_TRADE_KEY_FIELDS, LAST_TRADE_VALUE_FIELDS)

#define NEWS_ITEM_KEY_FIELDS(x, y) x(int64_t, ni_id)

// FIXME. ni_item size should be 100K, but adler copy incurs SIGSEGV with the
// 100K size.
#define NEWS_ITEM_VALUE_FIELDS(x, y)                                        \
  x(inline_str_fixed<80>, ni_headline) y(inline_str_fixed<255>, ni_summary) \
      y(inline_str_fixed<1000>, ni_item) y(uint64_t, ni_dts)                \
          y(inline_str_fixed<30>, ni_source)                                \
              y(inline_str_fixed<30>, ni_author)
DO_STRUCT(news_item, NEWS_ITEM_KEY_FIELDS, NEWS_ITEM_VALUE_FIELDS)

#define NEWS_XREF_KEY_FIELDS(x, y) x(int64_t, nx_co_id) y(int64_t, nx_ni_id)
#define NEWS_XREF_VALUE_FIELDS(x, y) x(bool, dummy)
DO_STRUCT(news_xref, NEWS_XREF_KEY_FIELDS, NEWS_XREF_VALUE_FIELDS)

// Added sc_name to PK
#define SECTOR_KEY_FIELDS(x, y) \
  x(inline_str_fixed<30>, sc_name) y(inline_str_fixed<2>, sc_id)
#define SECTOR_VALUE_FIELDS(x, y) x(bool, dummy)
DO_STRUCT(sector, SECTOR_KEY_FIELDS, SECTOR_VALUE_FIELDS)

#define SECURITY_KEY_FIELDS(x, y) x(inline_str_fixed<15>, s_symb)
#define SECURITY_VALUE_FIELDS(x, y)                                           \
  x(inline_str_fixed<6>, s_issue) y(inline_str_fixed<4>, s_st_id)             \
      y(inline_str_fixed<70>, s_name) y(inline_str_fixed<6>, s_ex_id)         \
          y(int64_t, s_co_id) y(int64_t, s_num_out) y(uint64_t, s_start_date) \
              y(uint64_t, s_exch_date) y(double, s_pe) y(float, s_52wk_high)  \
                  y(uint64_t, s_52wk_high_date) y(float, s_52wk_low)          \
                      y(uint64_t, s_52wk_low_date) y(double, s_dividend)      \
                          y(double, s_yield)
DO_STRUCT(security, SECURITY_KEY_FIELDS, SECURITY_VALUE_FIELDS)

#define SECURITY_INDEX_KEY_FIELDS(x, y)               \
  x(int64_t, s_co_id) y(inline_str_fixed<6>, s_issue) \
      y(inline_str_fixed<15>, s_symb)
#define SECURITY_INDEX_VALUE_FIELDS(x, y) x(bool, dummy)
DO_STRUCT(security_index, SECURITY_INDEX_KEY_FIELDS,
          SECURITY_INDEX_VALUE_FIELDS)

/*
   Dimension Tables
   */

#define ADDRESS_KEY_FIELDS(x, y) x(int64_t, ad_id)
#define ADDRESS_VALUE_FIELDS(x, y)                                    \
  x(inline_str_fixed<80>, ad_line1) y(inline_str_fixed<80>, ad_line2) \
      y(inline_str_fixed<12>, ad_zc_code) y(inline_str_fixed<80>, ad_ctry)
DO_STRUCT(address, ADDRESS_KEY_FIELDS, ADDRESS_VALUE_FIELDS)

#define STATUS_TYPE_KEY_FIELDS(x, y) x(inline_str_fixed<4>, st_id)
#define STATUS_TYPE_VALUE_FIELDS(x, y) x(inline_str_fixed<10>, st_name)
DO_STRUCT(status_type, STATUS_TYPE_KEY_FIELDS, STATUS_TYPE_VALUE_FIELDS)

#define TAX_RATE_KEY_FIELDS(x, y) x(inline_str_fixed<4>, tx_id)
#define TAX_RATE_VALUE_FIELDS(x, y) \
  x(inline_str_fixed<50>, tx_name) y(double, tx_rate)
DO_STRUCT(tax_rate, TAX_RATE_KEY_FIELDS, TAX_RATE_VALUE_FIELDS)

#define ZIP_CODE_KEY_FIELDS(x, y) x(inline_str_fixed<12>, zc_code)
#define ZIP_CODE_VALUE_FIELDS(x, y) \
  x(inline_str_fixed<80>, zc_town) y(inline_str_fixed<80>, zc_div)
DO_STRUCT(zip_code, ZIP_CODE_KEY_FIELDS, ZIP_CODE_VALUE_FIELDS)

#define TPCE_TABLE_LIST(x)                                                    \
  x(charge) x(commission_rate) x(exchange) x(industry) x(in_name_index) x(    \
      in_sc_id_index) x(sector) x(status_type) x(tax_rate) x(trade_type)      \
      x(zip_code) x(address) x(customers) x(c_tax_id_index) x(assets_history) \
          x(customer_account) x(ca_id_index) x(account_permission)            \
              x(customer_taxrate) x(watch_list) x(watch_item) x(company)      \
                  x(co_in_id_index) x(co_name_index) x(company_competitor)    \
                      x(daily_market) x(financial) x(last_trade) x(news_item) \
                          x(news_xref) x(security) x(security_index) x(trade) \
                              x(t_ca_id_index) x(t_s_symb_index)              \
                                  x(trade_request) x(trade_history)           \
                                      x(settlement) x(cash_transaction)       \
                                          x(broker) x(b_name_index)           \
                                              x(holding_history)              \
                                                  x(holding_summary)          \
                                                      x(holding)
