/**
 * An implementation of TPC-C based off of:
 * https://github.com/oltpbenchmark/oltpbench/tree/master/src/com/oltpbenchmark/benchmarks/tpcc
 */

#ifndef ADV_COROUTINE

#include "tpcc-common.h"

rc_t tpcc_worker::txn_new_order() {
  const uint warehouse_id = pick_wh(r, home_warehouse_id);
  const uint districtID = RandomNumber(r, 1, 10);
  const uint customerID = GetCustomerId(r);
  const uint numItems = RandomNumber(r, 5, 15);
  uint itemIDs[15], supplierWarehouseIDs[15], orderQuantities[15];
  bool allLocal = true;
  for (uint i = 0; i < numItems; i++) {
    itemIDs[i] = GetItemId(r);
    if (likely(g_disable_xpartition_txn || NumWarehouses() == 1 ||
               RandomNumber(r, 1, 100) > g_new_order_remote_item_pct)) {
      supplierWarehouseIDs[i] = warehouse_id;
    } else {
      do {
        supplierWarehouseIDs[i] = RandomNumber(r, 1, NumWarehouses());
      } while (supplierWarehouseIDs[i] == warehouse_id);
      allLocal = false;
    }
    orderQuantities[i] = RandomNumber(r, 1, 10);
  }
  ASSERT(!g_disable_xpartition_txn || allLocal);

  // XXX(stephentu): implement rollback
  //
  // worst case txn profile:
  //   1 customer get
  //   1 warehouse get
  //   1 district get
  //   1 new_order insert
  //   1 district put
  //   1 oorder insert
  //   1 oorder_cid_idx insert
  //   15 times:
  //      1 item get
  //      1 stock get
  //      1 stock put
  //      1 order_line insert
  //
  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 0
  //   max_read_set_size : 15
  //   max_write_set_size : 15
  //   num_txn_contexts : 9
  ermia::transaction *txn =
      db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);

  uint64_t order_line_schema_version = 0;
  uint64_t oorder_schema_version = 0;
  uint64_t customer_schema_version = 0;
  ermia::schema_record *order_line_schema_ptr = nullptr;
  ermia::schema_record *oorder_schema_ptr = nullptr;
  ermia::schema_record *customer_schema_ptr = nullptr;

#ifdef DDL
  // Read schema tables first
  ermia::schema_record order_line_schema;
  order_line_schema_version = tpcc_read_schema(txn, order_line_table_index, order_line_key, order_line_schema);
  order_line_schema_ptr = &order_line_schema;

  ermia::schema_record oorder_schema;
  oorder_schema_version = tpcc_read_schema(txn, oorder_table_index, oorder_key, oorder_schema);
  oorder_schema_ptr = &oorder_schema;

  ermia::schema_record customer_schema;
  customer_schema_version = tpcc_read_schema(txn, customer_table_index, customer_key, customer_schema);
  customer_schema_ptr = &customer_schema;
#endif

  const customer::key k_c(warehouse_id, districtID, customerID);
  ermia::varstr valptr;

#ifdef COPYDDL
  ermia::ConcurrentMasstreeIndex *customer_table_index =
      (ermia::ConcurrentMasstreeIndex *)customer_schema.index;
#endif

  if (customer_schema_version == 0) {
    customer::value v_c_temp;

    rc_t rc = rc_t{RC_INVALID};
    customer_table_index->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c),
                                    valptr, nullptr, customer_schema_ptr);
    // TryVerifyRelaxed(rc);
    TryCatch(rc);

    const customer::value *v_c = Decode(valptr, v_c_temp);
#ifndef NDEBUG
    checker::SanityCheckCustomer(&k_c, v_c);
#endif
  } else {
    customer_private::value v_c_temp;

    rc_t rc = rc_t{RC_INVALID};
    customer_table_index->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c),
                                    valptr, nullptr, customer_schema_ptr);
    // TryVerifyRelaxed(rc);
    TryCatch(rc);

    const customer_private::value *v_c = Decode(valptr, v_c_temp);
  }

  const warehouse::key k_w(warehouse_id);
  warehouse::value v_w_temp;

  rc_t rc = rc_t{RC_INVALID};
  tbl_warehouse(warehouse_id)
      ->GetRecord(txn, rc, Encode(str(Size(k_w)), k_w), valptr);
  TryVerifyRelaxed(rc);

  const warehouse::value *v_w = Decode(valptr, v_w_temp);
#ifndef NDEBUG
  checker::SanityCheckWarehouse(&k_w, v_w);
#endif

  const district::key k_d(warehouse_id, districtID);
  district::value v_d_temp;

  rc = rc_t{RC_INVALID};
  tbl_district(warehouse_id)
      ->GetRecord(txn, rc, Encode(str(Size(k_d)), k_d), valptr);
  TryVerifyRelaxed(rc);

  const district::value *v_d = Decode(valptr, v_d_temp);
#ifndef NDEBUG
  checker::SanityCheckDistrict(&k_d, v_d);
#endif

  const uint64_t my_next_o_id =
      g_new_order_fast_id_gen ? FastNewOrderIdGen(warehouse_id, districtID)
                              : v_d->d_next_o_id;

  const new_order::key k_no(warehouse_id, districtID, my_next_o_id);
  const new_order::value v_no;
  const size_t new_order_sz = Size(v_no);
  TryCatch(tbl_new_order(warehouse_id)
               ->InsertRecord(txn, Encode(str(Size(k_no)), k_no),
                              Encode(str(new_order_sz), v_no)));

  if (!g_new_order_fast_id_gen) {
    district::value v_d_new(*v_d);
    v_d_new.d_next_o_id++;
    TryCatch(tbl_district(warehouse_id)
                 ->UpdateRecord(txn, Encode(str(Size(k_d)), k_d),
                                Encode(str(Size(v_d_new)), v_d_new)));
  }

  float sum = 0.0;
  for (uint ol_number = 1; ol_number <= numItems; ol_number++) {
    const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
    const uint ol_i_id = itemIDs[ol_number - 1];
    const uint ol_quantity = orderQuantities[ol_number - 1];

    const item::key k_i(ol_i_id);
    item::value v_i_temp;

    rc = rc_t{RC_INVALID};
    tbl_item(1)->GetRecord(txn, rc, Encode(str(Size(k_i)), k_i), valptr);
    TryVerifyRelaxed(rc);

    const item::value *v_i = Decode(valptr, v_i_temp);
#ifndef NDEBUG
    checker::SanityCheckItem(&k_i, v_i);
#endif

    const stock::key k_s(ol_supply_w_id, ol_i_id);
    stock::value v_s_temp;

    rc = rc_t{RC_INVALID};
    tbl_stock(ol_supply_w_id)
        ->GetRecord(txn, rc, Encode(str(Size(k_s)), k_s), valptr);
    TryVerifyRelaxed(rc);

    const stock::value *v_s = Decode(valptr, v_s_temp);
#ifndef NDEBUG
    checker::SanityCheckStock(&k_s);
#endif

    stock::value v_s_new(*v_s);
    if (v_s_new.s_quantity - ol_quantity >= 10)
      v_s_new.s_quantity -= ol_quantity;
    else
      v_s_new.s_quantity += -int32_t(ol_quantity) + 91;
    v_s_new.s_ytd += ol_quantity;
    v_s_new.s_remote_cnt += (ol_supply_w_id == warehouse_id) ? 0 : 1;

    TryCatch(tbl_stock(ol_supply_w_id)
                 ->UpdateRecord(txn, Encode(str(Size(k_s)), k_s),
                                Encode(str(Size(v_s_new)), v_s_new)));

#ifdef COPYDDL
    ermia::ConcurrentMasstreeIndex *order_line_table_index =
        (ermia::ConcurrentMasstreeIndex *)order_line_schema.index;
#endif

    if (order_line_schema_version == 0) {
      const order_line::key k_ol(warehouse_id, districtID, k_no.no_o_id,
                                 ol_number);
      order_line::value v_ol;
      v_ol.ol_i_id = int32_t(ol_i_id);
      v_ol.ol_delivery_d = 0;  // not delivered yet
      v_ol.ol_amount = float(ol_quantity) * v_i->i_price;
      v_ol.ol_supply_w_id = int32_t(ol_supply_w_id);
      v_ol.ol_quantity = int8_t(ol_quantity);

      sum += v_ol.ol_amount;

      const size_t order_line_sz = Size(v_ol);
      TryCatch(order_line_table_index->InsertRecord(
          txn, Encode(str(Size(k_ol)), k_ol), Encode(str(order_line_sz), v_ol),
          nullptr, order_line_schema_ptr));
    } else {
      if (ddl_example == 0 || ddl_example == 10) {
        const order_line_1::key k_ol_1(warehouse_id, districtID, k_no.no_o_id,
                                       ol_number);

        order_line_1::value v_ol_1;
        v_ol_1.ol_i_id = int32_t(ol_i_id);
        v_ol_1.ol_delivery_d = 0;  // not delivered yet
        v_ol_1.ol_amount = float(ol_quantity) * v_i->i_price;
        v_ol_1.ol_supply_w_id = int32_t(ol_supply_w_id);
        v_ol_1.ol_quantity = int8_t(ol_quantity);
        v_ol_1.ol_tax = (float)0.1;

        sum += v_ol_1.ol_amount;

        const size_t order_line_sz = Size(v_ol_1);
        TryCatch(order_line_table_index->InsertRecord(
            txn, Encode(str(Size(k_ol_1)), k_ol_1),
            Encode(str(order_line_sz), v_ol_1), nullptr, order_line_schema_ptr));
      } else if (ddl_example == 4) {
        const order_line_stock::key k_ol_stock(warehouse_id, districtID,
                                               k_no.no_o_id, ol_number);

        order_line_stock::value v_ol_stock;
        v_ol_stock.ol_i_id = int32_t(ol_i_id);
        v_ol_stock.ol_delivery_d = 0;  // not delivered yet
        v_ol_stock.ol_amount = float(ol_quantity) * v_i->i_price;
        v_ol_stock.ol_supply_w_id = int32_t(ol_supply_w_id);
        v_ol_stock.ol_quantity = int8_t(ol_quantity);
        v_ol_stock.s_quantity = v_s_new.s_quantity;
        v_ol_stock.s_ytd = v_s_new.s_ytd;
	v_ol_stock.s_order_cnt = v_s_new.s_order_cnt;
	v_ol_stock.s_remote_cnt = v_s_new.s_remote_cnt;

        sum += v_ol_stock.ol_amount;

        const size_t order_line_sz = Size(v_ol_stock);
        TryCatch(order_line_table_index->InsertRecord(
            txn, Encode(str(Size(k_ol_stock)), k_ol_stock),
            Encode(str(order_line_sz), v_ol_stock), nullptr,
            order_line_schema_ptr));
      } else if (ddl_example == 9) {
        const order_line::key k_ol(warehouse_id, districtID, k_no.no_o_id,
                                   ol_number);
        order_line::value v_ol;
        v_ol.ol_i_id = int32_t(ol_i_id);
        v_ol.ol_delivery_d = 0;  // not delivered yet
        v_ol.ol_amount = float(ol_quantity) * v_i->i_price;
        v_ol.ol_supply_w_id = int32_t(ol_supply_w_id);
        v_ol.ol_quantity = int8_t(ol_quantity);

        sum += v_ol.ol_amount;

        const size_t order_line_sz = Size(v_ol);
        TryCatch(order_line_table_index->InsertRecord(
            txn, Encode(str(Size(k_ol)), k_ol), Encode(str(order_line_sz), v_ol),
            nullptr, order_line_schema_ptr));
      }
    }
  }

#ifdef COPYDDL
  ermia::ConcurrentMasstreeIndex *oorder_table_index =
      (ermia::ConcurrentMasstreeIndex *)oorder_schema.index;
#endif

  const oorder::key k_oo(warehouse_id, districtID, k_no.no_o_id);
  if (oorder_schema_version == 0) {
    oorder::value v_oo;
    v_oo.o_c_id = int32_t(customerID);
    v_oo.o_carrier_id = 0;  // seems to be ignored
    v_oo.o_ol_cnt = int8_t(numItems);
    v_oo.o_all_local = allLocal;
    v_oo.o_entry_d = GetCurrentTimeMillis();

    const size_t oorder_sz = Size(v_oo);
    ermia::OID v_oo_oid = 0;  // Get the OID and put it in oorder_c_id_idx later
    TryCatch(oorder_table_index->InsertRecord(
        txn, Encode(str(Size(k_oo)), k_oo), Encode(str(oorder_sz), v_oo),
        &v_oo_oid, oorder_schema_ptr));

    const oorder_c_id_idx::key k_oo_idx(warehouse_id, districtID, customerID,
                                        k_no.no_o_id);
    TryCatch(
        tbl_oorder_c_id_idx(warehouse_id)
            ->InsertOID(txn, Encode(str(Size(k_oo_idx)), k_oo_idx), v_oo_oid));
  } else {
    oorder_precompute_aggregate::value v_oo;
    v_oo.o_c_id = int32_t(customerID);
    v_oo.o_carrier_id = 0;  // seems to be ignored
    v_oo.o_ol_cnt = int8_t(numItems);
    v_oo.o_all_local = allLocal;
    v_oo.o_entry_d = GetCurrentTimeMillis();
    v_oo.o_total_amount = sum;

    const size_t oorder_sz = Size(v_oo);
    ermia::OID v_oo_oid = 0;  // Get the OID and put it in oorder_c_id_idx later
    TryCatch(oorder_table_index->InsertRecord(
        txn, Encode(str(Size(k_oo)), k_oo), Encode(str(oorder_sz), v_oo),
        &v_oo_oid, oorder_schema_ptr));

    const oorder_c_id_idx::key k_oo_idx(warehouse_id, districtID, customerID,
                                        k_no.no_o_id);
#ifdef COPYDDL
    ermia::ConcurrentMasstreeIndex *oorder_table_secondary_index =
        (ermia::ConcurrentMasstreeIndex *)(*(
            (oorder_schema.td)->GetSecIndexes().begin()));
#endif
    TryCatch(oorder_table_secondary_index->InsertOID(
        txn, Encode(str(Size(k_oo_idx)), k_oo_idx), v_oo_oid));
  }

  TryCatch(db->Commit(txn));
  return {RC_TRUE};
}  // new-order

rc_t tpcc_worker::txn_payment() {
  const uint warehouse_id = pick_wh(r, home_warehouse_id);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  uint customerDistrictID, customerWarehouseID;
  if (likely(g_disable_xpartition_txn || NumWarehouses() == 1 ||
             RandomNumber(r, 1, 100) <= 85)) {
    customerDistrictID = districtID;
    customerWarehouseID = warehouse_id;
  } else {
    customerDistrictID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
    do {
      customerWarehouseID = RandomNumber(r, 1, NumWarehouses());
    } while (customerWarehouseID == warehouse_id);
  }
  const float paymentAmount = (float)(RandomNumber(r, 100, 500000) / 100.0);
  const uint32_t ts = GetCurrentTimeMillis();
  ASSERT(!g_disable_xpartition_txn || customerWarehouseID == warehouse_id);

  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 10
  //   max_read_set_size : 71
  //   max_write_set_size : 1
  //   num_txn_contexts : 5
  ermia::transaction *txn =
      db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);

  uint64_t order_line_schema_version = 0;
  uint64_t customer_schema_version = 0;
  ermia::schema_record *order_line_schema_ptr = nullptr;
  ermia::schema_record *customer_schema_ptr = nullptr;

#ifdef DDL
  // Read schema tables first
  ermia::schema_record order_line_schema;
  order_line_schema_version = tpcc_read_schema(txn, order_line_table_index, order_line_key, order_line_schema);
  order_line_schema_ptr = &order_line_schema;

  ermia::schema_record customer_schema;
  customer_schema_version = tpcc_read_schema(txn, customer_table_index, customer_key, customer_schema);
  customer_schema_ptr = &customer_schema;
#endif

#ifdef COPYDDL
  ermia::ConcurrentMasstreeIndex *customer_table_index =
      (ermia::ConcurrentMasstreeIndex *)customer_schema.index;
  ermia::ConcurrentMasstreeIndex *customer_table_secondary_index =
      (ermia::ConcurrentMasstreeIndex *)(*(
          (customer_schema.td)->GetSecIndexes().begin()));
#endif

  ermia::varstr valptr;
  const warehouse::key k_w(warehouse_id);
  warehouse::value v_w_temp;

  rc_t rc = rc_t{RC_INVALID};
  tbl_warehouse(warehouse_id)
      ->GetRecord(txn, rc, Encode(str(Size(k_w)), k_w), valptr);
  TryVerifyRelaxed(rc);

  const warehouse::value *v_w = Decode(valptr, v_w_temp);
#ifndef NDEBUG
  checker::SanityCheckWarehouse(&k_w, v_w);
#endif

  warehouse::value v_w_new(*v_w);
  v_w_new.w_ytd += paymentAmount;
  TryCatch(tbl_warehouse(warehouse_id)
               ->UpdateRecord(txn, Encode(str(Size(k_w)), k_w),
                              Encode(str(Size(v_w_new)), v_w_new)));

  const district::key k_d(warehouse_id, districtID);
  district::value v_d_temp;

  rc = rc_t{RC_INVALID};
  tbl_district(warehouse_id)
      ->GetRecord(txn, rc, Encode(str(Size(k_d)), k_d), valptr);
  TryVerifyRelaxed(rc);

  const district::value *v_d = Decode(valptr, v_d_temp);
#ifndef NDEBUG
  checker::SanityCheckDistrict(&k_d, v_d);
#endif

  district::value v_d_new(*v_d);
  v_d_new.d_ytd += paymentAmount;
  TryCatch(tbl_district(warehouse_id)
               ->UpdateRecord(txn, Encode(str(Size(k_d)), k_d),
                              Encode(str(Size(v_d_new)), v_d_new)));

  customer::key k_c;
  customer::value v_c;
  customer_private::value v_c_private;
  if (RandomNumber(r, 1, 100) <= 60) {
    // cust by name
    uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
    static_assert(sizeof(lastname_buf) == 16, "xx");
    memset(lastname_buf, 0, sizeof(lastname_buf));
    GetNonUniformCustomerLastNameRun(lastname_buf, r);

    static const std::string zeros(16, 0);
    static const std::string ones(16, (char)255);

    customer_name_idx::key k_c_idx_0;
    k_c_idx_0.c_w_id = customerWarehouseID;
    k_c_idx_0.c_d_id = customerDistrictID;
    k_c_idx_0.c_last.assign((const char *)lastname_buf, 16);
    k_c_idx_0.c_first.assign(zeros);

    customer_name_idx::key k_c_idx_1;
    k_c_idx_1.c_w_id = customerWarehouseID;
    k_c_idx_1.c_d_id = customerDistrictID;
    k_c_idx_1.c_last.assign((const char *)lastname_buf, 16);
    k_c_idx_1.c_first.assign(ones);

    static_limit_callback<NMaxCustomerIdxScanElems> c(
        s_arena.get(), true);  // probably a safe bet for now

    if (ermia::config::scan_with_it) {
      auto iter = ermia::ConcurrentMasstree::
          ScanIterator</*IsRerverse=*/false>::factory(
              &customer_table_secondary_index->GetMasstree(),
              txn->GetXIDContext(), Encode(str(Size(k_c_idx_0)), k_c_idx_0),
              &Encode(str(Size(k_c_idx_1)), k_c_idx_1));
      ermia::dbtuple *tuple = nullptr;
      bool more = iter.init_or_next</*IsNext=*/false>();
      while (more) {
        tuple = ermia::oidmgr->oid_get_version(iter.tuple_array(), iter.value(),
                                               txn->GetXIDContext());
        if (tuple) {
          rc = txn->DoTupleRead(tuple, &valptr);
          if (rc._val == RC_TRUE) {
            c.Invoke(iter.key().data(), iter.key().length(), valptr);
          }
        }
        more = iter.init_or_next</*IsNext=*/true>();
      }
    } else {
      TryCatch(customer_table_secondary_index->Scan(
          txn, Encode(str(Size(k_c_idx_0)), k_c_idx_0),
          &Encode(str(Size(k_c_idx_1)), k_c_idx_1), c, customer_schema_ptr));

      if (c.size() <= 0) {
        TryCatch(rc_t{RC_ABORT_USER});
      }
    }

    ALWAYS_ASSERT(c.size() > 0);
    ASSERT(c.size() < NMaxCustomerIdxScanElems);  // we should detect this
    int index = c.size() / 2;
    if (c.size() % 2 == 0) index--;

    if (customer_schema_version == 0) {
      Decode(*c.values[index].second, v_c);
      k_c.c_id = v_c.c_id;
    } else {
      Decode(*c.values[index].second, v_c_private);
      k_c.c_id = v_c_private.c_id;
    }
    k_c.c_w_id = customerWarehouseID;
    k_c.c_d_id = customerDistrictID;
  } else {
    // cust by ID
    const uint customerID = GetCustomerId(r);
    k_c.c_w_id = customerWarehouseID;
    k_c.c_d_id = customerDistrictID;
    k_c.c_id = customerID;
    rc = rc_t{RC_INVALID};
    customer_table_index->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c),
                                    valptr, nullptr, customer_schema_ptr);
    // TryVerifyRelaxed(rc);
    TryCatch(rc);
    if (customer_schema_version == 0) {
      Decode(valptr, v_c);
    } else {
      Decode(valptr, v_c_private);
    }
  }
#ifndef NDEBUG
  // checker::SanityCheckCustomer(&k_c, &v_c);
#endif
  if (customer_schema_version == 0) {
    customer::value v_c_new(v_c);

    v_c_new.c_balance -= paymentAmount;
    v_c_new.c_ytd_payment += paymentAmount;
    v_c_new.c_payment_cnt++;
    if (strncmp(v_c.c_credit.data(), "BC", 2) == 0) {
      char buf[501];
      int n = snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s", k_c.c_id,
                       k_c.c_d_id, k_c.c_w_id, districtID, warehouse_id,
                       paymentAmount, v_c.c_data.c_str());
      v_c_new.c_data.resize_junk(
          std::min(static_cast<size_t>(n), v_c_new.c_data.max_size()));
      memcpy((void *)v_c_new.c_data.data(), &buf[0], v_c_new.c_data.size());
    }

    TryCatch(customer_table_index->UpdateRecord(
        txn, Encode(str(Size(k_c)), k_c), Encode(str(Size(v_c_new)), v_c_new),
        customer_schema_ptr));
  } else {
    customer_private::value v_c_new(v_c_private);

    v_c_new.c_balance -= paymentAmount;
    v_c_new.c_ytd_payment += paymentAmount;
    v_c_new.c_payment_cnt++;
    if (strncmp(v_c_private.c_credit.data(), "BC", 2) == 0) {
      char buf[501];
      int n = snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s", k_c.c_id,
                       k_c.c_d_id, k_c.c_w_id, districtID, warehouse_id,
                       paymentAmount, v_c_private.c_data.c_str());
      v_c_new.c_data.resize_junk(
          std::min(static_cast<size_t>(n), v_c_new.c_data.max_size()));
      memcpy((void *)v_c_new.c_data.data(), &buf[0], v_c_new.c_data.size());
    }

    TryCatch(customer_table_index->UpdateRecord(
        txn, Encode(str(Size(k_c)), k_c), Encode(str(Size(v_c_new)), v_c_new),
        customer_schema_ptr));
  }

  const history::key k_h(k_c.c_d_id, k_c.c_w_id, k_c.c_id, districtID,
                         warehouse_id, ts);
  history::value v_h;
  v_h.h_amount = paymentAmount;
  v_h.h_data.resize_junk(v_h.h_data.max_size());
  int n = snprintf((char *)v_h.h_data.data(), v_h.h_data.max_size() + 1,
                   "%.10s    %.10s", v_w->w_name.c_str(), v_d->d_name.c_str());
  v_h.h_data.resize_junk(
      std::min(static_cast<size_t>(n), v_h.h_data.max_size()));

  TryCatch(tbl_history(warehouse_id)
               ->InsertRecord(txn, Encode(str(Size(k_h)), k_h),
                              Encode(str(Size(v_h)), v_h)));

  TryCatch(db->Commit(txn));
  return {RC_TRUE};
}

rc_t tpcc_worker::txn_delivery() {
  const uint warehouse_id = pick_wh(r, home_warehouse_id);
  const uint o_carrier_id = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  const uint32_t ts = GetCurrentTimeMillis();

  // worst case txn profile:
  //   10 times:
  //     1 new_order scan node
  //     1 oorder get
  //     2 order_line scan nodes
  //     15 order_line puts
  //     1 new_order remove
  //     1 oorder put
  //     1 customer get
  //     1 customer put
  //
  // output from counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 21
  //   max_read_set_size : 133
  //   max_write_set_size : 133
  //   num_txn_contexts : 4
  ermia::transaction *txn =
      db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);

  uint64_t order_line_schema_version = 0;
  uint64_t oorder_schema_version = 0;
  uint64_t customer_schema_version = 0;
  ermia::schema_record *order_line_schema_ptr = nullptr;
  ermia::schema_record *oorder_schema_ptr = nullptr;
  ermia::schema_record *customer_schema_ptr = nullptr;

#ifdef DDL
  // Read schema tables first
  ermia::schema_record order_line_schema;
  order_line_schema_version = tpcc_read_schema(txn, order_line_table_index, order_line_key, order_line_schema);
  order_line_schema_ptr = &order_line_schema;

  ermia::schema_record oorder_schema;
  oorder_schema_version = tpcc_read_schema(txn, oorder_table_index, oorder_key, oorder_schema);
  oorder_schema_ptr = &oorder_schema;

  ermia::schema_record customer_schema;
  customer_schema_version = tpcc_read_schema(txn, customer_table_index, customer_key, customer_schema);
  customer_schema_ptr = &customer_schema;
#endif

#ifdef COPYDDL
  ermia::ConcurrentMasstreeIndex *oorder_table_index =
      (ermia::ConcurrentMasstreeIndex *)oorder_schema.index;
  ermia::ConcurrentMasstreeIndex *customer_table_index =
      (ermia::ConcurrentMasstreeIndex *)customer_schema.index;
#endif

  for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
    const new_order::key k_no_0(warehouse_id, d, last_no_o_ids[d - 1]);
    const new_order::key k_no_1(warehouse_id, d,
                                std::numeric_limits<int32_t>::max());
    new_order_scan_callback new_order_c;
    {
      TryCatch(tbl_new_order(warehouse_id)
                   ->Scan(txn, Encode(str(Size(k_no_0)), k_no_0),
                          &Encode(str(Size(k_no_1)), k_no_1), new_order_c));
    }

    const new_order::key *k_no = new_order_c.get_key();
    if (unlikely(!k_no)) continue;
    last_no_o_ids[d - 1] = k_no->no_o_id + 1;  // XXX: update last seen

    const oorder::key k_oo(warehouse_id, d, k_no->no_o_id);
    // even if we read the new order entry, there's no guarantee
    // we will read the oorder entry: in this case the txn will abort,
    // but we're simply bailing out early
    oorder::value v_oo_temp;
    oorder_precompute_aggregate::value v_oo_pa_temp;
    ermia::varstr valptr;

    const oorder::value *v_oo = nullptr;
    const oorder_precompute_aggregate::value *v_oo_pa = nullptr;

    rc_t rc = rc_t{RC_INVALID};
    if (oorder_schema_version == 0) {
      oorder_table_index->GetRecord(txn, rc, Encode(str(Size(k_oo)), k_oo),
                                    valptr, nullptr, oorder_schema_ptr);
      TryCatchCondAbort(rc);
      v_oo = Decode(valptr, v_oo_temp);
#ifndef NDEBUG
      checker::SanityCheckOOrder(&k_oo, v_oo);
#endif
    } else {
      oorder_table_index->GetRecord(txn, rc, Encode(str(Size(k_oo)), k_oo),
                                    valptr, nullptr, oorder_schema_ptr);
      TryCatchCondAbort(rc);
      v_oo_pa = Decode(valptr, v_oo_pa_temp);
    }

    static_limit_callback<15> c(
        s_arena.get(), false);  // never more than 15 order_lines per order
    const order_line::key k_oo_0(warehouse_id, d, k_no->no_o_id, 0);
    const order_line::key k_oo_1(warehouse_id, d, k_no->no_o_id,
                                 std::numeric_limits<int32_t>::max());

#ifdef COPYDDL
    ermia::ConcurrentMasstreeIndex *order_line_table_index =
        (ermia::ConcurrentMasstreeIndex *)order_line_schema.index;
#endif

    // XXX(stephentu): mutable scans would help here
    TryCatch(order_line_table_index->Scan(
        txn, Encode(str(Size(k_oo_0)), k_oo_0),
        &Encode(str(Size(k_oo_1)), k_oo_1), c, order_line_schema_ptr));

    if (order_line_schema_version != 0) {
      if (c.size() == 0) {
        TryCatch({RC_ABORT_USER});
      }
    }

    float sum = 0.0;
    for (size_t i = 0; i < c.size(); i++) {
      if (order_line_schema_version == 0) {
        order_line::value v_ol_temp;
        const order_line::value *v_ol = Decode(*c.values[i].second, v_ol_temp);

#ifndef NDEBUG
        order_line::key k_ol_temp;
        const order_line::key *k_ol = Decode(*c.values[i].first, k_ol_temp);
        checker::SanityCheckOrderLine(k_ol, v_ol);
#endif

        sum += v_ol->ol_amount;
        order_line::value v_ol_new(*v_ol);
        v_ol_new.ol_delivery_d = ts;
        ASSERT(s_arena.get()->manages(c.values[i].first));

        TryCatch(order_line_table_index->UpdateRecord(
            txn, *c.values[i].first, Encode(str(Size(v_ol_new)), v_ol_new),
            order_line_schema_ptr));
      } else {
        if (ddl_example == 0 || ddl_example == 10) {
          order_line_1::value v_ol_temp;
          const order_line_1::value *v_ol =
              Decode(*c.values[i].second, v_ol_temp);

#ifndef NDEBUG
          order_line_1::key k_ol_temp;
          const order_line_1::key *k_ol = Decode(*c.values[i].first, k_ol_temp);
          // checker::SanityCheckOrderLine(k_ol, v_ol);
#endif

          sum += v_ol->ol_amount;
          order_line_1::value v_ol_new(*v_ol);
          v_ol_new.ol_delivery_d = ts;
          v_ol_new.ol_tax = (float)0.1;
          ASSERT(s_arena.get()->manages(c.values[i].first));

          TryCatch(order_line_table_index->UpdateRecord(
              txn, *c.values[i].first, Encode(str(Size(v_ol_new)), v_ol_new),
              order_line_schema_ptr));
        } else if (ddl_example == 4) {
          order_line_stock::value v_ol_s_temp;
          const order_line_stock::value *v_ol_s =
              Decode(*c.values[i].second, v_ol_s_temp);

          sum += v_ol_s->ol_amount;
          order_line_stock::value v_ol_s_new(*v_ol_s);
          v_ol_s_new.ol_delivery_d = ts;
          ASSERT(s_arena.get()->manages(c.values[i].first));

          TryCatch(order_line_table_index->UpdateRecord(
              txn, *c.values[i].first,
              Encode(str(Size(v_ol_s_new)), v_ol_s_new), order_line_schema_ptr));
        } else if (ddl_example == 9) {
	  order_line::value v_ol_temp;
          const order_line::value *v_ol = Decode(*c.values[i].second, v_ol_temp);

          sum += v_ol->ol_amount;
          order_line::value v_ol_new(*v_ol);
          v_ol_new.ol_delivery_d = ts;
          ASSERT(s_arena.get()->manages(c.values[i].first));

          TryCatch(order_line_table_index->UpdateRecord(
              txn, *c.values[i].first, Encode(str(Size(v_ol_new)), v_ol_new),
              order_line_schema_ptr));
	}
      }
    }

    // delete new order
    TryCatch(tbl_new_order(warehouse_id)
                 ->RemoveRecord(txn, Encode(str(Size(*k_no)), *k_no)));

    // update oorder
    if (oorder_schema_version == 0) {
      oorder::value v_oo_new(*v_oo);
      v_oo_new.o_carrier_id = o_carrier_id;
      TryCatch(oorder_table_index->UpdateRecord(
          txn, Encode(str(Size(k_oo)), k_oo),
          Encode(str(Size(v_oo_new)), v_oo_new), oorder_schema_ptr));
    } else {
      oorder_precompute_aggregate::value v_oo_pa_new(*v_oo_pa);
      v_oo_pa_new.o_carrier_id = o_carrier_id;
      TryCatch(oorder_table_index->UpdateRecord(
          txn, Encode(str(Size(k_oo)), k_oo),
          Encode(str(Size(v_oo_pa_new)), v_oo_pa_new), oorder_schema_ptr));
    }

    uint c_id;

    if (oorder_schema_version == 0) {
      c_id = v_oo->o_c_id;
    } else {
      c_id = v_oo_pa->o_c_id;
    }

    const float ol_total = sum;

    // update customer
    const customer::key k_c(warehouse_id, d, c_id);

    if (customer_schema_version == 0) {
      customer::value v_c_temp;

      rc = rc_t{RC_INVALID};
      customer_table_index->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c),
                                      valptr, nullptr, customer_schema_ptr);
      // TryVerifyRelaxed(rc);
      TryCatch(rc);

      const customer::value *v_c = Decode(valptr, v_c_temp);
      customer::value v_c_new(*v_c);
      v_c_new.c_balance += ol_total;
      TryCatch(customer_table_index->UpdateRecord(
          txn, Encode(str(Size(k_c)), k_c), Encode(str(Size(v_c_new)), v_c_new),
          customer_schema_ptr));
    } else {
      customer_private::value v_c_temp;

      rc = rc_t{RC_INVALID};
      customer_table_index->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c),
                                      valptr, nullptr, customer_schema_ptr);
      // TryVerifyRelaxed(rc);
      TryCatch(rc);

      const customer_private::value *v_c = Decode(valptr, v_c_temp);
      customer_private::value v_c_new(*v_c);
      v_c_new.c_balance += ol_total;
      TryCatch(customer_table_index->UpdateRecord(
          txn, Encode(str(Size(k_c)), k_c), Encode(str(Size(v_c_new)), v_c_new),
          customer_schema_ptr));
    }
  }
  TryCatch(db->Commit(txn));
  return {RC_TRUE};
}

rc_t tpcc_worker::txn_order_status() {
  const uint warehouse_id = pick_wh(r, home_warehouse_id);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());

  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 13
  //   max_read_set_size : 81
  //   max_write_set_size : 0
  //   num_txn_contexts : 4
  ermia::transaction *txn = nullptr;
#if !defined(LAZYDDL)
  txn = db->NewTransaction(ermia::transaction::TXN_FLAG_READ_ONLY, *arena,
                           txn_buf());
#else
  txn = db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
#endif
  ermia::scoped_str_arena s_arena(arena);
  // NB: since txn_order_status() is a RO txn, we assume that
  // locking is un-necessary (since we can just read from some old snapshot)

  uint64_t order_line_schema_version = 0;
  uint64_t oorder_schema_version = 0;
  uint64_t customer_schema_version = 0;
  ermia::schema_record *order_line_schema_ptr = nullptr;
  ermia::schema_record *oorder_schema_ptr = nullptr;
  ermia::schema_record *customer_schema_ptr = nullptr;

#ifdef DDL
  // Read schema tables first
  ermia::schema_record order_line_schema;
  order_line_schema_version = tpcc_read_schema(txn, order_line_table_index, order_line_key, order_line_schema);
  order_line_schema_ptr = &order_line_schema;

  ermia::schema_record oorder_schema;
  oorder_schema_version = tpcc_read_schema(txn, oorder_table_index, oorder_key, oorder_schema);
  oorder_schema_ptr = &oorder_schema;

  ermia::schema_record customer_schema;
  customer_schema_version = tpcc_read_schema(txn, customer_table_index, customer_key, customer_schema);
  customer_schema_ptr = &customer_schema;
#endif

#ifdef COPYDDL
  ermia::ConcurrentMasstreeIndex *customer_table_index =
      (ermia::ConcurrentMasstreeIndex *)customer_schema.index;
  ermia::ConcurrentMasstreeIndex *customer_table_secondary_index =
      (ermia::ConcurrentMasstreeIndex *)(*(
          (customer_schema.td)->GetSecIndexes().begin()));
#endif

  customer::key k_c;
  ermia::varstr valptr;
  if (RandomNumber(r, 1, 100) <= 60) {
    // cust by name
    uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
    static_assert(sizeof(lastname_buf) == 16, "xx");
    memset(lastname_buf, 0, sizeof(lastname_buf));
    GetNonUniformCustomerLastNameRun(lastname_buf, r);

    static const std::string zeros(16, 0);
    static const std::string ones(16, (char)255);

    customer_name_idx::key k_c_idx_0;
    k_c_idx_0.c_w_id = warehouse_id;
    k_c_idx_0.c_d_id = districtID;
    k_c_idx_0.c_last.assign((const char *)lastname_buf, 16);
    k_c_idx_0.c_first.assign(zeros);

    customer_name_idx::key k_c_idx_1;
    k_c_idx_1.c_w_id = warehouse_id;
    k_c_idx_1.c_d_id = districtID;
    k_c_idx_1.c_last.assign((const char *)lastname_buf, 16);
    k_c_idx_1.c_first.assign(ones);

    static_limit_callback<NMaxCustomerIdxScanElems> c(
        s_arena.get(), true);  // probably a safe bet for now
    TryCatch(customer_table_secondary_index->Scan(
        txn, Encode(str(Size(k_c_idx_0)), k_c_idx_0),
        &Encode(str(Size(k_c_idx_1)), k_c_idx_1), c, customer_schema_ptr));

    if (c.size() <= 0) {
      TryCatch(rc_t{RC_ABORT_USER});
    }

    ALWAYS_ASSERT(c.size() > 0);
    ASSERT(c.size() < NMaxCustomerIdxScanElems);  // we should detect this
    int index = c.size() / 2;
    if (c.size() % 2 == 0) index--;

    if (customer_schema_version == 0) {
      customer::value v_c;
      Decode(*c.values[index].second, v_c);
      k_c.c_id = v_c.c_id;
    } else {
      customer_private::value v_c;
      Decode(*c.values[index].second, v_c);
      k_c.c_id = v_c.c_id;
    }
    k_c.c_w_id = warehouse_id;
    k_c.c_d_id = districtID;
  } else {
    // cust by ID
    const uint customerID = GetCustomerId(r);
    k_c.c_w_id = warehouse_id;
    k_c.c_d_id = districtID;
    k_c.c_id = customerID;

    rc_t rc = rc_t{RC_INVALID};
    customer_table_index->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c),
                                    valptr, nullptr, customer_schema_ptr);
    // TryVerifyRelaxed(rc);
    TryCatch(rc);

    if (customer_schema_version == 0) {
      customer::value v_c;
      Decode(valptr, v_c);
    } else {
      customer_private::value v_c;
      Decode(valptr, v_c);
    }
  }
#ifndef NDEBUG
  // checker::SanityCheckCustomer(&k_c, &v_c);
#endif

  oorder_c_id_idx::value sv;
  ermia::varstr *newest_o_c_id = s_arena.get()->next(Size(sv));
  if (g_order_status_scan_hack) {
    // XXX(stephentu): HACK- we bound the # of elems returned by this scan to
    // 15- this is because we don't have reverse scans. In an ideal system, a
    // reverse scan would only need to read 1 btree node. We could simulate a
    // lookup by only reading the first element- but then we would *always*
    // read the first order by any customer.  To make this more interesting, we
    // randomly select which elem to pick within the 1st or 2nd btree nodes.
    // This is obviously a deviation from TPC-C, but it shouldn't make that
    // much of a difference in terms of performance numbers (in fact we are
    // making it worse for us)
    latest_key_callback c_oorder(*newest_o_c_id, (r.next() % 15) + 1);
    const oorder_c_id_idx::key k_oo_idx_0(warehouse_id, districtID, k_c.c_id,
                                          0);
    const oorder_c_id_idx::key k_oo_idx_1(warehouse_id, districtID, k_c.c_id,
                                          std::numeric_limits<int32_t>::max());
    {
      if (oorder_schema_version == 0) {
        TryCatch(tbl_oorder_c_id_idx(warehouse_id)
                     ->Scan(txn, Encode(str(Size(k_oo_idx_0)), k_oo_idx_0),
                            &Encode(str(Size(k_oo_idx_1)), k_oo_idx_1),
                            c_oorder, oorder_schema_ptr));
      } else {
#ifdef COPYDDL
        ermia::ConcurrentMasstreeIndex *oorder_table_secondary_index =
            (ermia::ConcurrentMasstreeIndex *)(*(
                (oorder_schema.td)->GetSecIndexes().begin()));
#endif
        TryCatch(oorder_table_secondary_index->Scan(
            txn, Encode(str(Size(k_oo_idx_0)), k_oo_idx_0),
            &Encode(str(Size(k_oo_idx_1)), k_oo_idx_1), c_oorder,
            oorder_schema_ptr));

        if (!c_oorder.size()) {
          TryCatch(rc_t{RC_ABORT_USER});
        }
      }
    }
    ALWAYS_ASSERT(c_oorder.size());
  } else {
    latest_key_callback c_oorder(*newest_o_c_id, 1);
    const oorder_c_id_idx::key k_oo_idx_hi(warehouse_id, districtID, k_c.c_id,
                                           std::numeric_limits<int32_t>::max());
    if (oorder_schema_version == 0) {
      TryCatch(tbl_oorder_c_id_idx(warehouse_id)
                   ->ReverseScan(txn,
                                 Encode(str(Size(k_oo_idx_hi)), k_oo_idx_hi),
                                 nullptr, c_oorder, oorder_schema_ptr));
    } else {
#ifdef COPYDDL
      ermia::ConcurrentMasstreeIndex *oorder_table_secondary_index =
          (ermia::ConcurrentMasstreeIndex *)(*(
              (oorder_schema.td)->GetSecIndexes().begin()));
#endif
      TryCatch(oorder_table_secondary_index->ReverseScan(
          txn, Encode(str(Size(k_oo_idx_hi)), k_oo_idx_hi), nullptr, c_oorder,
          oorder_schema_ptr));

      if (c_oorder.size() != 1) {
        TryCatch(rc_t{RC_ABORT_USER});
      }
    }
    ALWAYS_ASSERT(c_oorder.size() == 1);
  }

  oorder_c_id_idx::key k_oo_idx_temp;
  const oorder_c_id_idx::key *k_oo_idx = Decode(*newest_o_c_id, k_oo_idx_temp);
  const uint o_id = k_oo_idx->o_o_id;

  order_line_nop_callback c_order_line(order_line_schema_version);
  const order_line::key k_ol_0(warehouse_id, districtID, o_id, 0);
  const order_line::key k_ol_1(warehouse_id, districtID, o_id,
                               std::numeric_limits<int32_t>::max());

#ifdef COPYDDL
  ermia::ConcurrentMasstreeIndex *order_line_table_index =
      (ermia::ConcurrentMasstreeIndex *)order_line_schema.index;
#endif

  if (order_line_schema_version == 0) {
    TryCatch(order_line_table_index->Scan(
        txn, Encode(str(Size(k_ol_0)), k_ol_0),
        &Encode(str(Size(k_ol_1)), k_ol_1), c_order_line, order_line_schema_ptr));
    if (c_order_line.n < 5 || c_order_line.n > 15) {
      TryCatch({RC_ABORT_USER});
    }
    ALWAYS_ASSERT(c_order_line.n >= 5 && c_order_line.n <= 15);
  } else {
    TryCatch(order_line_table_index->Scan(
        txn, Encode(str(Size(k_ol_0)), k_ol_0),
        &Encode(str(Size(k_ol_1)), k_ol_1), c_order_line, order_line_schema_ptr));

    if (c_order_line.n < 5 || c_order_line.n > 15) {
      TryCatch({RC_ABORT_USER});
    }
  }

  TryCatch(db->Commit(txn));
  return {RC_TRUE};
}

rc_t tpcc_worker::txn_stock_level() {
  const uint warehouse_id = pick_wh(r, home_warehouse_id);
  const uint threshold = RandomNumber(r, 10, 20);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());

  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 19
  //   max_read_set_size : 241
  //   max_write_set_size : 0
  //   n_node_scan_large_instances : 1
  //   n_read_set_large_instances : 2
  //   num_txn_contexts : 3
  ermia::transaction *txn = nullptr;
#if !defined(LAZYDDL)
  txn = db->NewTransaction(ermia::transaction::TXN_FLAG_READ_ONLY, *arena,
                           txn_buf());
#else
  txn = db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
#endif
  ermia::scoped_str_arena s_arena(arena);

  uint64_t order_line_schema_version = 0;
  ermia::schema_record *order_line_schema_ptr = nullptr;

#ifdef DDL
  // Read schema tables first
  ermia::schema_record order_line_schema;
  order_line_schema_version = tpcc_read_schema(txn, order_line_table_index, order_line_key, order_line_schema);
  order_line_schema_ptr = &order_line_schema;
#endif

  // NB: since txn_stock_level() is a RO txn, we assume that
  // locking is un-necessary (since we can just read from some old snapshot)
  const district::key k_d(warehouse_id, districtID);
  district::value v_d_temp;
  ermia::varstr valptr;

  rc_t rc = rc_t{RC_INVALID};
  tbl_district(warehouse_id)
      ->GetRecord(txn, rc, Encode(str(Size(k_d)), k_d), valptr);
  TryVerifyRelaxed(rc);

  const district::value *v_d = Decode(valptr, v_d_temp);
#ifndef NDEBUG
  checker::SanityCheckDistrict(&k_d, v_d);
#endif

  const uint64_t cur_next_o_id =
      g_new_order_fast_id_gen ? NewOrderIdHolder(warehouse_id, districtID)
                                    .load(std::memory_order_acquire)
                              : v_d->d_next_o_id;

  // manual joins are fun!
  order_line_scan_callback c(order_line_schema_version, threshold);
  const int32_t lower = cur_next_o_id >= 20 ? (cur_next_o_id - 20) : 0;
  const order_line::key k_ol_0(warehouse_id, districtID, lower, 0);
  const order_line::key k_ol_1(warehouse_id, districtID, cur_next_o_id, 0);

#ifdef COPYDDL
  ermia::ConcurrentMasstreeIndex *order_line_table_index =
      (ermia::ConcurrentMasstreeIndex *)order_line_schema.index;
#endif

  TryCatch(order_line_table_index->Scan(txn, Encode(str(Size(k_ol_0)), k_ol_0),
                                        &Encode(str(Size(k_ol_1)), k_ol_1), c,
                                        order_line_schema_ptr));

  if (order_line_schema_version != 0) {
    if (c.n == 0) {
      TryCatch({RC_ABORT_USER});
    }
  }
  if (order_line_schema_version != 0 && ddl_example == 4) {
    goto exit;
  }
  {
    std::unordered_map<uint, bool> s_i_ids_distinct;
    for (auto &p : c.s_i_ids) {
      const stock::key k_s(warehouse_id, p.first);
      stock::value v_s;
      ASSERT(p.first >= 1 && p.first <= NumItems());

      rc = rc_t{RC_INVALID};
      tbl_stock(warehouse_id)
          ->GetRecord(txn, rc, Encode(str(Size(k_s)), k_s), valptr);
      TryVerifyRelaxed(rc);

      const uint8_t *ptr = (const uint8_t *)valptr.data();
      int16_t i16tmp;
      ptr = serializer<int16_t, true>::read(ptr, &i16tmp);
      if (i16tmp < int(threshold)) s_i_ids_distinct[p.first] = 1;
    }
    // NB(stephentu): s_i_ids_distinct.size() is the computed result of this txn
  }
exit:
  TryCatch(db->Commit(txn));
  return {RC_TRUE};
}

rc_t tpcc_worker::txn_credit_check() {
  /*
          Note: Cahill's credit check transaction to introduce SI's anomaly.

          SELECT c_balance, c_credit_lim
          INTO :c_balance, :c_credit_lim
          FROM Customer
          WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id

          SELECT SUM(ol_amount) INTO :neworder_balance
          FROM OrderLine, Orders, NewOrder
          WHERE ol_o_id = o_id AND ol_d_id = :d_id
          AND ol_w_id = :w_id AND o_d_id = :d_id
          AND o_w_id = :w_id AND o_c_id = :c_id
          AND no_o_id = o_id AND no_d_id = :d_id
          AND no_w_id = :w_id

          if (c_balance + neworder_balance > c_credit_lim)
          c_credit = "BC";
          else
          c_credit = "GC";

          SQL UPDATE Customer SET c_credit = :c_credit
          WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id
  */
  const uint warehouse_id = pick_wh(r, home_warehouse_id);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  uint customerDistrictID, customerWarehouseID;
  if (likely(g_disable_xpartition_txn || NumWarehouses() == 1 ||
             RandomNumber(r, 1, 100) <= 85)) {
    customerDistrictID = districtID;
    customerWarehouseID = warehouse_id;
  } else {
    customerDistrictID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
    do {
      customerWarehouseID = RandomNumber(r, 1, NumWarehouses());
    } while (customerWarehouseID == warehouse_id);
  }
  ASSERT(!g_disable_xpartition_txn || customerWarehouseID == warehouse_id);

  ermia::transaction *txn =
      db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);

  uint64_t order_line_schema_version = 0;
  uint64_t oorder_schema_version = 0;
  uint64_t customer_schema_version = 0;
  ermia::schema_record *order_line_schema_ptr = nullptr;
  ermia::schema_record *oorder_schema_ptr = nullptr;
  ermia::schema_record *customer_schema_ptr = nullptr;

#ifdef DDL
  // Read schema tables first
  ermia::schema_record order_line_schema;
  order_line_schema_version = tpcc_read_schema(txn, order_line_table_index, order_line_key, order_line_schema);
  order_line_schema_ptr = &order_line_schema;

  ermia::schema_record oorder_schema;
  oorder_schema_version = tpcc_read_schema(txn, oorder_table_index, oorder_key, oorder_schema);
  oorder_schema_ptr = &oorder_schema;

  ermia::schema_record customer_schema;
  customer_schema_version = tpcc_read_schema(txn, customer_table_index, customer_key, customer_schema);
  customer_schema_ptr = &customer_schema;
#endif

#ifdef COPYDDL
  ermia::ConcurrentMasstreeIndex *customer_table_index =
      (ermia::ConcurrentMasstreeIndex *)customer_schema.index;
  ermia::ConcurrentMasstreeIndex *customer_table_secondary_index =
      (ermia::ConcurrentMasstreeIndex *)(*(
          (customer_schema.td)->GetSecIndexes().begin()));
#endif

  // select * from customer with random C_ID
  customer::key k_c;
  ermia::varstr valptr;
  const uint customerID = GetCustomerId(r);
  k_c.c_w_id = customerWarehouseID;
  k_c.c_d_id = customerDistrictID;
  k_c.c_id = customerID;

  rc_t rc = rc_t{RC_INVALID};
  customer_table_index->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c), valptr,
                                  nullptr, customer_schema_ptr);
  // TryVerifyRelaxed(rc);
  if (unlikely(rc._val == RC_FALSE)) {
    rc._val = RC_ABORT_USER;
  }
  TryCatch(rc);

  customer::value v_c_temp;
  const customer::value *v_c = nullptr;
  customer_private::value v_c_private_temp;
  const customer_private::value *v_c_private = nullptr;
  if (customer_schema_version == 0) {
    v_c = Decode(valptr, v_c_temp);
  } else {
    v_c_private = Decode(valptr, v_c_private_temp);
  }

  // scan order
  //		c_w_id = :w_id;
  //		c_d_id = :d_id;
  //		c_id = :c_id;
  credit_check_order_scan_callback c_no(s_arena.get());
  const new_order::key k_no_0(warehouse_id, districtID, 0);
  const new_order::key k_no_1(warehouse_id, districtID,
                              std::numeric_limits<int32_t>::max());

  TryCatch(tbl_new_order(warehouse_id)
               ->Scan(txn, Encode(str(Size(k_no_0)), k_no_0),
                      &Encode(str(Size(k_no_1)), k_no_1), c_no));
  if (!c_no.output.size()) {
    TryCatch({RC_ABORT_USER});
  }
  ALWAYS_ASSERT(c_no.output.size());

  double sum = 0;

#ifdef COPYDDL
  ermia::ConcurrentMasstreeIndex *order_line_table_index =
      (ermia::ConcurrentMasstreeIndex *)order_line_schema.index;
  ermia::ConcurrentMasstreeIndex *oorder_table_index =
      (ermia::ConcurrentMasstreeIndex *)oorder_schema.index;
#endif

  if (oorder_schema_version == 0) {
    for (auto &k : c_no.output) {
      new_order::key k_no_temp;
      const new_order::key *k_no = Decode(*k, k_no_temp);

      const oorder::key k_oo(warehouse_id, districtID, k_no->no_o_id);
      oorder::value v;
      rc = rc_t{RC_INVALID};
      tbl_oorder(warehouse_id)
          ->GetRecord(txn, rc, Encode(str(Size(k_oo)), k_oo), valptr, nullptr,
                      oorder_schema_ptr);
      TryCatchCond(rc, continue);
      auto *vv = Decode(valptr, v);

      // Order line scan
      //		ol_d_id = :d_id
      //		ol_w_id = :w_id
      //		ol_o_id = o_id
      //		ol_number = 1-15
      credit_check_order_line_scan_callback c_ol(order_line_schema_version);
      const order_line::key k_ol_0(warehouse_id, districtID, k_no->no_o_id, 1);
      const order_line::key k_ol_1(warehouse_id, districtID, k_no->no_o_id, 15);

      TryCatch(order_line_table_index->Scan(
          txn, Encode(str(Size(k_ol_0)), k_ol_0),
          &Encode(str(Size(k_ol_1)), k_ol_1), c_ol, order_line_schema_ptr));

      /* XXX(tzwang): moved to the callback to avoid storing keys
      ALWAYS_ASSERT(c_ol._v_ol.size());
      for (auto &v_ol : c_ol._v_ol) {
        order_line::value v_ol_temp;
        const order_line::value *val = Decode(*v_ol, v_ol_temp);

        // Aggregation
        sum += val->ol_amount;
      }
      */
      sum += c_ol.sum;
    }
  } else {
    for (auto &k : c_no.output) {
      new_order::key k_no_temp;
      const new_order::key *k_no = Decode(*k, k_no_temp);
      const oorder_precompute_aggregate::key k_oo(warehouse_id, districtID,
                                                  k_no->no_o_id);
      oorder_precompute_aggregate::value v;
      rc = rc_t{RC_INVALID};
      oorder_table_index->GetRecord(txn, rc, Encode(str(Size(k_oo)), k_oo),
                                    valptr, nullptr, oorder_schema_ptr);
      TryCatchCond(rc, continue);
      auto *vv = Decode(valptr, v);
      sum += vv->o_total_amount;
    }
  }

  // c_credit update
  if (customer_schema_version == 0) {
#ifndef NDEBUG
    checker::SanityCheckCustomer(&k_c, v_c);
#endif
    customer::value v_c_new(*v_c);
    if (v_c_new.c_balance + sum >= 5000)  // Threshold = 5K
      v_c_new.c_credit.assign("BC");
    else
      v_c_new.c_credit.assign("GC");
    TryCatch(customer_table_index->UpdateRecord(
        txn, Encode(str(Size(k_c)), k_c), Encode(str(Size(v_c_new)), v_c_new),
        customer_schema_ptr));
  } else {
    customer_private::value v_c_new(*v_c_private);
    if (v_c_new.c_balance + sum >= 5000)  // Threshold = 5K
      v_c_new.c_credit.assign("BC");
    else
      v_c_new.c_credit.assign("GC");
    TryCatch(customer_table_index->UpdateRecord(
        txn, Encode(str(Size(k_c)), k_c), Encode(str(Size(v_c_new)), v_c_new),
        customer_schema_ptr));
  }

  TryCatch(db->Commit(txn));
  return {RC_TRUE};
}

rc_t tpcc_worker::txn_query2() {
  // TODO(yongjunh): use TXN_FLAG_READ_MOSTLY once SSN's and SSI's read
  // optimization are available.
  ermia::transaction *txn =
      db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);

  uint64_t order_line_schema_version = 0;
  ermia::schema_record *order_line_schema_ptr = nullptr;

#ifdef DDL
  // Read schema tables first
  ermia::schema_record order_line_schema;
  order_line_schema_version = tpcc_read_schema(txn, order_line_table_index, order_line_key, order_line_schema);
  order_line_schema_ptr = &order_line_schema;
#endif

  static thread_local tpcc_table_scanner r_scanner(arena);
  r_scanner.clear();
  const region::key k_r_0(0);
  const region::key k_r_1(5);
  TryCatch(tbl_region(1)->Scan(txn, Encode(str(sizeof(k_r_0)), k_r_0),
                               &Encode(str(sizeof(k_r_1)), k_r_1), r_scanner));
  ALWAYS_ASSERT(r_scanner.output.size() == 5);

  static thread_local tpcc_table_scanner n_scanner(arena);
  n_scanner.clear();
  const nation::key k_n_0(0);
  const nation::key k_n_1(std::numeric_limits<int32_t>::max());
  TryCatch(tbl_nation(1)->Scan(txn, Encode(str(sizeof(k_n_0)), k_n_0),
                               &Encode(str(sizeof(k_n_1)), k_n_1), n_scanner));
  ALWAYS_ASSERT(n_scanner.output.size() == 62);

  // Pick a target region
  auto target_region = RandomNumber(r, 0, 4);
  //	auto target_region = 3;
  ALWAYS_ASSERT(0 <= target_region and target_region <= 4);

  // Scan region
  for (auto &r_r : r_scanner.output) {
    region::value v_r_temp;
    const region::value *v_r = Decode(*r_r.second, v_r_temp);

    // filtering region
    if (v_r->r_name != std::string(regions[target_region])) continue;

    region::key k_r_temp;
    const region::key *k_r = Decode(*r_r.first, k_r_temp);
    // Scan nation
    for (auto &r_n : n_scanner.output) {
      nation::value v_n_temp;
      const nation::value *v_n = Decode(*r_n.second, v_n_temp);

      // filtering nation
      if (k_r->r_regionkey != v_n->n_regionkey) continue;

      nation::key k_n_temp;
      const nation::key *k_n = Decode(*r_n.first, k_n_temp);

      // Scan suppliers
      for (auto i = 0; i < g_nr_suppliers; i++) {
        const supplier::key k_su(i);
        supplier::value v_su_tmp;
        ermia::varstr valptr;

        rc_t rc = rc_t{RC_INVALID};
        tbl_supplier(1)->GetRecord(txn, rc, Encode(str(Size(k_su)), k_su),
                                   valptr);
        TryVerifyRelaxed(rc);

        arena->return_space(Size(k_su));

        const supplier::value *v_su = Decode(valptr, v_su_tmp);

        // Filtering suppliers
        if (k_n->n_nationkey != v_su->su_nationkey) continue;

        // aggregate - finding a stock tuple having min. stock level
        stock::key min_k_s(0, 0);
        stock::value min_v_s(0, 0, 0, 0);

        int16_t min_qty = std::numeric_limits<int16_t>::max();
        for (auto &it : supp_stock_map
                 [k_su.su_suppkey])  // already know
                                     // "mod((s_w_id*s_i_id),10000)=su_suppkey"
                                     // items
        {
          const stock::key k_s(it.first, it.second);
          stock::value v_s_tmp(0, 0, 0, 0);
          rc = rc_t{RC_INVALID};
          tbl_stock(it.first)->GetRecord(txn, rc, Encode(str(Size(k_s)), k_s),
                                         valptr);
          TryVerifyRelaxed(rc);

          arena->return_space(Size(k_s));
          const stock::value *v_s = Decode(valptr, v_s_tmp);

          ASSERT(k_s.s_w_id * k_s.s_i_id % 10000 == k_su.su_suppkey);
          if (min_qty > v_s->s_quantity) {
            min_k_s.s_w_id = k_s.s_w_id;
            min_k_s.s_i_id = k_s.s_i_id;
            min_v_s.s_quantity = v_s->s_quantity;
            min_v_s.s_ytd = v_s->s_ytd;
            min_v_s.s_order_cnt = v_s->s_order_cnt;
            min_v_s.s_remote_cnt = v_s->s_remote_cnt;
          }
        }

        // fetch the (lowest stock level) item info
        const item::key k_i(min_k_s.s_i_id);
        item::value v_i_temp;
        rc = rc_t{RC_INVALID};
        tbl_item(1)->GetRecord(txn, rc, Encode(str(Size(k_i)), k_i), valptr);
        TryVerifyRelaxed(rc);

        arena->return_space(Size(k_i));
        const item::value *v_i = Decode(valptr, v_i_temp);
#ifndef NDEBUG
        checker::SanityCheckItem(&k_i, v_i);
#endif

        //  filtering item (i_data like '%b')
        auto found = v_i->i_data.str().find('b');
        if (found != std::string::npos) continue;

        // XXX. read-mostly txn: update stock or item here

        if (min_v_s.s_quantity < 15) {
          stock::value new_v_s;
          new_v_s.s_quantity = min_v_s.s_quantity + 50;
          new_v_s.s_ytd = min_v_s.s_ytd;
          new_v_s.s_order_cnt = min_v_s.s_order_cnt;
          new_v_s.s_remote_cnt = min_v_s.s_remote_cnt;
#ifndef NDEBUG
          checker::SanityCheckStock(&min_k_s);
#endif
          TryCatch(tbl_stock(min_k_s.s_w_id)
                       ->UpdateRecord(txn, Encode(str(Size(min_k_s)), min_k_s),
                                      Encode(str(Size(new_v_s)), new_v_s)));
        }

        // TODO. sorting by n_name, su_name, i_id

        /*
        cout << k_su.su_suppkey        << ","
                << v_su->su_name                << ","
                << v_n->n_name                  << ","
                << k_i.i_id                     << ","
                << v_i->i_name                  << std::endl;
                */
      }
    }
  }

  TryCatch(db->Commit(txn));
  return {RC_TRUE};
}

rc_t tpcc_worker::txn_microbench_random() {
  ermia::transaction *txn =
      db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);
  uint start_w = 0, start_s = 0;
  ASSERT(NumWarehouses() * NumItems() >= g_microbench_rows);

  // pick start row, if it's not enough, later wrap to the first row
  uint w = start_w = RandomNumber(r, 1, NumWarehouses());
  uint s = start_s = RandomNumber(r, 1, NumItems());

  // read rows
  ermia::varstr sv;
  for (uint i = 0; i < g_microbench_rows; i++) {
    const stock::key k_s(w, s);
    DLOG(INFO) << "rd " << w << " " << s;
    rc_t rc = rc_t{RC_INVALID};
    tbl_stock(w)->GetRecord(txn, rc, Encode(str(Size(k_s)), k_s), sv);
    TryCatch(rc);

    if (++s > NumItems()) {
      s = 1;
      if (++w > NumWarehouses()) w = 1;
    }
  }

  // now write, in the same read-set
  uint n_write_rows = g_microbench_wr_rows;
  for (uint i = 0; i < n_write_rows; i++) {
    // generate key
    uint row_nr = RandomNumber(
        r, 1, n_write_rows + 1);  // XXX. do we need overlap checking?

    // index starting with 1 is a pain with %, starting with 0 instead:
    // convert row number to (w, s) tuple
    const uint idx =
        (start_w - 1) * NumItems() + (start_s - 1 + row_nr) % NumItems();
    const uint ww = idx / NumItems() + 1;
    const uint ss = idx % NumItems() + 1;

    DLOG(INFO) << (ww - 1) * NumItems() + ss - 1;
    DLOG(INFO) << ((start_w - 1) * NumItems() + start_s - 1 + row_nr) %
                      (NumItems() * (NumWarehouses()));
    ASSERT((ww - 1) * NumItems() + ss - 1 < NumItems() * NumWarehouses());
    ASSERT((ww - 1) * NumItems() + ss - 1 ==
           ((start_w - 1) * NumItems() + (start_s - 1 + row_nr) % NumItems()) %
               (NumItems() * (NumWarehouses())));

    // TODO. more plausible update needed
    const stock::key k_s(ww, ss);
    DLOG(INFO) << "wr " << ww << " " << ss << " row_nr=" << row_nr;

    stock::value v;
    v.s_quantity = RandomNumber(r, 10, 100);
    v.s_ytd = 0;
    v.s_order_cnt = 0;
    v.s_remote_cnt = 0;

#ifndef NDEBUG
    checker::SanityCheckStock(&k_s);
#endif
    TryCatch(tbl_stock(ww)->UpdateRecord(txn, Encode(str(Size(k_s)), k_s),
                                         Encode(str(Size(v)), v)));
  }

  DLOG(INFO) << "micro-random finished";
#ifndef NDEBUG
  abort();
#endif

  TryCatch(db->Commit(txn));
  return {RC_TRUE};
}

rc_t tpcc_worker::txn_ddl(uint32_t ddl_example) {
  ermia::ddl::ddl_executor *ddl_exe = new ermia::ddl::ddl_executor();
  
  ermia::transaction *txn =
      db->NewTransaction(ermia::transaction::TXN_FLAG_DDL, *arena, txn_buf(), 0, ddl_exe);

  ddl_exe->set_transaction(txn);

  switch (ddl_example) {
    case 0:
      TryCatch(add_column(txn, ddl_exe, ddl_example));
      break;
    case 1:
      TryCatch(table_split(txn, ddl_exe, ddl_example));
      break;
    case 2:
      TryCatch(preaggregation(txn, ddl_exe, ddl_example));
      break;
    case 3:
      TryCatch(create_index(txn, ddl_exe, ddl_example));
      break;
    case 4:
      TryCatch(table_join(txn, ddl_exe, ddl_example));
      break;
    case 5:
      TryCatch(add_column(txn, ddl_exe, ddl_example));
      break;
    case 6:
      TryCatch(table_split(txn, ddl_exe, ddl_example));
      break;
    case 7:
      TryCatch(preaggregation(txn, ddl_exe, ddl_example));
      break;
    case 8:
      TryCatch(table_join(txn, ddl_exe, ddl_example));
      break;
    case 9:
      TryCatch(add_constraint(txn, ddl_exe, ddl_example));
      break;
    case 10:
      TryCatch(add_column_and_constraint(txn, ddl_exe, ddl_example));
    default:
      break;
  }

  TryCatch(db->Commit(txn, ddl_exe));
  DLOG(INFO) << "DDL commit OK";
  return {RC_TRUE};
}

bench_worker::workload_desc_vec tpcc_worker::get_workload() const {
  workload_desc_vec w;
  // numbers from sigmod.csail.mit.edu:
  // w.push_back(workload_desc("NewOrder", 1.0, TxnNewOrder)); // ~10k ops/sec
  // w.push_back(workload_desc("Payment", 1.0, TxnPayment)); // ~32k ops/sec
  // w.push_back(workload_desc("Delivery", 1.0, TxnDelivery)); // ~104k
  // ops/sec
  // w.push_back(workload_desc("OrderStatus", 1.0, TxnOrderStatus)); // ~33k
  // ops/sec
  // w.push_back(workload_desc("StockLevel", 1.0, TxnStockLevel)); // ~2k
  // ops/sec
  unsigned m = 0;
  for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
    m += g_txn_workload_mix[i];
  ALWAYS_ASSERT(m == 100);
  double base = 100.0;
  if (g_txn_workload_mix[0])
    w.push_back(workload_desc("NewOrder", double(g_txn_workload_mix[0]) / base,
                              TxnNewOrder));
  if (g_txn_workload_mix[1])
    w.push_back(workload_desc("Payment", double(g_txn_workload_mix[1]) / base,
                              TxnPayment));
  if (g_txn_workload_mix[2])
    w.push_back(workload_desc(
        "CreditCheck", double(g_txn_workload_mix[2]) / base, TxnCreditCheck));
  if (g_txn_workload_mix[3])
    w.push_back(workload_desc("Delivery", double(g_txn_workload_mix[3]) / base,
                              TxnDelivery));
  if (g_txn_workload_mix[4])
    w.push_back(workload_desc(
        "OrderStatus", double(g_txn_workload_mix[4]) / base, TxnOrderStatus));
  if (g_txn_workload_mix[5])
    w.push_back(workload_desc(
        "StockLevel", double(g_txn_workload_mix[5]) / base, TxnStockLevel));
  if (g_txn_workload_mix[6])
    w.push_back(workload_desc("Query2", double(g_txn_workload_mix[6]) / base,
                              TxnQuery2));
  if (g_txn_workload_mix[7])
    w.push_back(workload_desc("MicroBenchRandom",
                              double(g_txn_workload_mix[7]) / base,
                              TxnMicroBenchRandom));

  return w;
}

static const std::string get_example_name(uint32_t ddl_example) {
  switch (ddl_example) {
    case 0:
      return "DDL_ADD_COLUMN";
    case 1:
      return "DDL_TABLE_SPLIT";
    case 2:
      return "DDL_PREAGGREGATION";
    case 3:
      return "DDL_CREATE_INDEX";
    case 4:
      return "DDL_TABLE_JOIN";
    case 5:
      return "DDL_ADD_COLUMN_NO_COPY";
    case 6:
      return "DDL_TABLE_SPLIT_NO_COPY";
    case 7:
      return "DDL_PREAGGREGATION_NO_COPY";
    case 8:
      return "DDL_TABLE_JOIN_NO_COPY";
    case 9:
      return "DDL_ADD_CONSTRAINT";
    case 10:
      return "DDL_ADD_COLUMN_AND_CONSTRAINT";
    default:
      LOG(FATAL) << "Not supported";
  }
}

bench_worker::ddl_workload_desc_vec tpcc_worker::get_ddl_workload() const {
  ddl_workload_desc_vec ddl_w;
  for (int i = 0; i < ermia::config::ddl_total; i++) {
    ddl_w.push_back(ddl_workload_desc(get_example_name(ddl_examples[i]), 0, TxnDDL,
                    ddl_examples[i]));
  }
  return ddl_w;
}

#endif  // ADV_COROUTINE
