/**
 * An implementation of TPC-C based off of:
 * https://github.com/oltpbenchmark/oltpbench/tree/master/src/com/oltpbenchmark/benchmarks/tpcc
 */

#ifndef ADV_COROUTINE

#include "tpcc-common.h"

/*volatile bool ddl_running_1 = true;
volatile bool ddl_running_2 = false;
std::atomic<uint64_t> ddl_end(0);
*/
rc_t tpcc_worker::txn_new_order() {
  // printf("new_order begin\n");
#ifdef BLOCKDDL
  db->ReadLock("SCHEMA");
  // db->WriteLock("order_line");
  // db->WriteLock("oorder");
#endif
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
  ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);

  // Read schema tables first
  char str1[] = "order_line", str2[] = "oorder";
  ermia::varstr &k1 = Encode_(str(sizeof(str1)), str1), &k2 = Encode_(str(sizeof(str2)), str2);
  ermia::varstr v1, v2;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID oid = ermia::INVALID_OID;

  schema_index->ReadSchemaTable(txn, rc, k1, v1, &oid);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  rc = rc_t{RC_INVALID};
  oid = ermia::INVALID_OID;
  schema_index->ReadSchemaTable(txn, rc, k2, v2, &oid);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  struct Schema_record order_line_schema;
  memcpy(&order_line_schema, (char *)v1.data(), sizeof(order_line_schema));
  // if (order_line_schema.v != 0) printf("new_order begin\n");
  /*#ifdef DCOPYDDL
    if (order_line_schema.state != 1 && ermia::volatile_read(ddl_running_1)) {
      TryCatch({RC_ABORT_USER});
    }
  #endif*/

  struct Schema_record oorder_schema;
  memcpy(&oorder_schema, (char *)v2.data(), sizeof(oorder_schema));
  // if (oorder_schema.v != 0) printf("ddl new_order begin\n");

  const customer::key k_c(warehouse_id, districtID, customerID);
  customer::value v_c_temp;
  ermia::varstr valptr;

  rc = rc_t{RC_INVALID};
  tbl_customer(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c), valptr);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  const customer::value *v_c = Decode(valptr, v_c_temp);
#ifndef NDEBUG
  checker::SanityCheckCustomer(&k_c, v_c);
#endif

  const warehouse::key k_w(warehouse_id);
  warehouse::value v_w_temp;

  rc = rc_t{RC_INVALID};
  tbl_warehouse(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_w)), k_w), valptr);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  const warehouse::value *v_w = Decode(valptr, v_w_temp);
#ifndef NDEBUG
  checker::SanityCheckWarehouse(&k_w, v_w);
#endif

  const district::key k_d(warehouse_id, districtID);
  district::value v_d_temp;

  rc = rc_t{RC_INVALID};
  tbl_district(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_d)), k_d), valptr);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

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

  const oorder::key k_oo(warehouse_id, districtID, k_no.no_o_id);
  if (oorder_schema.v == 0) {
    oorder::value v_oo;
    v_oo.o_c_id = int32_t(customerID);
    v_oo.o_carrier_id = 0;  // seems to be ignored
    v_oo.o_ol_cnt = int8_t(numItems);
    v_oo.o_all_local = allLocal;
    v_oo.o_entry_d = GetCurrentTimeMillis();

    const size_t oorder_sz = Size(v_oo);
    ermia::OID v_oo_oid = 0;  // Get the OID and put it in oorder_c_id_idx later
    TryCatch(tbl_oorder(warehouse_id)
                 ->InsertRecord(txn, Encode(str(Size(k_oo)), k_oo),
                                Encode(str(oorder_sz), v_oo), &v_oo_oid));

    const oorder_c_id_idx::key k_oo_idx(warehouse_id, districtID, customerID, k_no.no_o_id);
    TryCatch(tbl_oorder_c_id_idx(warehouse_id)
                 ->InsertOID(txn, Encode(str(Size(k_oo_idx)), k_oo_idx), v_oo_oid));
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
    // TryCatch(rc);

    const item::value *v_i = Decode(valptr, v_i_temp);
#ifndef NDEBUG
    checker::SanityCheckItem(&k_i, v_i);
#endif

    const stock::key k_s(ol_supply_w_id, ol_i_id);
    stock::value v_s_temp;

    rc = rc_t{RC_INVALID};
    tbl_stock(ol_supply_w_id)->GetRecord(txn, rc, Encode(str(Size(k_s)), k_s), valptr);
    TryVerifyRelaxed(rc);
    // TryCatch(rc);

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

    // if (k_no.no_o_id > 3000) printf("k_no.no_o_id: %d\n", k_no.no_o_id);
    if (order_line_schema.v == 0) {
      const order_line::key k_ol(warehouse_id, districtID, k_no.no_o_id,
                                 ol_number);
      order_line::value v_ol;
      v_ol.ol_i_id = int32_t(ol_i_id);
      v_ol.ol_delivery_d = 0;  // not delivered yet
      v_ol.ol_amount = float(ol_quantity) * v_i->i_price;
      v_ol.ol_supply_w_id = int32_t(ol_supply_w_id);
      v_ol.ol_quantity = int8_t(ol_quantity);
      v_ol.v = order_line_schema.v;

      sum += v_ol.ol_amount;

      const size_t order_line_sz = Size(v_ol);
      TryCatch(tbl_order_line(warehouse_id)
                   ->InsertRecord(txn, Encode(str(Size(k_ol)), k_ol),
                                  Encode(str(order_line_sz), v_ol)));
    } else {
      const order_line_1::key k_ol_1(warehouse_id, districtID, k_no.no_o_id,
                                     ol_number);

      order_line_1::value v_ol_1;
      v_ol_1.ol_i_id = int32_t(ol_i_id);
      v_ol_1.ol_delivery_d = 0;  // not delivered yet
      v_ol_1.ol_amount = float(ol_quantity) * v_i->i_price;
      v_ol_1.ol_supply_w_id = int32_t(ol_supply_w_id);
      v_ol_1.ol_quantity = int8_t(ol_quantity);
      v_ol_1.v = order_line_schema.v;
      v_ol_1.ol_tax = 0;

      sum += v_ol_1.ol_amount;

      const size_t order_line_sz = Size(v_ol_1);
#ifdef COPYDDL
      ermia::ConcurrentMasstreeIndex *order_line_table_index = (ermia::ConcurrentMasstreeIndex *) order_line_schema.index;
#endif
      TryCatch(order_line_table_index
                   ->InsertRecord(txn, Encode(str(Size(k_ol_1)), k_ol_1),
                                  Encode(str(order_line_sz), v_ol_1)));
#ifdef DCOPYDDL
      if (order_line_schema.state == 1 && ermia::volatile_read(ddl_running_1)) {
        ermia::ConcurrentMasstreeIndex *old_order_line_table_index =
            (ermia::ConcurrentMasstreeIndex *)
                order_line_schema.old_td->GetPrimaryIndex();
        if (order_line_schema.old_v == 0) {
          const order_line::key k_ol(warehouse_id, districtID, k_no.no_o_id,
                                     ol_number);
          order_line::value v_ol;
          v_ol.ol_i_id = int32_t(ol_i_id);
          v_ol.ol_delivery_d = 0; // not delivered yet
          v_ol.ol_amount = float(ol_quantity) * v_i->i_price;
          v_ol.ol_supply_w_id = int32_t(ol_supply_w_id);
          v_ol.ol_quantity = int8_t(ol_quantity);
          v_ol.v = order_line_schema.old_v;

          TryCatch(tbl_order_line(warehouse_id)
                       ->InsertRecord(txn, Encode(str(Size(k_ol)), k_ol),
                                      Encode(str(order_line_sz), v_ol)));
        } else {
          v_ol_1.v = order_line_schema.old_v;
          TryCatch(old_order_line_table_index->InsertRecord(
              txn, Encode(str(Size(k_ol_1)), k_ol_1),
              Encode(str(order_line_sz), v_ol_1)));
        }
      }
#endif
    }
  }

  if (oorder_schema.v != 0) {
    oorder_precompute_aggregate::value v_oo;
    v_oo.o_c_id = int32_t(customerID);
    v_oo.o_carrier_id = 0;  // seems to be ignored
    v_oo.o_ol_cnt = int8_t(numItems);
    v_oo.o_all_local = allLocal;
    v_oo.o_entry_d = GetCurrentTimeMillis();
    v_oo.o_total_amount = sum;

    const size_t oorder_sz = Size(v_oo);
    ermia::OID v_oo_oid = 0;  // Get the OID and put it in oorder_c_id_idx later
#ifdef COPYDDL
    ermia::ConcurrentMasstreeIndex *oorder_table_index =
	    (ermia::ConcurrentMasstreeIndex *) oorder_schema.index;
    ermia::ConcurrentMasstreeIndex *oorder_table_secondary_index = 
	    (ermia::ConcurrentMasstreeIndex *) (*((oorder_schema.td)->GetSecIndexes().begin()));
#endif
    TryCatch(oorder_table_index
                 ->InsertRecord(txn, Encode(str(Size(k_oo)), k_oo),
                                Encode(str(oorder_sz), v_oo), &v_oo_oid));

    const oorder_c_id_idx::key k_oo_idx(warehouse_id, districtID, customerID, k_no.no_o_id);
    TryCatch(oorder_table_secondary_index
                 ->InsertOID(txn, Encode(str(Size(k_oo_idx)), k_oo_idx), v_oo_oid));
  }

  TryCatch(db->Commit(txn));
  // printf("new_order commit ok\n");
  // if (order_line_schema.v != 0) printf("ddl new_order commit ok\n");
#ifdef BLOCKDDL
  db->ReadUnlock("SCHEMA");
  // db->WriteUnlock("order_line");
  // db->WriteUnlock("oorder");
#endif
  return {RC_TRUE};
}  // new-order

rc_t tpcc_worker::txn_payment() {
  // printf("payment begin\n");
#ifdef BLOCKDDL
  db->ReadLock("SCHEMA");
  // db->WriteLock("order_line");
#endif
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
  ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);

  // Read schema tables first
  char str1[] = "order_line";
  ermia::varstr &k1 = Encode_(str(sizeof(str1)), str1);
  ermia::varstr v1;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID oid = ermia::INVALID_OID;

  schema_index->ReadSchemaTable(txn, rc, k1, v1, &oid);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  struct Schema_record schema;
  memcpy(&schema, (char *)v1.data(), sizeof(schema));

  rc = rc_t{RC_INVALID};
  ermia::varstr valptr;
  const warehouse::key k_w(warehouse_id);
  warehouse::value v_w_temp;

  tbl_warehouse(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_w)), k_w), valptr);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

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
  tbl_district(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_d)), k_d), valptr);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

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
      auto iter =
          ermia::ConcurrentMasstree::ScanIterator</*IsRerverse=*/false>::factory(
              &tbl_customer_name_idx(customerWarehouseID)->GetMasstree(),
              txn->GetXIDContext(), Encode(str(Size(k_c_idx_0)), k_c_idx_0),
              &Encode(str(Size(k_c_idx_1)), k_c_idx_1));
      ermia::dbtuple* tuple = nullptr;
      bool more = iter.init_or_next</*IsNext=*/false>();
      while (more) {
        tuple = ermia::oidmgr->oid_get_version(
            iter.tuple_array(), iter.value(), txn->GetXIDContext());
        if (tuple) {
          rc = txn->DoTupleRead(tuple, &valptr);
          if (rc._val == RC_TRUE) {
            c.Invoke(iter.key().data(), iter.key().length(), valptr);
          }
        }
        more = iter.init_or_next</*IsNext=*/true>();
      }
    } else {
      TryCatch(tbl_customer_name_idx(customerWarehouseID)
                   ->Scan(txn, Encode(str(Size(k_c_idx_0)), k_c_idx_0),
                          &Encode(str(Size(k_c_idx_1)), k_c_idx_1), c));
    }

    if (c.size() <= 0) printf("customer c null in payment\n");
    ALWAYS_ASSERT(c.size() > 0);
    ASSERT(c.size() < NMaxCustomerIdxScanElems);  // we should detect this
    int index = c.size() / 2;
    if (c.size() % 2 == 0) index--;

    Decode(*c.values[index].second, v_c);
    k_c.c_w_id = customerWarehouseID;
    k_c.c_d_id = customerDistrictID;
    k_c.c_id = v_c.c_id;
  } else {
    // cust by ID
    const uint customerID = GetCustomerId(r);
    k_c.c_w_id = customerWarehouseID;
    k_c.c_d_id = customerDistrictID;
    k_c.c_id = customerID;
    rc = rc_t{RC_INVALID};
    tbl_customer(customerWarehouseID)->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c), valptr);
    TryVerifyRelaxed(rc);
    // TryCatch(rc);
    Decode(valptr, v_c);
  }
#ifndef NDEBUG
  checker::SanityCheckCustomer(&k_c, &v_c);
#endif
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

  TryCatch(tbl_customer(customerWarehouseID)
               ->UpdateRecord(txn, Encode(str(Size(k_c)), k_c),
                              Encode(str(Size(v_c_new)), v_c_new)));

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
  // printf("new_order commit ok\n");
#ifdef BLOCKDDL
  db->ReadUnlock("SCHEMA");
  // db->WriteUnlock("order_line");
#endif
  return {RC_TRUE};
}

rc_t tpcc_worker::txn_delivery() {
  // printf("delivery begin\n");
#ifdef BLOCKDDL
  db->ReadLock("SCHEMA");
  // db->WriteLock("order_line");
  // db->WriteLock("oorder");
#endif
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
  ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);

  // Read schema tables first 
  char str1[] = "order_line", str2[] = "oorder";
  ermia::varstr &k1 = Encode_(str(sizeof(str1)), str1), &k2 = Encode_(str(sizeof(str2)), str2);
  ermia::varstr v1, v2;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID oid = ermia::INVALID_OID;

  schema_index->ReadSchemaTable(txn, rc, k1, v1, &oid);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  rc = rc_t{RC_INVALID};
  oid = ermia::INVALID_OID;
  schema_index->ReadSchemaTable(txn, rc, k2, v2, &oid);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  struct Schema_record order_line_schema;
  memcpy(&order_line_schema, (char *)v1.data(), sizeof(order_line_schema));
  // if (order_line_schema.v != 0) printf("delivery begin\n");
  /*#ifdef DCOPYDDL
    if (order_line_schema.state != 1 && ermia::volatile_read(ddl_running_1)) {
      TryCatch({RC_ABORT_USER});
    }
  #endif*/

  struct Schema_record oorder_schema;
  memcpy(&oorder_schema, (char *)v2.data(), sizeof(oorder_schema));

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

#ifdef COPYDDL
    ermia::ConcurrentMasstreeIndex *oorder_table_index = (ermia::ConcurrentMasstreeIndex *) oorder_schema.index;
#endif

    const oorder::key k_oo(warehouse_id, d, k_no->no_o_id);
    // even if we read the new order entry, there's no guarantee
    // we will read the oorder entry: in this case the txn will abort,
    // but we're simply bailing out early
    oorder::value v_oo_temp;
    oorder_precompute_aggregate::value v_oo_pa_temp;
    ermia::varstr valptr;

    const oorder::value *v_oo = nullptr;
    const oorder_precompute_aggregate::value *v_oo_pa = nullptr;

    rc = rc_t{RC_INVALID};
    if (oorder_schema.v == 0) {
      tbl_oorder(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_oo)), k_oo), valptr);
      TryCatchCondAbort(rc);
      v_oo = Decode(valptr, v_oo_temp);
#ifndef NDEBUG
      checker::SanityCheckOOrder(&k_oo, v_oo);
#endif
    } else {
#ifdef COPYDDL
      ermia::ConcurrentMasstreeIndex *oorder_table_index = (ermia::ConcurrentMasstreeIndex *) oorder_schema.index;
#endif
      oorder_table_index->GetRecord(txn, rc, Encode(str(Size(k_oo)), k_oo), valptr);
      TryCatchCondAbort(rc);
      v_oo_pa = Decode(valptr, v_oo_pa_temp);
    }

    static_limit_callback<15> c(
        s_arena.get(), false);  // never more than 15 order_lines per order
    const order_line::key k_oo_0(warehouse_id, d, k_no->no_o_id, 0);
    const order_line::key k_oo_1(warehouse_id, d, k_no->no_o_id,
                                 std::numeric_limits<int32_t>::max());

#ifdef COPYDDL
    ermia::ConcurrentMasstreeIndex *order_line_table_index = (ermia::ConcurrentMasstreeIndex *) order_line_schema.index;
#endif

#ifdef DCOPYDDL
    ermia::ConcurrentMasstreeIndex *old_order_line_table_index =
        (ermia::ConcurrentMasstreeIndex *)
            order_line_schema.old_td->GetPrimaryIndex();
#endif

    if (order_line_schema.v == 0) {
      // XXX(stephentu): mutable scans would help here
      TryCatch(tbl_order_line(warehouse_id)
                   ->Scan(txn, Encode(str(Size(k_oo_0)), k_oo_0),
                          &Encode(str(Size(k_oo_1)), k_oo_1), c));
    } else {
      if (order_line_schema.state == 1 && ermia::volatile_read(ddl_running_1)) {
#ifdef DCOPYDDL
        TryCatch(old_order_line_table_index->Scan(
            txn, Encode(str(Size(k_oo_0)), k_oo_0),
            &Encode(str(Size(k_oo_1)), k_oo_1), c));
#endif
      } else {

        static_limit_callback<15> c1(s_arena.get(), false);
        TryCatch(tbl_order_line(warehouse_id)
                     ->Scan(txn, Encode(str(Size(k_oo_0)), k_oo_0),
                            &Encode(str(Size(k_oo_1)), k_oo_1), c1));

        TryCatch(order_line_table_index->Scan(
            txn, Encode(str(Size(k_oo_0)), k_oo_0),
            &Encode(str(Size(k_oo_1)), k_oo_1), c));

        if ((c1.size() != 0 && c.size() != c1.size())) {
          // printf("delivery abort\n");
#ifdef LAZYDDL
        ermia::ConcurrentMasstreeIndex *old_order_line_table_index =
            (ermia::ConcurrentMasstreeIndex *)order_line_schema.old_index;

        order_line_nop_callback c_order_line_1(order_line_schema.v - 1, true,
                                               txn, s_arena.get(),
                                               order_line_table_index);

        TryCatch(old_order_line_table_index->Scan(
            txn, Encode(str(Size(k_oo_0)), k_oo_0),
            &Encode(str(Size(k_oo_1)), k_oo_1), c_order_line_1));

        TryCatch(c_order_line_1.invoke_status);

        if (c1.size() != c_order_line_1.n) {
          TryCatch({RC_ABORT_USER});
        }
#else
        TryCatch({RC_ABORT_USER});
#endif
      }
      }
    }

    float sum = 0.0;
    for (size_t i = 0; i < c.size(); i++) {
      if (order_line_schema.v == 0) {
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
        v_ol_new.v = order_line_schema.v;
	ASSERT(s_arena.get()->manages(c.values[i].first));
        TryCatch(tbl_order_line(warehouse_id)
                     ->UpdateRecord(txn, *c.values[i].first,
                                    Encode(str(Size(v_ol_new)), v_ol_new)));

      } else {
        order_line_1::value v_ol_temp;
        const order_line_1::value *v_ol = Decode(*c.values[i].second, v_ol_temp);

#ifndef NDEBUG
        order_line_1::key k_ol_temp;
        const order_line_1::key *k_ol = Decode(*c.values[i].first, k_ol_temp);
        // checker::SanityCheckOrderLine(k_ol, v_ol);
#endif

        sum += v_ol->ol_amount;
        order_line_1::value v_ol_new(*v_ol);
        v_ol_new.ol_delivery_d = ts;
	v_ol_new.v = order_line_schema.v;
        v_ol_new.ol_tax = 0;
        ASSERT(s_arena.get()->manages(c.values[i].first));
#ifdef LAZYDDL
	TryCatch(order_line_table_index
                      ->UpdateRecord(txn, *c.values[i].first,
                            Encode(str(Size(v_ol_new)), v_ol_new),
				    order_line_schema.old_td));
#else
        TryCatch(order_line_table_index->UpdateRecord(
            txn, *c.values[i].first, Encode(str(Size(v_ol_new)), v_ol_new)));
#endif

#ifdef DCOPYDDL
        if (order_line_schema.state == 1 &&
            ermia::volatile_read(ddl_running_1)) {
          ermia::ConcurrentMasstreeIndex *old_order_line_table_index =
              (ermia::ConcurrentMasstreeIndex *)
                  order_line_schema.old_td->GetPrimaryIndex();
          if (order_line_schema.old_v == 0) {
            order_line::value v_ol_temp;
            const order_line::value *v_ol =
                Decode(*c.values[i].second, v_ol_temp);

            order_line::value v_ol_new(*v_ol);
            v_ol_new.ol_delivery_d = ts;
            v_ol_new.v = order_line_schema.old_v;
            ASSERT(s_arena.get()->manages(c.values[i].first));
            TryCatch(tbl_order_line(warehouse_id)
                         ->UpdateRecord(txn, *c.values[i].first,
                                        Encode(str(Size(v_ol_new)), v_ol_new)));
          } else {
            v_ol_new.v = order_line_schema.old_v;
            TryCatch(old_order_line_table_index->UpdateRecord(
                txn, *c.values[i].first,
                Encode(str(Size(v_ol_new)), v_ol_new)));
          }
        }
#endif
      }
    }

    // delete new order
    TryCatch(tbl_new_order(warehouse_id)
                 ->RemoveRecord(txn, Encode(str(Size(*k_no)), *k_no)));

    // update oorder
    if (oorder_schema.v == 0) {
      oorder::value v_oo_new(*v_oo);
      v_oo_new.o_carrier_id = o_carrier_id;
      TryCatch(tbl_oorder(warehouse_id)
                   ->UpdateRecord(txn, Encode(str(Size(k_oo)), k_oo),
                                  Encode(str(Size(v_oo_new)), v_oo_new)));
    } else {
      oorder_precompute_aggregate::value v_oo_pa_new(*v_oo_pa);
      v_oo_pa_new.o_carrier_id = o_carrier_id;
#ifdef COPYDDL
      ermia::ConcurrentMasstreeIndex *oorder_table_index = (ermia::ConcurrentMasstreeIndex *) oorder_schema.index;
#endif
#ifdef LAZYDDL
      TryCatch(oorder_table_index
                    ->UpdateRecord(txn, Encode(str(Size(k_oo)), k_oo),
                          Encode(str(Size(v_oo_pa_new)), v_oo_pa_new),
				  oorder_schema.old_td));
#else
      TryCatch(oorder_table_index
                   ->UpdateRecord(txn, Encode(str(Size(k_oo)), k_oo),
                                  Encode(str(Size(v_oo_pa_new)), v_oo_pa_new)));
#endif
    }

    uint c_id;

    if (oorder_schema.v == 0) {
      c_id = v_oo->o_c_id;
    } else {
      c_id = v_oo_pa->o_c_id;
    }

    const float ol_total = sum;

    // update customer
    const customer::key k_c(warehouse_id, d, c_id);
    customer::value v_c_temp;

    rc = rc_t{RC_INVALID};
    tbl_customer(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c), valptr);
    TryVerifyRelaxed(rc);
    // TryCatch(rc);

    const customer::value *v_c = Decode(valptr, v_c_temp);
    customer::value v_c_new(*v_c);
    v_c_new.c_balance += ol_total;
    TryCatch(tbl_customer(warehouse_id)
                 ->UpdateRecord(txn, Encode(str(Size(k_c)), k_c),
                                Encode(str(Size(v_c_new)), v_c_new)));
  }
  TryCatch(db->Commit(txn));
  // printf("delivery commit ok\n");
  // if (order_line_schema.v != 0) printf("delivery commit ok\n");
#ifdef BLOCKDDL
  db->ReadUnlock("SCHEMA");
  // db->WriteUnlock("order_line");
  // db->WriteUnlock("oorder");
#endif
  return {RC_TRUE};
}

rc_t tpcc_worker::txn_order_status() {
  // printf("order status begin\n");
#ifdef BLOCKDDL
  db->ReadLock("SCHEMA");
  // db->WriteLock("order_line");
  // db->ReadLock("oorder");
#endif
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

  // Read schema tables first
  char str1[] = "order_line", str2[] = "oorder";
  ermia::varstr &k1 = Encode_(str(sizeof(str1)), str1), &k2 = Encode_(str(sizeof(str2)), str2);
  ermia::varstr v1, v2;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID oid = ermia::INVALID_OID;

  schema_index->ReadSchemaTable(txn, rc, k1, v1, &oid);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  rc = rc_t{RC_INVALID};
  oid = ermia::INVALID_OID;
  schema_index->ReadSchemaTable(txn, rc, k2, v2, &oid);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  struct Schema_record order_line_schema;
  memcpy(&order_line_schema, (char *)v1.data(), sizeof(order_line_schema));

  struct Schema_record oorder_schema;
  memcpy(&oorder_schema, (char *)v2.data(), sizeof(oorder_schema));

  customer::key k_c;
  customer::value v_c;
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
    TryCatch(tbl_customer_name_idx(warehouse_id)
                 ->Scan(txn, Encode(str(Size(k_c_idx_0)), k_c_idx_0),
                        &Encode(str(Size(k_c_idx_1)), k_c_idx_1), c));
    
    if (c.size() <= 0) printf("customer c null in order status\n");
    ALWAYS_ASSERT(c.size() > 0);
    ASSERT(c.size() < NMaxCustomerIdxScanElems);  // we should detect this
    int index = c.size() / 2;
    if (c.size() % 2 == 0) index--;

    Decode(*c.values[index].second, v_c);
    k_c.c_w_id = warehouse_id;
    k_c.c_d_id = districtID;
    k_c.c_id = v_c.c_id;
  } else {
    // cust by ID
    const uint customerID = GetCustomerId(r);
    k_c.c_w_id = warehouse_id;
    k_c.c_d_id = districtID;
    k_c.c_id = customerID;

    rc = rc_t{RC_INVALID};
    tbl_customer(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c), valptr);
    TryVerifyRelaxed(rc);
    // TryCatch(rc);

    Decode(valptr, v_c);
  }
#ifndef NDEBUG
  checker::SanityCheckCustomer(&k_c, &v_c);
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
      if (oorder_schema.v == 0) {
	TryCatch(tbl_oorder_c_id_idx(warehouse_id)
               ->Scan(txn, Encode(str(Size(k_oo_idx_0)), k_oo_idx_0),
                      &Encode(str(Size(k_oo_idx_1)), k_oo_idx_1), c_oorder));
      } else {
#ifdef COPYDDL
	ermia::ConcurrentMasstreeIndex *oorder_table_secondary_index = 
            (ermia::ConcurrentMasstreeIndex *) (*((oorder_schema.td)->GetSecIndexes().begin()));
#endif
        TryCatch(oorder_table_secondary_index
               ->Scan(txn, Encode(str(Size(k_oo_idx_0)), k_oo_idx_0),
                      &Encode(str(Size(k_oo_idx_1)), k_oo_idx_1), c_oorder));
      }
    }
    /*if (!c_oorder.size()) {
      TryCatch({RC_ABORT_INTERNAL});
    }*/
    if (c_oorder.size() == 0)
      printf("c_oorder.size() == 0");
    ALWAYS_ASSERT(c_oorder.size());
  } else {
    latest_key_callback c_oorder(*newest_o_c_id, 1);
    const oorder_c_id_idx::key k_oo_idx_hi(warehouse_id, districtID, k_c.c_id,
                                           std::numeric_limits<int32_t>::max());
    if (oorder_schema.v == 0) {
      TryCatch(tbl_oorder_c_id_idx(warehouse_id)
                   ->ReverseScan(txn, Encode(str(Size(k_oo_idx_hi)), k_oo_idx_hi),
                                 nullptr, c_oorder));
    } else {
#ifdef COPYDDL
      ermia::ConcurrentMasstreeIndex *oorder_table_secondary_index =
          (ermia::ConcurrentMasstreeIndex *) (*((oorder_schema.td)->GetSecIndexes().begin()));
#endif
      TryCatch(oorder_table_secondary_index
                   ->ReverseScan(txn, Encode(str(Size(k_oo_idx_hi)), k_oo_idx_hi),
                                 nullptr, c_oorder));
    }
    /*if (c_oorder.size() != 1) {
      TryCatch({RC_ABORT_INTERNAL});
    }*/
    ALWAYS_ASSERT(c_oorder.size() == 1);
  }

  oorder_c_id_idx::key k_oo_idx_temp;
  const oorder_c_id_idx::key *k_oo_idx = Decode(*newest_o_c_id, k_oo_idx_temp);
  const uint o_id = k_oo_idx->o_o_id;

  order_line_nop_callback c_order_line(order_line_schema.state == 1
                                           ? order_line_schema.old_v
                                           : order_line_schema.v,
                                       false, txn, s_arena.get(), nullptr);
  const order_line::key k_ol_0(warehouse_id, districtID, o_id, 0);
  const order_line::key k_ol_1(warehouse_id, districtID, o_id,
                               std::numeric_limits<int32_t>::max());

  if (order_line_schema.v == 0) {
    TryCatch(tbl_order_line(warehouse_id)
                 ->Scan(txn, Encode(str(Size(k_ol_0)), k_ol_0),
                        &Encode(str(Size(k_ol_1)), k_ol_1), c_order_line));
    ALWAYS_ASSERT(c_order_line.n >= 5 && c_order_line.n <= 15);
  } else {
#ifdef COPYDDL
    ermia::ConcurrentMasstreeIndex *order_line_table_index = (ermia::ConcurrentMasstreeIndex *) order_line_schema.index;
#endif

#ifdef DCOPYDDL
    ermia::ConcurrentMasstreeIndex *old_order_line_table_index =
        (ermia::ConcurrentMasstreeIndex *)
            order_line_schema.old_td->GetPrimaryIndex();
#endif

    if (order_line_schema.state == 1 && ermia::volatile_read(ddl_running_1)) {
#ifdef DCOPYDDL
      TryCatch(old_order_line_table_index->Scan(
          txn, Encode(str(Size(k_ol_0)), k_ol_0),
          &Encode(str(Size(k_ol_1)), k_ol_1), c_order_line));
#endif
    } else {
      TryCatch(order_line_table_index->Scan(
          txn, Encode(str(Size(k_ol_0)), k_ol_0),
          &Encode(str(Size(k_ol_1)), k_ol_1), c_order_line));
    }

    if (c_order_line.n < 5 || c_order_line.n > 15) {
#ifdef LAZYDDL
      ermia::ConcurrentMasstreeIndex *old_order_line_table_index =
          (ermia::ConcurrentMasstreeIndex *)order_line_schema.old_index;

      order_line_nop_callback c_order_line_1(order_line_schema.v - 1, true, txn,
                                             s_arena.get(),
                                             order_line_table_index);

      TryCatch(old_order_line_table_index->Scan(
          txn, Encode(str(Size(k_ol_0)), k_ol_0),
          &Encode(str(Size(k_ol_1)), k_ol_1), c_order_line_1));

      TryCatch(c_order_line_1.invoke_status);

      if (c_order_line_1.n < 5 || c_order_line_1.n > 15) {
        printf("c_order_line_1.n < 5 || c_order_line_1.n > 15\n");
      }
      ALWAYS_ASSERT(c_order_line_1.n >= 5 && c_order_line_1.n <= 15);

      /*order_line_nop_callback c_order_line_2(order_line_schema.v, false, txn,
                                       s_arena.get(), nullptr);

      TryCatch(order_line_table_index
                 ->Scan(txn, Encode(str(Size(k_ol_0)), k_ol_0),
                        &Encode(str(Size(k_ol_1)), k_ol_1), c_order_line_2));

      TryCatch(c_order_line_2.invoke_status);

      if (c_order_line_2.n < 5 || c_order_line_2.n > 15) {
        printf("c_order_line_2.n < 5 || c_order_line_2.n > 15\n");
      }
      ALWAYS_ASSERT(c_order_line_2.n >= 5 && c_order_line_2.n <= 15);
      */
      // TryCatch({RC_ABORT_USER});
#else
      // printf("c_order_line.n < 5 || c_order_line.n > 15\n");
      // ALWAYS_ASSERT(c_order_line.n >= 5 && c_order_line.n <= 15);
      TryCatch({RC_ABORT_USER});
#endif
    }
  }

  /*if (c_order_line.n < 5 || c_order_line.n > 15) {
    // printf("c_order_line.n: %zu, w: %d, d: %d, o_id: %d\n", c_order_line.n,
warehouse_id, districtID, o_id);
    // printf("txn begin: %lu\n", txn->GetXIDContext()->begin);
#ifdef COPYDDL
    ermia::ConcurrentMasstreeIndex *order_line_table_index =
(ermia::ConcurrentMasstreeIndex *) order_line_schema.index; const
order_line::key k_ol_test(2, 7, 2, 2); ermia::varstr valptr; rc =
rc_t{RC_INVALID}; order_line_table_index->GetRecord(txn, rc,
Encode(str(Size(k_ol_test)), k_ol_test), valptr); if (rc._val != RC_TRUE) {
      printf("Yes, no value got\n");
    } else {
      printf("value got\n");
    }
#endif
    TryCatch({RC_ABORT_USER});
    // TryCatch({RC_FALSE});
  }*/
  // ALWAYS_ASSERT(c_order_line.n >= 5 && c_order_line.n <= 15);

  TryCatch(db->Commit(txn));
  // printf("order_status commit ok\n");
  // if (oorder_schema.v != 0) printf("order_status commit ok\n");
#ifdef BLOCKDDL
  db->ReadUnlock("SCHEMA");
  // db->WriteUnlock("order_line");
  // db->ReadUnlock("oorder");
#endif
  return {RC_TRUE};
}

rc_t tpcc_worker::txn_stock_level() {
  // printf("stock level begin\n");
#ifdef BLOCKDDL
  db->ReadLock("SCHEMA");
  // db->WriteLock("order_line");
#endif
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

  // Read schema tables first 
  char str1[] = "order_line";
  ermia::varstr &k1 = Encode_(str(sizeof(str1)), str1);
  ermia::varstr v1;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID oid = ermia::INVALID_OID;

  schema_index->ReadSchemaTable(txn, rc, k1, v1, &oid);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  struct Schema_record order_line_schema;
  memcpy(&order_line_schema, (char *)v1.data(), sizeof(order_line_schema));
  // if (schema.v != 0) printf("stock_level start\n");

  // NB: since txn_stock_level() is a RO txn, we assume that
  // locking is un-necessary (since we can just read from some old snapshot)
  const district::key k_d(warehouse_id, districtID);
  district::value v_d_temp;
  ermia::varstr valptr;

  rc = rc_t{RC_INVALID};
  tbl_district(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_d)), k_d), valptr);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  const district::value *v_d = Decode(valptr, v_d_temp);
#ifndef NDEBUG
  checker::SanityCheckDistrict(&k_d, v_d);
#endif

  const uint64_t cur_next_o_id =
      g_new_order_fast_id_gen
      ? NewOrderIdHolder(warehouse_id, districtID)
          .load(std::memory_order_acquire)
      : v_d->d_next_o_id;

  // manual joins are fun!
  order_line_scan_callback c(order_line_schema.state == 1
                                 ? order_line_schema.old_v
                                 : order_line_schema.v,
                             false, txn, s_arena.get(), nullptr);
  const int32_t lower = cur_next_o_id >= 20 ? (cur_next_o_id - 20) : 0;
  const order_line::key k_ol_0(warehouse_id, districtID, lower, 0);
  const order_line::key k_ol_1(warehouse_id, districtID, cur_next_o_id, 0);
  if (order_line_schema.v == 0) {
    TryCatch(tbl_order_line(warehouse_id)
                 ->Scan(txn, Encode(str(Size(k_ol_0)), k_ol_0),
                        &Encode(str(Size(k_ol_1)), k_ol_1), c));
  } else {
#ifdef COPYDDL
    ermia::ConcurrentMasstreeIndex *order_line_table_index = (ermia::ConcurrentMasstreeIndex *) order_line_schema.index;
#endif

#ifdef DCOPYDDL
    ermia::ConcurrentMasstreeIndex *old_order_line_table_index =
        (ermia::ConcurrentMasstreeIndex *)
            order_line_schema.old_td->GetPrimaryIndex();
#endif

    if (order_line_schema.state == 1 && ermia::volatile_read(ddl_running_1)) {
#ifdef DCOPYDDL
      TryCatch(old_order_line_table_index->Scan(
          txn, Encode(str(Size(k_ol_0)), k_ol_0),
          &Encode(str(Size(k_ol_1)), k_ol_1), c));
#endif
    } else {

      order_line_scan_callback c1(order_line_schema.v - 1, false, txn,
                                  s_arena.get(), nullptr);
      TryCatch(tbl_order_line(warehouse_id)
                   ->Scan(txn, Encode(str(Size(k_ol_0)), k_ol_0),
                          &Encode(str(Size(k_ol_1)), k_ol_1), c1));

      TryCatch(
          order_line_table_index->Scan(txn, Encode(str(Size(k_ol_0)), k_ol_0),
                                       &Encode(str(Size(k_ol_1)), k_ol_1), c));

      if ((c1.n != 0 && c.n != c1.n)) {
        // printf("stock abort\n");
#ifdef LAZYDDL
      ermia::ConcurrentMasstreeIndex *old_order_line_table_index =
          (ermia::ConcurrentMasstreeIndex *)order_line_schema.old_index;

      order_line_scan_callback c2(order_line_schema.v - 1, true, txn,
                                  s_arena.get(), order_line_table_index);

      TryCatch(old_order_line_table_index->Scan(
          txn, Encode(str(Size(k_ol_0)), k_ol_0),
          &Encode(str(Size(k_ol_1)), k_ol_1), c2));

      if (c1.n != c2.n) {
        TryCatch({RC_ABORT_USER});
      }
#else
      TryCatch({RC_ABORT_USER});
#endif
    }
    }
  }
  {
    std::unordered_map<uint, bool> s_i_ids_distinct;
    for (auto &p : c.s_i_ids) {
      const stock::key k_s(warehouse_id, p.first);
      stock::value v_s;
      ASSERT(p.first >= 1 && p.first <= NumItems());

      rc = rc_t{RC_INVALID};
      tbl_stock(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_s)), k_s), valptr);
      TryVerifyRelaxed(rc);
      // TryCatch(rc);

      const uint8_t *ptr = (const uint8_t *)valptr.data();
      int16_t i16tmp;
      ptr = serializer<int16_t, true>::read(ptr, &i16tmp);
      if (i16tmp < int(threshold)) s_i_ids_distinct[p.first] = 1;
    }
    // NB(stephentu): s_i_ids_distinct.size() is the computed result of this txn
  }
  TryCatch(db->Commit(txn));
  // printf("stock_level commit ok\n");
#ifdef BLOCKDDL
  db->ReadUnlock("SCHEMA");
  // db->WriteUnlock("order_line");
#endif
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

  // printf("credit check begin\n");
#ifdef BLOCKDDL
  db->ReadLock("SCHEMA");
  // db->WriteLock("order_line");
  // db->ReadLock("oorder");
#endif
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

  ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);

  // Read schema tables first 
  char str1[] = "order_line", str2[] = "oorder";
  ermia::varstr &k1 = Encode_(str(sizeof(str1)), str1), &k2 = Encode_(str(sizeof(str2)), str2);
  ermia::varstr v1, v2;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID oid = ermia::INVALID_OID;

  schema_index->ReadSchemaTable(txn, rc, k1, v1, &oid);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  rc = rc_t{RC_INVALID};
  oid = ermia::INVALID_OID;
  schema_index->ReadSchemaTable(txn, rc, k2, v2, &oid);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  struct Schema_record order_line_schema;
  memcpy(&order_line_schema, (char *)v1.data(), sizeof(order_line_schema));

  struct Schema_record oorder_schema;
  memcpy(&oorder_schema, (char *)v2.data(), sizeof(oorder_schema));
  if (oorder_schema.v != 0) printf("credit check begin\n");

  // select * from customer with random C_ID
  customer::key k_c;
  customer::value v_c_temp;
  ermia::varstr valptr;
  const uint customerID = GetCustomerId(r);
  k_c.c_w_id = customerWarehouseID;
  k_c.c_d_id = customerDistrictID;
  k_c.c_id = customerID;

  rc = rc_t{RC_INVALID};
  tbl_customer(customerWarehouseID)->GetRecord(txn, rc, Encode(str(Size(k_c)), k_c), valptr);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  const customer::value *v_c = Decode(valptr, v_c_temp);
#ifndef NDEBUG
  checker::SanityCheckCustomer(&k_c, v_c);
#endif

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
  if (c_no.output.size() == 0) printf("new order size null in credit\n");
  ALWAYS_ASSERT(c_no.output.size());

  double sum = 0;

  if (oorder_schema.v == 0) {
    for (auto &k : c_no.output) {
      new_order::key k_no_temp;
      const new_order::key *k_no = Decode(*k, k_no_temp);

      const oorder::key k_oo(warehouse_id, districtID, k_no->no_o_id);
      oorder::value v;
      rc = rc_t{RC_INVALID};
      tbl_oorder(warehouse_id)->GetRecord(txn, rc, Encode(str(Size(k_oo)), k_oo), valptr);
      TryCatchCond(rc, continue);
      auto *vv = Decode(valptr, v);

      // Order line scan
      //		ol_d_id = :d_id
      //		ol_w_id = :w_id
      //		ol_o_id = o_id
      //		ol_number = 1-15
      credit_check_order_line_scan_callback c_ol(order_line_schema.v);
      const order_line::key k_ol_0(warehouse_id, districtID, k_no->no_o_id, 1);
      const order_line::key k_ol_1(warehouse_id, districtID, k_no->no_o_id, 15);

      if (order_line_schema.v == 0) {
        TryCatch(tbl_order_line(warehouse_id)
                     ->Scan(txn, Encode(str(Size(k_ol_0)), k_ol_0),
                            &Encode(str(Size(k_ol_1)), k_ol_1), c_ol));
      } else {
#ifdef COPYDDL
        ermia::ConcurrentMasstreeIndex *order_line_table_index = (ermia::ConcurrentMasstreeIndex *) order_line_schema.index;
#endif
        TryCatch(order_line_table_index
                     ->Scan(txn, Encode(str(Size(k_ol_0)), k_ol_0),
                            &Encode(str(Size(k_ol_1)), k_ol_1), c_ol));
      }

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
      const oorder_precompute_aggregate::key k_oo(warehouse_id, districtID, k_no->no_o_id);
      oorder_precompute_aggregate::value v;
      rc = rc_t{RC_INVALID};
#ifdef COPYDDL
      ermia::ConcurrentMasstreeIndex *oorder_table_index = (ermia::ConcurrentMasstreeIndex *) oorder_schema.index;
#endif
#ifdef LAZYDDL
      oorder_table_index->GetRecord(txn, rc, Encode(str(Size(k_oo)), k_oo), valptr,
		                    nullptr, oorder_schema.old_td);
#else
      oorder_table_index->GetRecord(txn, rc, Encode(str(Size(k_oo)), k_oo), valptr);
#endif
      TryCatchCond(rc, continue);
      auto *vv = Decode(valptr, v);
      sum += vv->o_total_amount;
    }
    printf("credit check sum end\n");
  }

  // c_credit update
  customer::value v_c_new(*v_c);
  if (v_c_new.c_balance + sum >= 5000)  // Threshold = 5K
    v_c_new.c_credit.assign("BC");
  else
    v_c_new.c_credit.assign("GC");
  TryCatch(tbl_customer(customerWarehouseID)
               ->UpdateRecord(txn, Encode(str(Size(k_c)), k_c),
                              Encode(str(Size(v_c_new)), v_c_new)));

  TryCatch(db->Commit(txn));
  // printf("credit check commit ok\n");
  // if (oorder_schema.v != 0) printf("credit check commit ok\n");
#ifdef BLOCKDDL
  db->ReadUnlock("SCHEMA");
  // db->WriteUnlock("order_line");
  // db->ReadUnlock("oorder");
#endif
  return {RC_TRUE};
}

rc_t tpcc_worker::txn_query2() {
  // TODO(yongjunh): use TXN_FLAG_READ_MOSTLY once SSN's and SSI's read optimization are available.
  // printf("query2 begin\n");
#ifdef BLOCKDDL
  db->ReadLock("SCHEMA");
  // db->WriteLock("order_line");
#endif
  ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
  ermia::scoped_str_arena s_arena(arena);

  // Read schema tables first 
  char str1[] = "order_line";
  ermia::varstr &k1 = Encode_(str(sizeof(str1)), str1);
  ermia::varstr v1;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID oid = ermia::INVALID_OID;

  schema_index->ReadSchemaTable(txn, rc, k1, v1, &oid);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  struct Schema_record schema;
  memcpy(&schema, (char *)v1.data(), sizeof(schema));
  // if (schema.v != 0) printf("query2 start\n");

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

        rc = rc_t{RC_INVALID};
        tbl_supplier(1)->GetRecord(txn, rc, Encode(str(Size(k_su)), k_su), valptr);
        TryVerifyRelaxed(rc);
        // TryCatch(rc);

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
          tbl_stock(it.first)->GetRecord(txn, rc, Encode(str(Size(k_s)), k_s), valptr);
          TryVerifyRelaxed(rc);
          // TryCatch(rc);

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
        // TryCatch(rc);

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
  // printf("query2 commit ok\n");
#ifdef BLOCKDDL
  db->ReadUnlock("SCHEMA");
  // db->WriteUnlock("order_line");
#endif
  return {RC_TRUE};
}

rc_t tpcc_worker::txn_microbench_random() {
  ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_DML, *arena, txn_buf());
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

ermia::varstr *add_column_op(const char *keyp,
		             size_t keylen, 
		             const ermia::varstr &value,
		             uint64_t schema_version,
                             ermia::transaction *txn,
		             ermia::str_arena *arena,
		             ermia::OrderedIndex *index) {
  order_line::value v_ol_temp;
  const order_line::value *v_ol = Decode(value, v_ol_temp);

  order_line_1::value v_ol_1;
  v_ol_1.ol_i_id = v_ol->ol_i_id;
  printf("v_ol->ol_i_id: %u\n", v_ol->ol_i_id);
  v_ol_1.ol_delivery_d = v_ol->ol_delivery_d;
  v_ol_1.ol_amount = v_ol->ol_amount;
  v_ol_1.ol_supply_w_id = v_ol->ol_supply_w_id;
  v_ol_1.ol_quantity = v_ol->ol_quantity;
  v_ol_1.v = v_ol->v;
  printf("v_ol->v: %lu\n", v_ol->v);
  v_ol_1.ol_tax = 0;

  const size_t order_line_sz = ::Size(v_ol_1);
  ermia::varstr *d_v = arena->next(order_line_sz);

  if (!d_v) {
    arena = new ermia::str_arena(ermia::config::arena_size_mb);
    d_v = arena->next(order_line_sz);
  }
  d_v = &Encode(*d_v, v_ol_1);
  return d_v;
}

ermia::varstr *precompute_aggregate_op(const char *keyp,
                                       size_t keylen,
			               const ermia::varstr &value,
			               uint64_t schema_version,
                                       ermia::transaction *txn,
			               ermia::str_arena *arena,
			               ermia::OrderedIndex *index) {
  oorder::key k_oo_temp;
  const oorder::key *k_oo = Decode(keyp, k_oo_temp);

  credit_check_order_line_scan_callback c_ol(schema_version);
  const order_line::key k_ol_0(k_oo->o_w_id, k_oo->o_d_id, k_oo->o_id, 1);
  const order_line::key k_ol_1(k_oo->o_w_id, k_oo->o_d_id, k_oo->o_id, 15);
  
  ermia::varstr *k_ol_0_str = arena->next(Size(k_ol_0));
  if (!k_ol_0_str) {
    arena = new ermia::str_arena(ermia::config::arena_size_mb);
    k_ol_0_str = arena->next(Size(k_ol_0));
  }
  
  ermia::varstr *k_ol_1_str = arena->next(Size(k_ol_1));
  if (!k_ol_1_str) {
    arena = new ermia::str_arena(ermia::config::arena_size_mb);
    k_ol_1_str = arena->next(Size(k_ol_1));
  }
  
  index->Scan(txn, Encode(*k_ol_0_str, k_ol_0), 
		  &Encode(*k_ol_1_str, k_ol_1), c_ol);
  
  oorder::value v_oo_temp;
  const oorder::value *v_oo = Decode(value, v_oo_temp);
  
  oorder_precompute_aggregate::value v_oo_pa;
  v_oo_pa.o_c_id = v_oo->o_c_id;
  v_oo_pa.o_carrier_id = v_oo->o_carrier_id;
  v_oo_pa.o_ol_cnt = v_oo->o_ol_cnt;
  v_oo_pa.o_all_local = v_oo->o_all_local;
  v_oo_pa.o_entry_d = v_oo->o_entry_d;
  v_oo_pa.o_total_amount = c_ol.sum;
  
  const size_t oorder_sz = Size(v_oo_pa);
  ermia::varstr *d_v = arena->next(oorder_sz);
  
  if (!d_v) {
    arena = new ermia::str_arena(ermia::config::arena_size_mb);
    d_v = arena->next(oorder_sz);
  }
  d_v = &Encode(*d_v, v_oo_pa);  
  return d_v;
}

rc_t tpcc_worker::txn_ddl() {
#ifdef BLOCKDDL
  db->WriteLock("SCHEMA");
  // db->WriteLock("order_line");
  // db->WriteLock("oorder");

  ermia::transaction *txn =
      db->NewTransaction(ermia::transaction::TXN_FLAG_DDL, *arena, txn_buf());

  char str1[] = "order_line";
  ermia::varstr &k1 = Encode_(str(sizeof(str1)), str1);
  ermia::varstr v1;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID oid = ermia::INVALID_OID;
  
  schema_index->ReadSchemaTable(txn, rc, k1, v1, &oid);
  // TryVerifyRelaxed(rc);
  TryCatch(rc);
  
  struct Schema_base schema;
  memcpy(&schema, (char *)v1.data(), sizeof(schema));
  
  uint64_t schema_version = schema.v + 1;
  std::cerr << "change to new schema: " << schema_version << std::endl;
  schema.v = schema_version;
  char str2[sizeof(Schema_base)];
  memcpy(str2, &schema, sizeof(str2));
  ermia::varstr &v2 = str(sizeof(str2));
  v2.copy_from(str2, sizeof(str2));
  
  rc = rc_t{RC_INVALID};
  rc = schema_index->WriteSchemaTable(txn, rc, k1, v2);
  TryCatch(rc);

  rc = order_line_table_index->WriteNormalTable(arena, order_line_table_index,
                                                txn, v2, add_column_op);
  TryCatch(rc);
  
  TryCatch(db->Commit(txn));

  /*ermia::transaction *txn =
  db->NewTransaction(ermia::transaction::TXN_FLAG_DDL, *arena, txn_buf());

  // Read schema tables first
  char str1[] = "order_line", str2[] = "oorder";
  ermia::varstr &k1 = Encode_(str(sizeof(str1)), str1), &k2 =
  Encode_(str(sizeof(str2)), str2); ermia::varstr v1, v2; rc_t rc =
  rc_t{RC_INVALID}; ermia::OID oid = ermia::INVALID_OID;

  schema_index->ReadSchemaTable(txn, rc, k1, v1, &oid);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  rc = rc_t{RC_INVALID};
  oid = ermia::INVALID_OID;
  schema_index->ReadSchemaTable(txn, rc, k2, v2, &oid);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  struct Schema_base order_line_schema;
  memcpy(&order_line_schema, (char *)v1.data(), sizeof(order_line_schema));

  struct Schema_base oorder_schema;
  memcpy(&oorder_schema, (char *)v2.data(), sizeof(oorder_schema));

  uint64_t old_oorder_schema_version = oorder_schema.v;

  uint64_t schema_version = old_oorder_schema_version + 1;
  std::cerr << "Change to a new schema, version: " << schema_version <<
  std::endl; oorder_schema.v = schema_version;
  // oorder_schema.op = precompute_aggregate_op;

  rc = rc_t{RC_INVALID};

  char str4[sizeof(Schema_base)];
  memcpy(str4, &oorder_schema, sizeof(str4));
  ermia::varstr &v3 = Encode_(str(sizeof(str4)), str4);

  schema_index->WriteSchemaTable(txn, rc, k2, v3);
  TryCatch(rc);

  rc = rc_t{RC_INVALID};
  rc = oorder_table_index->WriteNormalTable1(arena, oorder_table_index,
  order_line_table_index, nullptr, txn, v1, precompute_aggregate_op);
  TryCatch(rc);

  TryCatch(db->Commit(txn));
  */
  db->WriteUnlock("SCHEMA");
  // db->WriteUnlock("order_line");
  // db->WriteUnlock("oorder");
#elif COPYDDL
  // ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
  ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_DDL, *arena, txn_buf());
  /*#ifdef DCOPYDDL
    ermia::volatile_write(txn->GetXIDContext()->state, ermia::TXN::TXN_DDL);
  #endif*/
  printf("DDL txn begin: %lu\n", txn->GetXIDContext()->begin);

  // Read schema tables first 
  char str1[] = "order_line", str2[] = "oorder";
  ermia::varstr &k1 = Encode_(str(sizeof(str1)), str1), &k2 = Encode_(str(sizeof(str2)), str2);
  ermia::varstr v1, v2;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID oid = ermia::INVALID_OID;

  schema_index->ReadSchemaTable(txn, rc, k1, v1, &oid);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  rc = rc_t{RC_INVALID};
  oid = ermia::INVALID_OID;
  schema_index->ReadSchemaTable(txn, rc, k2, v2, &oid);
  TryVerifyRelaxed(rc);
  // TryCatch(rc);

  struct Schema_record order_line_schema;
  memcpy(&order_line_schema, (char *)v1.data(), sizeof(order_line_schema));

  struct Schema_record oorder_schema;
  memcpy(&oorder_schema, (char *)v2.data(), sizeof(oorder_schema));

  ermia::ConcurrentMasstreeIndex *old_oorder_table_index = (ermia::ConcurrentMasstreeIndex *) oorder_schema.index;
  ermia::TableDescriptor *old_oorder_td = oorder_schema.td;
  ermia::ConcurrentMasstreeIndex *old_order_line_table_index = (ermia::ConcurrentMasstreeIndex *) order_line_schema.index;
  ermia::TableDescriptor *old_order_line_td = order_line_schema.td;
  
  //uint64_t schema_version = oorder_schema.v + 1;
  uint64_t old_schema_version = order_line_schema.v;
  uint64_t schema_version = order_line_schema.v + 1;
  std::cerr << "Change to a new schema, version: " << schema_version << std::endl;
  //oorder_schema.v = schema_version;
  order_line_schema.v = schema_version;
  ALWAYS_ASSERT(order_line_schema.state == 0);
  order_line_schema.old_v = old_schema_version;

  rc = rc_t{RC_INVALID};

  std::stringstream ss;
  ss << schema_version;

  //std::string str3 = std::string(str2);
  std::string str3 = std::string(str1);
  str3 += ss.str();
  
  /*if (!db->BuildIndexMap(str3.c_str())) {
    // printf("Duplicate table creation\n");
    TryCatch({RC_ABORT_USER});
  }

  db->WriteLock(str3.c_str());
  */
  db->CreateTable(str3.c_str());
  db->CreateMasstreePrimaryIndex(str3.c_str(), str3);

  /*std::string secondary_index_name = std::string("oorder_c_id_idx");
  secondary_index_name += ss.str();
  db->CreateMasstreeSecondaryIndex(str3.c_str(), secondary_index_name);
  */
  // std::cerr << "Create a new table: " << str3 << std::endl;
  
#ifdef LAZYDDL
  order_line_schema.old_index = old_order_line_table_index;
  order_line_schema.old_td = old_order_line_td;

  //oorder_schema.old_index = oorder_schema.index;
  //oorder_schema.old_td = oorder_schema.td;
#elif DCOPYDDL
  order_line_schema.old_td = old_order_line_td;
  order_line_schema.state = 1;
#endif
  //oorder_schema.index = ermia::Catalog::GetTable(str3.c_str())->GetPrimaryIndex();
  //oorder_schema.td = ermia::Catalog::GetTable(str3.c_str());
  //oorder_schema.op = precompute_aggregate_op;
  order_line_schema.index = ermia::Catalog::GetTable(str3.c_str())->GetPrimaryIndex();
  order_line_schema.td = ermia::Catalog::GetTable(str3.c_str());
  //order_line_schema.op = add_column_op;
  char str4[sizeof(Schema_record)];
  ALWAYS_ASSERT(sizeof(Schema_record) == sizeof(order_line_schema));
  //memcpy(str4, &oorder_schema, sizeof(str4));
  memcpy(str4, &order_line_schema, sizeof(str4));
  ermia::varstr &v3 = Encode_(str(sizeof(str4)), str4);

  ermia::ddl_td = old_order_line_td;
  //txn->set_table_descriptors(oorder_schema.td, old_oorder_td);
  txn->set_table_descriptors(order_line_schema.td, old_order_line_td);

  // db->WriteUnlock(str3.c_str());

#if !defined(LAZYDDL) && !defined(DCOPYDDL)
  std::vector<ermia::thread::Thread *> cdc_workers =
      txn->changed_data_capture();
#endif

  //schema_index->WriteSchemaTable(txn, rc, k2, v3);
  schema_index->WriteSchemaTable(txn, rc, k1, v3);
  TryCatch(rc);
  /*#ifdef DCOPYDDL
    ermia::volatile_write(txn->GetXIDContext()->state, ermia::TXN::TXN_DDL);
  #endif*/
  // TryCatch(db->Commit(txn));

  // txn = db->NewTransaction(ermia::transaction::TXN_FLAG_DDL, *arena, txn_buf());
#if !defined(LAZYDDL)
  rc = rc_t{RC_INVALID};
  ermia::ConcurrentMasstreeIndex *new_order_line_table_index = (ermia::ConcurrentMasstreeIndex *) order_line_schema.index;
  ALWAYS_ASSERT(new_order_line_table_index);
  rc = new_order_line_table_index->WriteNormalTable(
      arena, old_order_line_table_index, txn, v3, add_column_op,
      tbl_new_order(1));
  //ermia::ConcurrentMasstreeIndex *oorder_table_index = (ermia::ConcurrentMasstreeIndex *) oorder_schema.index;
  //ermia::ConcurrentMasstreeIndex *oorder_table_secondary_index = (ermia::ConcurrentMasstreeIndex *) ermia::Catalog::GetIndex(secondary_index_name);
  //ALWAYS_ASSERT(oorder_table_secondary_index);
  //rc = oorder_table_index->WriteNormalTable1(arena, old_oorder_table_index, old_order_line_table_index, oorder_table_secondary_index, txn, v1, oorder_schema.op);
#if !defined(DCOPYDDL)
  if (rc._val != RC_TRUE) {
    txn->join_changed_data_capture_threads(cdc_workers);
  }
#endif
  TryCatch(rc);

#ifdef DCOPYDDL
  order_line_schema.state = 0;
  memcpy(str4, &order_line_schema, sizeof(str4));
  v3 = Encode_(str(sizeof(str4)), str4);

  schema_index->WriteSchemaTable(txn, rc, k1, v3);
  TryCatch(rc);
#endif
#endif

#if !defined(LAZYDDL) && !defined(DCOPYDDL)
  // txn->set_ddl_running_1(false);
#endif

  TryCatch(db->Commit(txn));
  ermia::ddl_td = NULL;

#if !defined(LAZYDDL) && !defined(DCOPYDDL)
  txn->join_changed_data_capture_threads(cdc_workers);
#endif
#endif
  printf("Commit OK\n");
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
  // ALWAYS_ASSERT(m == 100);
  double base = 1000000.0;
  // double base = 100.0;
  if (g_txn_workload_mix[0])
    w.push_back(workload_desc(
        "NewOrder", double(g_txn_workload_mix[0]) / base, TxnNewOrder));
  if (g_txn_workload_mix[1])
    w.push_back(workload_desc(
        "Payment", double(g_txn_workload_mix[1]) / base, TxnPayment));
  if (g_txn_workload_mix[2])
    w.push_back(workload_desc("CreditCheck",
                              double(g_txn_workload_mix[2]) / base,
                              TxnCreditCheck));
  if (g_txn_workload_mix[3])
    w.push_back(workload_desc(
        "Delivery", double(g_txn_workload_mix[3]) / base, TxnDelivery));
  if (g_txn_workload_mix[4])
    w.push_back(workload_desc("OrderStatus",
                              double(g_txn_workload_mix[4]) / base,
                              TxnOrderStatus));
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

  w.push_back(workload_desc("DDL", double(1) / base, TxnDDL));

  return w;
}

#endif // ADV_COROUTINE

