#include "tpcc-common.h"

template <typename T1, typename T2, typename T3>
struct key_tuple {
  struct hash {
    std::size_t operator()(const key_tuple<T1, T2, T3> &key) const {
      return key.t1 ^ key.t2 ^ key.t3;
    }
  };

  T1 t1;
  T2 t2;
  T3 t3;

  bool operator==(const key_tuple<T1, T2, T3> &key) const {
    return std::tie(t1, t2, t3) == std::tie(key.t1, key.t2, key.t3);
  }
};

static ermia::ddl::ddl_type get_example_ddl_type(uint32_t ddl_example) {
  switch (ddl_example) {
    case 5:
      return ermia::ddl::NO_COPY_VERIFICATION;
    default:
      return ermia::ddl::COPY_ONLY;
  }
}

rc_t tpcc_worker::add_column(ermia::transaction *txn, uint32_t ddl_example) {
  ermia::varstr valptr;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID oid = ermia::INVALID_OID;

  schema_index->ReadSchemaRecord(txn, rc, *order_line_key, valptr, &oid);
  TryVerifyStrict(rc);

  struct ermia::schema_record schema;
  schema_kv::value schema_value_temp;
  const schema_kv::value *old_schema_value = Decode(valptr, schema_value_temp);
  schema.value_to_record(old_schema_value);
  schema.ddl_type = get_example_ddl_type(ddl_example);

#ifdef COPYDDL
  schema.old_v = schema.v;
  uint64_t schema_version = schema.old_v + 1;
  DLOG(INFO) << "Change to a new schema, version: " << schema_version;
  schema.v = schema_version;
  schema.old_td = schema.td;
  schema.state = ermia::ddl::schema_state_type::NOT_READY;
  schema.show_index = true;

  rc = rc_t{RC_INVALID};

  if (schema.ddl_type != ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
    char table_name[20];
    snprintf(table_name, 20, "order_line_%lu", schema_version);

    db->CreateTable(table_name);

    schema.td = ermia::Catalog::GetTable(table_name);
    schema.old_index = schema.index;
#ifdef LAZYDDL
    schema.old_tds[schema.old_v] = schema.old_td;
    schema.old_tds_total = schema.v;
#endif

#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
    auto *new_table_index =
        new ermia::ConcurrentMasstreeIndex(table_name, true);
    new_table_index->SetArrays(true);
    schema.td->SetPrimaryIndex(new_table_index);
    schema.index = new_table_index;
#else
    ermia::Catalog::GetTable(table_name)
        ->SetPrimaryIndex(schema.old_index, table_name);
    schema.index = ermia::Catalog::GetTable(table_name)->GetPrimaryIndex();
#endif

    txn->set_old_td(schema.old_td);
    txn->add_new_td_map(schema.td);
    txn->add_old_td_map(schema.old_td);
  } else {
    schema.reformats_total = schema.v;
  }

  schema_kv::value new_schema_value;
  schema.record_to_value(new_schema_value);

  rc = rc_t{RC_INVALID};
  schema_index->WriteSchemaTable(
      txn, rc, *order_line_key,
      Encode(str(Size(new_schema_value)), new_schema_value));
  TryCatch(rc);

  // New a ddl executor
  ermia::ddl::ddl_executor *ddl_exe =
      new ermia::ddl::ddl_executor(schema.ddl_type);
  ddl_exe->add_ddl_executor_paras(schema.v, schema.old_v, schema.ddl_type,
                                  schema.reformat_idx, schema.constraint_idx,
                                  schema.td, schema.old_td, schema.index,
                                  schema.state);
  txn->set_ddl_executor(ddl_exe);

  if (schema.ddl_type != ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
#if !defined(LAZYDDL)
    rc = rc_t{RC_INVALID};
    rc = ddl_exe->scan(txn, arena);
    TryCatch(rc);
#endif
  }
#elif BLOCKDDL
  uint64_t schema_version = schema.v + 1;
  DLOG(INFO) << "change to new schema: " << schema_version;
  schema.v = schema_version;

  schema_kv::value new_schema_value;
  schema.record_to_value(new_schema_value);

  TryCatch(schema_index->WriteSchemaTable(
      txn, rc, *order_line_key,
      Encode(str(Size(new_schema_value)), new_schema_value)));

  // New a ddl executor
  ermia::ddl::ddl_executor *ddl_exe =
      new ermia::ddl::ddl_executor(schema.ddl_type);
  ddl_exe->add_ddl_executor_paras(
      schema.v, -1, schema.ddl_type, schema.reformat_idx, schema.constraint_idx,
      schema.td, schema.td, schema.index, ermia::ddl::schema_state_type::READY);

  txn->set_ddl_executor(ddl_exe);
  txn->set_old_td(schema.td);
  txn->add_old_td_map(schema.td);
  txn->add_new_td_map(schema.td);

  TryCatch(ddl_exe->scan(txn, arena));
#endif
  return rc_t{RC_TRUE};
};

rc_t tpcc_worker::table_split(ermia::transaction *txn, uint32_t ddl_example) {
  auto split_customer_private =
      [=](ermia::varstr *key, ermia::varstr &value, ermia::str_arena *arena,
          uint64_t schema_version, ermia::FID fid, ermia::OID oid) {
        customer::value v_c_temp;
        const customer::value *v_c = Decode(value, v_c_temp);

        customer_private::value v_c_private;
        v_c_private.c_id = v_c->c_id;
        v_c_private.c_discount = v_c->c_discount;
        v_c_private.c_credit.assign(v_c->c_credit.data(), v_c->c_credit.size());
        v_c_private.c_credit_lim = v_c->c_credit_lim;
        v_c_private.c_balance = v_c->c_balance;
        v_c_private.c_ytd_payment = v_c->c_ytd_payment;
        v_c_private.c_payment_cnt = v_c->c_payment_cnt;
        v_c_private.c_delivery_cnt = v_c->c_delivery_cnt;
        v_c_private.c_data.assign(v_c->c_data.data(), v_c->c_data.size());
        v_c_private.v = schema_version;

        const size_t customer_private_sz = ::Size(v_c_private);
        ermia::varstr *new_value = arena->next(customer_private_sz);
        new_value = &Encode(*new_value, v_c_private);

        return new_value;
      };

  auto create_secondary_index_key =
      [=](ermia::varstr *key, ermia::varstr &value, ermia::str_arena *arena,
          uint64_t schema_version, ermia::FID fid, ermia::OID oid) {
        const char *keyp = (const char *)(key->p);
        customer::key k_c_temp;
        const customer::key *k_c = Decode(keyp, k_c_temp);

        customer::value v_c_temp;
        const customer::value *v_c = Decode(value, v_c_temp);

        const customer_name_idx::key k_idx(k_c->c_w_id, k_c->c_d_id,
                                           v_c->c_last.str(true),
                                           v_c->c_first.str(true));

        const size_t customer_name_idx_sz = ::Size(k_idx);
        ermia::varstr *new_key = arena->next(customer_name_idx_sz);
        new_key = &Encode(*new_key, k_idx);

        return new_key;
      };

  auto split_customer_public =
      [=](ermia::varstr *key, ermia::varstr &value, ermia::str_arena *arena,
          uint64_t schema_version, ermia::FID fid, ermia::OID oid) {
        customer::value v_c_temp;
        const customer::value *v_c = Decode(value, v_c_temp);

        customer_public::value v_c_public;
        v_c_public.c_id = v_c->c_id;
        v_c_public.c_last.assign(v_c->c_last.data(), v_c->c_last.size());
        v_c_public.c_first.assign(v_c->c_first.data(), v_c->c_first.size());
        v_c_public.c_street_1.assign(v_c->c_street_1.data(),
                                     v_c->c_street_1.size());
        v_c_public.c_street_2.assign(v_c->c_street_2.data(),
                                     v_c->c_street_2.size());
        v_c_public.c_city.assign(v_c->c_city.data(), v_c->c_city.size());
        v_c_public.c_state.assign(v_c->c_state.data(), v_c->c_state.size());
        v_c_public.c_zip.assign(v_c->c_zip.data(), v_c->c_zip.size());
        v_c_public.c_phone.assign(v_c->c_phone.data(), v_c->c_phone.size());
        v_c_public.c_since = v_c->c_since;
        v_c_public.c_middle.assign("OE");
        v_c_public.v = schema_version;

        const size_t customer_public_sz = ::Size(v_c_public);
        ermia::varstr *new_value = arena->next(customer_public_sz);
        new_value = &Encode(*new_value, v_c_public);

        return new_value;
      };

  ermia::varstr valptr;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID oid = ermia::INVALID_OID;

  schema_index->ReadSchemaRecord(txn, rc, *customer_key, valptr, &oid);
  TryVerifyStrict(rc);

  struct ermia::schema_record customer_schema;
  schema_kv::value schema_value_temp;
  const schema_kv::value *old_schema_value = Decode(valptr, schema_value_temp);
  customer_schema.value_to_record(old_schema_value);
  customer_schema.ddl_type = get_example_ddl_type(ddl_example);

  customer_schema.reformat_idx = ermia::ddl::reformats.size();
  ermia::ddl::reformats.push_back(split_customer_private);

  customer_schema.secondary_index_key_create_idx = ermia::ddl::reformats.size();
  ermia::ddl::reformats.push_back(create_secondary_index_key);

#ifdef COPYDDL
  customer_schema.old_v = customer_schema.v;
  uint64_t schema_version = customer_schema.old_v + 1;
  DLOG(INFO) << "Change to a new schema, version: " << schema_version;
  customer_schema.v = schema_version;
  customer_schema.old_td = customer_schema.td;
  customer_schema.state = ermia::ddl::schema_state_type::NOT_READY;
  customer_schema.show_index = true;

  rc = rc_t{RC_INVALID};

  if (customer_schema.ddl_type != ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
    char table_name[20];
    snprintf(table_name, 20, "customer_%lu", schema_version);

    db->CreateTable(table_name);

    customer_schema.td = ermia::Catalog::GetTable(table_name);
    customer_schema.old_index = customer_schema.index;
#ifdef LAZYDDL
    customer_schema.old_tds[customer_schema.old_v] = customer_schema.old_td;
    customer_schema.old_tds_total = customer_schema.v;
#endif

#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
    auto *new_private_customer_table_index =
        new ermia::ConcurrentMasstreeIndex(table_name, true);
    new_private_customer_table_index->SetArrays(true);
    auto *new_private_customer_table_secondary_index =
        new ermia::ConcurrentMasstreeIndex(table_name, false);
    new_private_customer_table_secondary_index->SetArrays(false);
    customer_schema.td->SetPrimaryIndex(new_private_customer_table_index);
    customer_schema.td->AddSecondaryIndex(
        new_private_customer_table_secondary_index);
    customer_schema.index = new_private_customer_table_index;
#else
    ermia::Catalog::GetTable(table_name)
        ->SetPrimaryIndex(customer_schema.old_index, table_name);
    ermia::Catalog::GetTable(table_name)
        ->AddSecondaryIndex(customer_schema.old_td->GetSecIndexes().at(0));
    ALWAYS_ASSERT(tbl_customer_name_idx(1) ==
                  customer_schema.old_td->GetSecIndexes().at(0));
    customer_schema.index =
        ermia::Catalog::GetTable(table_name)->GetPrimaryIndex();
#endif

    txn->set_old_td(customer_schema.old_td);
    txn->add_new_td_map(customer_schema.td);
    txn->add_old_td_map(customer_schema.old_td);
  } else {
    customer_schema.reformats_total = 1;
  }

  schema_kv::value new_schema_value;
  customer_schema.record_to_value(new_schema_value);

  rc = rc_t{RC_INVALID};
  schema_index->WriteSchemaTable(
      txn, rc, *customer_key,
      Encode(str(Size(new_schema_value)), new_schema_value));
  TryCatch(rc);

  // New a ddl executor
  ermia::ddl::ddl_executor *ddl_exe =
      new ermia::ddl::ddl_executor(customer_schema.ddl_type);
  ddl_exe->add_ddl_executor_paras(
      customer_schema.v, customer_schema.old_v, customer_schema.ddl_type,
      customer_schema.reformat_idx, customer_schema.constraint_idx,
      customer_schema.td, customer_schema.old_td, customer_schema.index,
      customer_schema.state);
  txn->set_ddl_executor(ddl_exe);

  if (customer_schema.ddl_type != ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
#if !defined(LAZYDDL)
    rc = rc_t{RC_INVALID};
    rc = ddl_exe->scan(txn, arena);
    TryCatch(rc);
#endif
  }
#elif BLOCKDDL
  uint64_t schema_version = customer_schema.v + 1;
  DLOG(INFO) << "change to new schema: " << schema_version;
  customer_schema.v = schema_version;

  schema_kv::value new_schema_value;
  customer_schema.record_to_value(new_schema_value);

  TryCatch(schema_index->WriteSchemaTable(
      txn, rc, *customer_key,
      Encode(str(Size(new_schema_value)), new_schema_value)));

  // New a ddl executor
  ermia::ddl::ddl_executor *ddl_exe =
      new ermia::ddl::ddl_executor(customer_schema.ddl_type);
  ddl_exe->add_ddl_executor_paras(
      customer_schema.v, -1, customer_schema.ddl_type,
      customer_schema.reformat_idx, customer_schema.constraint_idx,
      customer_schema.td, customer_schema.td, customer_schema.index,
      ermia::ddl::schema_state_type::READY);

  txn->set_ddl_executor(ddl_exe);
  txn->set_old_td(customer_schema.td);
  txn->add_old_td_map(customer_schema.td);
  txn->add_new_td_map(customer_schema.td);

  TryCatch(ddl_exe->scan(txn, arena));
#endif
  return rc_t{RC_TRUE};
};

rc_t tpcc_worker::preaggregation(ermia::transaction *txn,
                                 uint32_t ddl_example) {
  ermia::varstr valptr1, valptr2;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID oid = ermia::INVALID_OID;

  schema_index->ReadSchemaRecord(txn, rc, *order_line_key, valptr1, &oid);
  TryVerifyStrict(rc);

  schema_index->ReadSchemaRecord(txn, rc, *oorder_key, valptr2, &oid);
  TryVerifyStrict(rc);

  schema_kv::value schema_value_temp_1, schema_value_temp_2;
  const schema_kv::value *old_order_line_schema_value =
      Decode(valptr1, schema_value_temp_1);
  const schema_kv::value *old_oorder_schema_value =
      Decode(valptr2, schema_value_temp_2);

  struct ermia::schema_record order_line_schema;
  order_line_schema.value_to_record(old_order_line_schema_value);

  struct ermia::schema_record oorder_schema;
  oorder_schema.value_to_record(old_oorder_schema_value);

  return rc_t{RC_TRUE};
}

rc_t tpcc_worker::create_index(ermia::transaction *txn, uint32_t ddl_example) {
  ermia::varstr valptr;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID oid = ermia::INVALID_OID;

  schema_index->ReadSchemaRecord(txn, rc, *order_line_key, valptr, &oid);
  TryVerifyStrict(rc);

  struct ermia::schema_record schema;
  schema_kv::value schema_value_temp;
  const schema_kv::value *old_schema_value = Decode(valptr, schema_value_temp);
  schema.value_to_record(old_schema_value);
  schema.ddl_type = get_example_ddl_type(ddl_example);

  schema.old_v = schema.v;
  uint64_t schema_version = schema.old_v + 1;
  DLOG(INFO) << "Change to a new schema, version: " << schema_version;
  schema.v = schema_version;
  schema.old_td = schema.td;
  schema.state = ermia::ddl::schema_state_type::NOT_READY;
  schema.show_index = true;

  auto *new_order_line_table_index =
      new ermia::ConcurrentMasstreeIndex("order_line", true);

  auto add_index = [=](ermia::varstr *key, ermia::varstr &value,
                       ermia::str_arena *arena, uint64_t schema_version,
                       ermia::FID fid, ermia::OID oid) {
    if (!key) return nullptr;
    new_order_line_table_index->InsertOID(txn, *key, oid);
    return nullptr;
  };

  schema.reformat_idx = ermia::ddl::reformats.size();
  ermia::ddl::reformats.push_back(add_index);

  new_order_line_table_index->SetArrays(true);
  schema.td->SetPrimaryIndex(new_order_line_table_index);
  schema.old_index = schema.index;
  schema.index = new_order_line_table_index;

  schema_kv::value new_schema_value;
  schema.record_to_value(new_schema_value);

  rc = rc_t{RC_INVALID};
  schema_index->WriteSchemaTable(
      txn, rc, *order_line_key,
      Encode(str(Size(new_schema_value)), new_schema_value));
  TryCatch(rc);

  // New a ddl executor
  ermia::ddl::ddl_executor *ddl_exe = new ermia::ddl::ddl_executor();
  ddl_exe->add_ddl_executor_paras(schema.v, schema.old_v, schema.ddl_type,
                                  schema.reformat_idx, schema.constraint_idx,
                                  schema.td, schema.old_td, schema.index,
                                  schema.state, -1, true, false, -1);
  txn->set_ddl_executor(ddl_exe);

  txn->set_old_td(schema.td);
  txn->add_old_td_map(schema.td);

#if !defined(LAZYDDL)
  rc = ddl_exe->scan(txn, arena);
  TryCatch(rc);
#endif
  return rc_t{RC_TRUE};
}

rc_t tpcc_worker::table_join(ermia::transaction *txn, uint32_t ddl_example) {
  auto order_line_stock_join =
      [=](ermia::varstr *key, ermia::varstr &value, ermia::str_arena *arena,
          uint64_t schema_version, ermia::FID fid, ermia::OID oid) {
        const char *keyp = (const char *)(key->p);
        order_line::key k_ol_temp;
        const order_line::key *k_ol = Decode(keyp, k_ol_temp);

        order_line::value v_ol_temp;
        const order_line::value *v_ol = Decode(value, v_ol_temp);

        ermia::OID o = 0;
        rc_t rc = {RC_INVALID};
        const stock::key k_s(k_ol->ol_w_id, v_ol->ol_i_id);
        const size_t stock_sz = ::Size(k_s);
        ermia::varstr *stock_key = arena->next(stock_sz);
        AWAIT tbl_stock(1)->GetOID(*stock_key, rc, txn->GetXIDContext(), o);

        order_line_stock::value v_ol_stock;
        v_ol_stock.ol_i_id = v_ol->ol_i_id;
        v_ol_stock.ol_delivery_d = v_ol->ol_delivery_d;
        v_ol_stock.ol_amount = v_ol->ol_amount;
        v_ol_stock.ol_supply_w_id = v_ol->ol_supply_w_id;
        v_ol_stock.ol_quantity = v_ol->ol_quantity;
        v_ol_stock.s_oid = o;

        const size_t order_line_sz = ::Size(v_ol_stock);
        ermia::varstr *new_value = arena->next(order_line_sz);
        new_value = &Encode(*new_value, v_ol_stock);

        return new_value;
      };

  ermia::varstr valptr;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID oid = ermia::INVALID_OID;

  schema_index->ReadSchemaRecord(txn, rc, *order_line_key, valptr, &oid);
  TryVerifyStrict(rc);

  struct ermia::schema_record schema;
  schema_kv::value schema_value_temp;
  const schema_kv::value *old_schema_value = Decode(valptr, schema_value_temp);
  schema.value_to_record(old_schema_value);
  schema.ddl_type = get_example_ddl_type(ddl_example);

  schema.reformat_idx = ermia::ddl::reformats.size();
  ermia::ddl::reformats.push_back(order_line_stock_join);

#ifdef COPYDDL
  schema.old_v = schema.v;
  uint64_t schema_version = schema.old_v + 1;
  DLOG(INFO) << "Change to a new schema, version: " << schema_version;
  schema.v = schema_version;
  schema.old_td = schema.td;
  schema.state = ermia::ddl::schema_state_type::NOT_READY;
  schema.show_index = true;

  rc = rc_t{RC_INVALID};

  if (schema.ddl_type != ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
    char table_name[20];
    snprintf(table_name, 20, "order_line_%lu", schema_version);

    db->CreateTable(table_name);

    schema.td = ermia::Catalog::GetTable(table_name);
    schema.old_index = schema.index;
#ifdef LAZYDDL
    schema.old_tds[schema.old_v] = schema.old_td;
    schema.old_tds_total = schema.v;
#endif

#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
    auto *new_table_index =
        new ermia::ConcurrentMasstreeIndex(table_name, true);
    new_table_index->SetArrays(true);
    schema.td->SetPrimaryIndex(new_table_index);
    schema.index = new_table_index;
#else
    ermia::Catalog::GetTable(table_name)
        ->SetPrimaryIndex(schema.old_index, table_name);
    schema.index = ermia::Catalog::GetTable(table_name)->GetPrimaryIndex();
#endif

    txn->set_old_td(schema.old_td);
    txn->add_new_td_map(schema.td);
    txn->add_old_td_map(schema.old_td);
  } else {
    schema.reformats_total = 1;
  }

  schema_kv::value new_schema_value;
  schema.record_to_value(new_schema_value);

  rc = rc_t{RC_INVALID};
  schema_index->WriteSchemaTable(
      txn, rc, *order_line_key,
      Encode(str(Size(new_schema_value)), new_schema_value));
  TryCatch(rc);

  // New a ddl executor
  ermia::ddl::ddl_executor *ddl_exe =
      new ermia::ddl::ddl_executor(schema.ddl_type);
  ddl_exe->add_ddl_executor_paras(schema.v, schema.old_v, schema.ddl_type,
                                  schema.reformat_idx, schema.constraint_idx,
                                  schema.td, schema.old_td, schema.index,
                                  schema.state);
  txn->set_ddl_executor(ddl_exe);

  if (schema.ddl_type != ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
#if !defined(LAZYDDL)
    rc = rc_t{RC_INVALID};
    rc = ddl_exe->scan(txn, arena);
    TryCatch(rc);
#endif
  }
#elif BLOCKDDL
  uint64_t schema_version = schema.v + 1;
  DLOG(INFO) << "change to new schema: " << schema_version;
  schema.v = schema_version;

  schema_kv::value new_schema_value;
  schema.record_to_value(new_schema_value);

  TryCatch(schema_index->WriteSchemaTable(
      txn, rc, *order_line_key,
      Encode(str(Size(new_schema_value)), new_schema_value)));

  // New a ddl executor
  ermia::ddl::ddl_executor *ddl_exe =
      new ermia::ddl::ddl_executor(schema.ddl_type);
  ddl_exe->add_ddl_executor_paras(
      schema.v, -1, schema.ddl_type, schema.reformat_idx, schema.constraint_idx,
      schema.td, schema.td, schema.index, ermia::ddl::schema_state_type::READY);

  txn->set_ddl_executor(ddl_exe);
  txn->set_old_td(schema.td);
  txn->add_old_td_map(schema.td);
  txn->add_new_td_map(schema.td);

  TryCatch(ddl_exe->scan(txn, arena));
#endif
  return rc_t{RC_TRUE};
}
