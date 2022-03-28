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
    case 6:
      return ermia::ddl::NO_COPY_VERIFICATION;
    case 7:
      return ermia::ddl::NO_COPY_VERIFICATION;
    default:
      return ermia::ddl::COPY_ONLY;
  }
}

rc_t tpcc_worker::add_column(ermia::transaction *txn, uint32_t ddl_example) {
  ermia::varstr valptr;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID oid = ermia::INVALID_OID;

  ermia::catalog::read_schema(txn, schema_index, *order_line_key, valptr, &oid);
  TryVerifyStrict(rc);

  struct ermia::schema_record schema;
  schema_kv::value schema_value_temp;
  const schema_kv::value *old_schema_value = Decode(valptr, schema_value_temp);
  schema.value_to_record(old_schema_value);
  schema.ddl_type = get_example_ddl_type(ddl_example);

  ermia::ddl::ddl_executor *ddl_exe = txn->get_ddl_executor();
  ddl_exe->set_ddl_type(schema.ddl_type);

  schema.old_v = schema.v;
  uint64_t schema_version = schema.old_v + 1;
  DLOG(INFO) << "Change to a new schema, version: " << schema_version;
  schema.v = schema_version;
  schema.old_td = schema.td;
  schema.show_index = true;

#ifdef COPYDDL
  schema.state = ermia::ddl::schema_state_type::NOT_READY;

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
    db->CreateMasstreePrimaryIndex(table_name, std::string(table_name));
#else
    schema.td->SetPrimaryIndex(schema.old_index, table_name);
#endif
    schema.index = schema.td->GetPrimaryIndex();

    ddl_exe->set_old_td(schema.old_td);
    ddl_exe->add_new_td_map(schema.td);
    ddl_exe->add_old_td_map(schema.old_td);
  } else {
    schema.reformats_total = schema.v;
  }

  schema_kv::value new_schema_value;
  schema.record_to_value(new_schema_value);

  rc = ermia::catalog::write_schema(txn, schema_index, *order_line_key,
    Encode(str(Size(new_schema_value)), new_schema_value), oid);
  TryCatch(rc);

  ddl_exe->add_ddl_executor_paras(schema.v, schema.old_v, schema.ddl_type,
                                  schema.reformat_idx, schema.constraint_idx,
                                  schema.td, schema.old_td, schema.index,
                                  schema.state);

  if (schema.ddl_type != ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
#if !defined(LAZYDDL)
    rc = rc_t{RC_INVALID};
    rc = ddl_exe->scan(txn, arena);
    TryCatch(rc);
#endif
  }
#elif BLOCKDDL
  schema_kv::value new_schema_value;
  schema.record_to_value(new_schema_value);

  TryCatch(ermia::catalog::write_schema(
      txn, schema_index, *order_line_key,
      Encode(str(Size(new_schema_value)), new_schema_value), oid));

  ddl_exe->add_ddl_executor_paras(schema.v, schema.old_v, schema.ddl_type,
                                  schema.reformat_idx, schema.constraint_idx,
                                  schema.td, schema.td, schema.index,
                                  ermia::ddl::schema_state_type::READY);

  ddl_exe->set_old_td(schema.td);
  ddl_exe->add_old_td_map(schema.td);
  ddl_exe->add_new_td_map(schema.td);

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

  ermia::catalog::read_schema(txn, schema_index, *customer_key, valptr, &oid);
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

  ermia::ddl::ddl_executor *ddl_exe = txn->get_ddl_executor();
  ddl_exe->set_ddl_type(customer_schema.ddl_type);

  customer_schema.old_v = customer_schema.v;
  uint64_t schema_version = customer_schema.old_v + 1;
  DLOG(INFO) << "Change to a new schema, version: " << schema_version;
  customer_schema.v = schema_version;
  customer_schema.old_td = customer_schema.td;
  customer_schema.show_index = true;

#ifdef COPYDDL
  customer_schema.state = ermia::ddl::schema_state_type::NOT_READY;

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
    db->CreateMasstreePrimaryIndex(table_name, std::string(table_name));
    db->CreateMasstreeSecondaryIndex(table_name, std::string(table_name));
#else
    customer_schema.td->SetPrimaryIndex(customer_schema.old_index, table_name);
    customer_schema.td->AddSecondaryIndex(
        customer_schema.old_td->GetSecIndexes().at(0));
    ALWAYS_ASSERT(tbl_customer_name_idx(1) ==
                  customer_schema.old_td->GetSecIndexes().at(0));
#endif
    customer_schema.index = customer_schema.td->GetPrimaryIndex();

    ddl_exe->set_old_td(customer_schema.old_td);
    ddl_exe->add_new_td_map(customer_schema.td);
    ddl_exe->add_old_td_map(customer_schema.old_td);
  } else {
    customer_schema.reformats_total = customer_schema.v;
    customer_schema.reformats[customer_schema.old_v] =
        customer_schema.reformat_idx;
  }

  schema_kv::value new_schema_value;
  customer_schema.record_to_value(new_schema_value);

  rc = ermia::catalog::write_schema(txn, schema_index, *customer_key,
      Encode(str(Size(new_schema_value)), new_schema_value), oid);
  TryCatch(rc);

  ddl_exe->add_ddl_executor_paras(
      customer_schema.v, customer_schema.old_v, customer_schema.ddl_type,
      customer_schema.reformat_idx, customer_schema.constraint_idx,
      customer_schema.td, customer_schema.old_td, customer_schema.index,
      customer_schema.state, customer_schema.secondary_index_key_create_idx);

  if (customer_schema.ddl_type != ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
#if !defined(LAZYDDL)
    rc = rc_t{RC_INVALID};
    rc = ddl_exe->scan(txn, arena);
    TryCatch(rc);
#endif
  }
#elif BLOCKDDL
  schema_kv::value new_schema_value;
  customer_schema.record_to_value(new_schema_value);

  TryCatch(ermia::catalog::write_schema(txn, schema_index, *customer_key,
      Encode(str(Size(new_schema_value)), new_schema_value), oid));

  ddl_exe->add_ddl_executor_paras(
      customer_schema.v, customer_schema.old_v, customer_schema.ddl_type,
      customer_schema.reformat_idx, customer_schema.constraint_idx,
      customer_schema.td, customer_schema.td, customer_schema.index,
      ermia::ddl::schema_state_type::READY);

  ddl_exe->set_old_td(customer_schema.td);
  ddl_exe->add_old_td_map(customer_schema.td);
  ddl_exe->add_new_td_map(customer_schema.td);

  TryCatch(ddl_exe->scan(txn, arena));
#endif
  return rc_t{RC_TRUE};
};

rc_t tpcc_worker::preaggregation(ermia::transaction *txn,
                                 uint32_t ddl_example) {
  auto precompute_aggregate_1 =
      [=](ermia::varstr *key, ermia::varstr &value, ermia::str_arena *arena,
          uint64_t schema_version, ermia::FID fid, ermia::OID oid) {
        const char *keyp = (const char *)(key->p);
        oorder::key k_oo_temp;
        const oorder::key *k_oo = Decode(keyp, k_oo_temp);

        oorder::value v_oo_temp;
        const oorder::value *v_oo = Decode(value, v_oo_temp);

        credit_check_order_line_scan_callback c_ol(schema_version - 1);
        const order_line::key k_ol_0(k_oo->o_w_id, k_oo->o_d_id, k_oo->o_id, 0);
        const order_line::key k_ol_1(k_oo->o_w_id, k_oo->o_d_id, k_oo->o_id,
                                     std::numeric_limits<int32_t>::max());

        ermia::varstr *k_ol_0_str = arena->next(Size(k_ol_0));
        ermia::varstr *k_ol_1_str = arena->next(Size(k_ol_1));

        tbl_order_line(1)->Scan(txn, Encode(*k_ol_0_str, k_ol_0),
                                &Encode(*k_ol_1_str, k_ol_1), c_ol);

        oorder_precompute_aggregate::value v_oo_pa;
        v_oo_pa.o_c_id = v_oo->o_c_id;
        v_oo_pa.o_carrier_id = v_oo->o_carrier_id;
        v_oo_pa.o_ol_cnt = v_oo->o_ol_cnt;
        v_oo_pa.o_all_local = v_oo->o_all_local;
        v_oo_pa.o_entry_d = v_oo->o_entry_d;
        v_oo_pa.o_total_amount = c_ol.sum;

        const size_t oorder_precompute_aggregate_sz = ::Size(v_oo_pa);
        ermia::varstr *new_value = arena->next(oorder_precompute_aggregate_sz);
        new_value = &Encode(*new_value, v_oo_pa);

        return new_value;
      };

  auto create_secondary_index_key =
      [=](ermia::varstr *key, ermia::varstr &value, ermia::str_arena *arena,
          uint64_t schema_version, ermia::FID fid, ermia::OID oid) {
        const char *keyp = (const char *)(key->p);
        oorder::key k_oo_temp;
        const oorder::key *k_oo = Decode(keyp, k_oo_temp);

        oorder::value v_oo_temp;
        const oorder::value *v_oo = Decode(value, v_oo_temp);

        const oorder_c_id_idx::key k_oo_idx(k_oo->o_w_id, k_oo->o_d_id,
                                            v_oo->o_c_id, k_oo->o_id);

        const size_t oorder_c_id_idx_sz = ::Size(k_oo_idx);
        ermia::varstr *new_key = arena->next(oorder_c_id_idx_sz);
        new_key = &Encode(*new_key, k_oo_idx);

        return new_key;
      };

  ermia::varstr valptr1, valptr2;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID order_line_oid = ermia::INVALID_OID;
  ermia::OID oorder_oid = ermia::INVALID_OID;

  ermia::catalog::read_schema(txn, schema_index, *order_line_key, valptr1, &order_line_oid);
  TryVerifyStrict(rc);

  ermia::catalog::read_schema(txn, schema_index, *oorder_key, valptr2, &oorder_oid);
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
  oorder_schema.ddl_type = get_example_ddl_type(ddl_example);

  ermia::FID old_oorder_fid = oorder_schema.td->GetTupleFid();
  ermia::FID old_order_line_fid = order_line_schema.td->GetTupleFid();

  auto precompute_aggregate_2 = [=](ermia::varstr *key, ermia::varstr &value,
                                    ermia::str_arena *arena,
                                    uint64_t schema_version, ermia::FID fid,
                                    ermia::OID oid) {
    thread_local std::unordered_map<key_tuple<int, int, int>, float,
                                    key_tuple<int, int, int>::hash>
        key_sum_map;
    ermia::varstr *new_value = nullptr;
    if (fid == old_oorder_fid) {
      const char *keyp = (const char *)(key->p);
      oorder::key k_oo_temp;
      const oorder::key *k_oo = Decode(keyp, k_oo_temp);

      oorder::value v_oo_temp;
      const oorder::value *v_oo = Decode(value, v_oo_temp);

      auto it = key_sum_map.find(
          key_tuple<int, int, int>{k_oo->o_w_id, k_oo->o_d_id, k_oo->o_id});

      oorder_precompute_aggregate::value v_oo_pa;
      v_oo_pa.o_c_id = v_oo->o_c_id;
      v_oo_pa.o_carrier_id = v_oo->o_carrier_id;
      v_oo_pa.o_ol_cnt = v_oo->o_ol_cnt;
      v_oo_pa.o_all_local = v_oo->o_all_local;
      v_oo_pa.o_entry_d = v_oo->o_entry_d;
      v_oo_pa.o_total_amount = (it != key_sum_map.end()) ? it->second : 0;

      const size_t oorder_precompute_aggregate_sz = ::Size(v_oo_pa);
      new_value = arena->next(oorder_precompute_aggregate_sz);
      new_value = &Encode(*new_value, v_oo_pa);
    } else if (fid == old_order_line_fid) {
      const char *keyp = (const char *)(key->p);
      order_line::key k_ol_temp;
      const order_line::key *k_ol = Decode(keyp, k_ol_temp);

      order_line::value v_ol_temp;
      const order_line::value *v_ol = Decode(value, v_ol_temp);

      auto it = key_sum_map.find(key_tuple<int, int, int>{
          k_ol->ol_w_id, k_ol->ol_d_id, k_ol->ol_o_id});
      if (it != key_sum_map.end()) {
        it->second += v_ol->ol_amount;
      } else {
        key_sum_map.insert(std::make_pair<key_tuple<int, int, int> >(
            {k_ol->ol_w_id, k_ol->ol_d_id, k_ol->ol_o_id}, v_ol->ol_amount));
      }
    }

    return new_value;
  };

  uint64_t scan_reformat_idx = ermia::ddl::reformats.size();
#ifdef LAZYDDL
  oorder_schema.reformat_idx = ermia::ddl::reformats.size();
#endif
  ermia::ddl::reformats.push_back(precompute_aggregate_1);

#if !defined(LAZYDDL)
  oorder_schema.reformat_idx = ermia::ddl::reformats.size();
#endif
  order_line_schema.reformat_idx = ermia::ddl::reformats.size();
  ermia::ddl::reformats.push_back(precompute_aggregate_2);

  oorder_schema.secondary_index_key_create_idx = ermia::ddl::reformats.size();
  ermia::ddl::reformats.push_back(create_secondary_index_key);

  ermia::ddl::ddl_executor *ddl_exe = txn->get_ddl_executor();
  ddl_exe->set_ddl_type(oorder_schema.ddl_type);

  oorder_schema.old_v = oorder_schema.v;
  uint64_t schema_version = oorder_schema.old_v + 1;
  DLOG(INFO) << "Change to a new schema, version: " << schema_version;
  oorder_schema.v = schema_version;
  oorder_schema.old_td = oorder_schema.td;
  oorder_schema.show_index = true;

#ifdef COPYDDL
  oorder_schema.state = ermia::ddl::schema_state_type::NOT_READY;

  if (oorder_schema.ddl_type != ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
    char table_name[20];
    snprintf(table_name, 20, "oorder_%lu", schema_version);

    db->CreateTable(table_name);

    oorder_schema.td = ermia::Catalog::GetTable(table_name);
    oorder_schema.old_index = oorder_schema.index;
#ifdef LAZYDDL
    oorder_schema.old_tds[oorder_schema.old_v] = oorder_schema.old_td;
    oorder_schema.old_tds_total = oorder_schema.v;
#endif

#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
    db->CreateMasstreePrimaryIndex(table_name, std::string(table_name));
    db->CreateMasstreeSecondaryIndex(table_name, std::string(table_name));
#else
    oorder_schema.td->SetPrimaryIndex(oorder_schema.old_index, table_name);
    oorder_schema.td->AddSecondaryIndex(
        oorder_schema.old_td->GetSecIndexes().at(0));
#endif
    oorder_schema.index = oorder_schema.td->GetPrimaryIndex();

    ddl_exe->set_old_td(oorder_schema.old_td);
    ddl_exe->add_new_td_map(oorder_schema.td);
    ddl_exe->add_old_td_map(oorder_schema.old_td);
    ddl_exe->add_old_td_map(order_line_schema.td);
  } else {
    oorder_schema.reformats_total = oorder_schema.v;
    oorder_schema.reformats[oorder_schema.old_v] = scan_reformat_idx;
  }

  schema_kv::value new_schema_value;
  oorder_schema.record_to_value(new_schema_value);

  rc = ermia::catalog::write_schema(txn, schema_index, *oorder_key,
      Encode(str(Size(new_schema_value)), new_schema_value), oorder_oid);
  TryCatch(rc);

  ddl_exe->add_ddl_executor_paras(
      oorder_schema.v, oorder_schema.old_v, oorder_schema.ddl_type,
      oorder_schema.reformat_idx, oorder_schema.constraint_idx,
      oorder_schema.td, oorder_schema.old_td, oorder_schema.index,
      oorder_schema.state, oorder_schema.secondary_index_key_create_idx, true,
      true, scan_reformat_idx);
  ddl_exe->add_ddl_executor_paras(
      oorder_schema.v, oorder_schema.old_v, oorder_schema.ddl_type,
      order_line_schema.reformat_idx, oorder_schema.constraint_idx,
      oorder_schema.td, order_line_schema.td, oorder_schema.index,
      oorder_schema.state, -1, true, false, order_line_schema.reformat_idx);

  if (oorder_schema.ddl_type != ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
#if !defined(LAZYDDL)
    rc = rc_t{RC_INVALID};
    rc = ddl_exe->scan(txn, arena);
    TryCatch(rc);
#endif
  }
#elif BLOCKDDL
  oorder_schema.reformat_idx = scan_reformat_idx;

  schema_kv::value new_schema_value;
  oorder_schema.record_to_value(new_schema_value);

  TryCatch(ermia::catalog::write_schema(txn, schema_index, *oorder_key,
      Encode(str(Size(new_schema_value)), new_schema_value), oorder_oid));

  ddl_exe->add_ddl_executor_paras(
      oorder_schema.v, oorder_schema.old_v, oorder_schema.ddl_type,
      oorder_schema.reformat_idx, oorder_schema.constraint_idx,
      oorder_schema.td, oorder_schema.old_td, oorder_schema.index,
      oorder_schema.state);

  ddl_exe->set_old_td(oorder_schema.td);
  ddl_exe->add_old_td_map(oorder_schema.td);
  ddl_exe->add_new_td_map(oorder_schema.td);

  TryCatch(ddl_exe->scan(txn, arena));
#endif

  return rc_t{RC_TRUE};
}

rc_t tpcc_worker::create_index(ermia::transaction *txn, uint32_t ddl_example) {
  ermia::varstr valptr;
  rc_t rc = rc_t{RC_INVALID};
  ermia::OID oid = ermia::INVALID_OID;

  ermia::catalog::read_schema(txn, schema_index, *order_line_key, valptr, &oid);
  TryVerifyStrict(rc);

  struct ermia::schema_record schema;
  schema_kv::value schema_value_temp;
  const schema_kv::value *old_schema_value = Decode(valptr, schema_value_temp);
  schema.value_to_record(old_schema_value);
  schema.ddl_type = get_example_ddl_type(ddl_example);

  ermia::ddl::ddl_executor *ddl_exe = txn->get_ddl_executor();
  ddl_exe->set_ddl_type(schema.ddl_type);

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

  rc = ermia::catalog::write_schema(txn, schema_index, *order_line_key,
    Encode(str(Size(new_schema_value)), new_schema_value), oid);
  TryCatch(rc);

  ddl_exe->add_ddl_executor_paras(schema.v, schema.old_v, schema.ddl_type,
                                  schema.reformat_idx, schema.constraint_idx,
                                  schema.td, schema.old_td, schema.index,
                                  schema.state, -1, true, false, -1);

  ddl_exe->set_old_td(schema.td);
  ddl_exe->add_old_td_map(schema.td);

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

  ermia::catalog::read_schema(txn, schema_index, *order_line_key, valptr, &oid);
  TryVerifyStrict(rc);

  struct ermia::schema_record schema;
  schema_kv::value schema_value_temp;
  const schema_kv::value *old_schema_value = Decode(valptr, schema_value_temp);
  schema.value_to_record(old_schema_value);
  schema.ddl_type = get_example_ddl_type(ddl_example);

  schema.reformat_idx = ermia::ddl::reformats.size();
  ermia::ddl::reformats.push_back(order_line_stock_join);

  ermia::ddl::ddl_executor *ddl_exe = txn->get_ddl_executor();
  ddl_exe->set_ddl_type(schema.ddl_type);

  schema.old_v = schema.v;
  uint64_t schema_version = schema.old_v + 1;
  DLOG(INFO) << "Change to a new schema, version: " << schema_version;
  schema.v = schema_version;
  schema.old_td = schema.td;
  schema.show_index = true;

#ifdef COPYDDL
  schema.state = ermia::ddl::schema_state_type::NOT_READY;

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
    db->CreateMasstreePrimaryIndex(table_name, std::string(table_name));
#else
    schema.td->SetPrimaryIndex(schema.old_index, table_name);
#endif
    schema.index = schema.td->GetPrimaryIndex();

    ddl_exe->set_old_td(schema.old_td);
    ddl_exe->add_new_td_map(schema.td);
    ddl_exe->add_old_td_map(schema.old_td);
  } else {
    schema.reformats_total = schema.v;
  }

  schema_kv::value new_schema_value;
  schema.record_to_value(new_schema_value);

  rc = ermia::catalog::write_schema(txn, schema_index, *order_line_key,
    Encode(str(Size(new_schema_value)), new_schema_value), oid);
  TryCatch(rc);

  ddl_exe->add_ddl_executor_paras(schema.v, schema.old_v, schema.ddl_type,
                                  schema.reformat_idx, schema.constraint_idx,
                                  schema.td, schema.old_td, schema.index,
                                  schema.state);

  if (schema.ddl_type != ermia::ddl::ddl_type::NO_COPY_VERIFICATION) {
#if !defined(LAZYDDL)
    rc = rc_t{RC_INVALID};
    rc = ddl_exe->scan(txn, arena);
    TryCatch(rc);
#endif
  }
#elif BLOCKDDL
  schema_kv::value new_schema_value;
  schema.record_to_value(new_schema_value);

  TryCatch(ermia::catalog::write_schema(
      txn, schema_index, *order_line_key,
      Encode(str(Size(new_schema_value)), new_schema_value), oid));

  ddl_exe->add_ddl_executor_paras(schema.v, schema.old_v, schema.ddl_type,
                                  schema.reformat_idx, schema.constraint_idx,
                                  schema.td, schema.td, schema.index,
                                  ermia::ddl::schema_state_type::READY);

  ddl_exe->set_old_td(schema.td);
  ddl_exe->add_old_td_map(schema.td);
  ddl_exe->add_new_td_map(schema.td);

  TryCatch(ddl_exe->scan(txn, arena));
#endif
  return rc_t{RC_TRUE};
}
