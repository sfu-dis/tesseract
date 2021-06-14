#include "dbcore/rcu.h"
#include "dbcore/sm-thread.h"
#include "engine.h"
#include "txn.h"
#include "benchmarks/tpcc-common.h"

namespace ermia {

TableDescriptor *schema_td = NULL;

thread_local dlog::tls_log tlog;
dlog::tls_log *GetLog() {
  thread_local bool initialized = false;
  if (!initialized) {
    tlog.initialize(config::log_dir.c_str(),
                    thread::MyId(),
                    numa_node_of_cpu(sched_getcpu()),
                    config::log_buffer_mb,
                    config::log_segment_mb);
    initialized = true;
  }
  return &tlog;
}

class ddl_add_column_scan_callback : public OrderedIndex::ScanCallback {
 public:
  ddl_add_column_scan_callback(OrderedIndex *index, transaction *t, 
		  uint64_t schema_version, ermia::str_arena *arena)
	  : _index(index), _txn(t), _version(schema_version) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const varstr &value) {
    MARK_REFERENCED(value);
    // _arena->reset();

    varstr *k = _arena->next(keylen);
    if (!k) {
      _arena = new ermia::str_arena(ermia::config::arena_size_mb);
      k = _arena->next(keylen);
    }
    ASSERT(k);
    k->copy_from(keyp, keylen);

    /*
    char str2[sizeof(Schema2)];
    struct Schema2 record2;
    record2.v = _version;
    record2.a = _version;
    record2.b = _version;
    record2.c = _version;
    memcpy(str2, &record2, sizeof(str2));
    varstr *d_v = _arena->next(sizeof(str2));
    */

    order_line::value v_ol_temp;
    const order_line::value *v_ol = Decode(value, v_ol_temp);

    order_line_1::value v_ol_1;
    v_ol_1.ol_i_id = v_ol->ol_i_id;
    v_ol_1.ol_delivery_d = v_ol->ol_delivery_d;
    v_ol_1.ol_amount = v_ol->ol_amount;
    v_ol_1.ol_supply_w_id = v_ol->ol_supply_w_id;
    v_ol_1.ol_quantity = v_ol->ol_quantity;
    v_ol_1.v = _version;
    v_ol_1.ol_tax = 0;

    const size_t order_line_sz = Size(v_ol_1);
    varstr *d_v = _arena->next(order_line_sz);

    if (!d_v) {
      _arena = new ermia::str_arena(ermia::config::arena_size_mb);
      d_v = _arena->next(order_line_sz);
    }
    d_v = &Encode(*d_v, v_ol_1);

#if defined(BLOCKDDL) || defined(SIDDL)
    invoke_status = _index->UpdateRecord(_txn, *k, *d_v);
    if (invoke_status._val != RC_TRUE) {
      printf("DDL normal record update false\n");
      return false;
    }
#elif defined(COPYDDL)
    invoke_status = _index->InsertRecord(_txn, *k, *d_v);
    if (invoke_status._val != RC_TRUE) {
      printf("DDL normal record insert false\n");
      return false;
    }
#endif
    /*rc_t rc = rc_t{RC_INVALID};
    ermia::varstr valptr;
    order_line_1::value v_ol_1_temp;
    _index->GetRecord(_txn, rc, *k, valptr);
    if (rc._val != RC_TRUE) printf("DDL insert failed\n");
    const order_line_1::value *v_ol_2 = Decode(valptr, v_ol_1_temp);
    ALWAYS_ASSERT(v_ol_2->ol_tax == 0);
    ALWAYS_ASSERT(v_ol_2->v == _version);
    */
    return true;
  }
  std::vector<varstr *> output;
  OrderedIndex *_index;
  transaction *_txn;
  uint64_t _version;
  ermia::str_arena *_arena = new ermia::str_arena(ermia::config::arena_size_mb);
  rc_t invoke_status = rc_t{RC_TRUE};
};

rc_t ConcurrentMasstreeIndex::WriteSchemaTable(transaction *t, rc_t &rc, const varstr &key, varstr &value) {
  rc = UpdateRecord(t, key, value);
  if (rc._val != RC_TRUE) {
    printf("DDL schema update false\n");
    return rc;
  }

  // printf("Update schema table ok\n");
  return rc;
}

void ConcurrentMasstreeIndex::ReadSchemaTable(transaction *t, rc_t &rc, const varstr &key,
                                        varstr &value, OID *out_oid) {
  GetRecord(t, rc, key, value, out_oid);
  if (rc._val != RC_TRUE) printf("Read schema table failed\n");
  ALWAYS_ASSERT(rc._val == RC_TRUE);

#ifdef COPYDDL
  if (t->is_dml()) {
    struct Schema_record schema;
    memcpy(&schema, (char *)value.data(), sizeof(schema));
    t->schema_read_map[schema.td] = *out_oid;
  }
#endif
}

rc_t ConcurrentMasstreeIndex::WriteNormalTable(str_arena *arena, OrderedIndex *index, transaction *t, varstr &value) {
  rc_t r;
#ifdef COPYDDL
  struct Schema_record schema;
#else
  struct Schema_base schema;
#endif
  memcpy(&schema, (char *)value.data(), sizeof(schema));
  uint64_t schema_version = schema.v;

  ddl_add_column_scan_callback c_add_column(this, t, schema_version, arena);

  // Here we assume we can get table and index information
  // such as statistical info (MIN, MAX, AVG, etc.) and index key type
  /*
  char str1[sizeof(uint64_t)];
  uint64_t start = 0;
  memcpy(str1, &start, sizeof(str1));
  varstr *const start_key = arena->next(sizeof(str1));
  start_key->copy_from(str1, sizeof(str1));
  */

  printf("begin op\n");
  /*
  for (uint w = 1; w <= NumWarehouses(); w++) {
    printf("w: %u\n", w);
    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
      // printf("d: %u\n", d);
      for (uint c = 1; c <= NumCustomersPerDistrict(); c++) {
	// printf("c: %u\n", c);
	const order_line::key k_ol_0(w, d, c, 0);
	const order_line::key k_ol_1(w, d, c,
                                 std::numeric_limits<int32_t>::max());
	varstr *start_key = arena->next(::Size(k_ol_0));
	if (!start_key) {
	  arena = new ermia::str_arena(ermia::config::arena_size_mb);
          start_key = arena->next(::Size(k_ol_0));
	}
	varstr *end_key = arena->next(::Size(k_ol_1));
        if (!end_key) {
	  arena = new ermia::str_arena(ermia::config::arena_size_mb);
          end_key = arena->next(::Size(k_ol_1));
	}
	r = index->Scan(t, Encode(*start_key, k_ol_0), &Encode(*end_key, k_ol_1), c_add_column, arena);
        if (r._val != RC_TRUE) {
          printf("DDL scan false\n");
          return r;
        }
      }
    }
  }*/

  const order_line::key k_ol_0(1, 1, 1, 1);
        // const order_line::key k_ol_1(w, d, c,
        //                          std::numeric_limits<int32_t>::max());
        varstr *start_key = arena->next(::Size(k_ol_0));
        if (!start_key) {
          arena = new ermia::str_arena(ermia::config::arena_size_mb);
          start_key = arena->next(::Size(k_ol_0));
        }
        /*varstr *end_key = arena->next(::Size(k_ol_1));
        if (!end_key) {
          arena = new ermia::str_arena(ermia::config::arena_size_mb);
          end_key = arena->next(::Size(k_ol_1));
        }*/
        // r = index->Scan(t, Encode(*start_key, k_ol_0), &Encode(*end_key, k_ol_1), c_add_column, arena);
        r = index->Scan(t, Encode(*start_key, k_ol_0), nullptr, c_add_column, arena);
	if (r._val != RC_TRUE) {
          printf("DDL scan false\n");
          return r;
        }

  /*r = index->Scan(t, *start_key, nullptr, c_add_column, arena);
  if (r._val != RC_TRUE) {
    printf("DDL scan false\n");
    return r;
  }*/

  printf("scan invoke status: %hu\n", c_add_column.invoke_status._val);

  return c_add_column.invoke_status;
}

class ddl_add_constraint_scan_callback : public OrderedIndex::ScanCallback {
 public:
  ddl_add_constraint_scan_callback(OrderedIndex *index, transaction *t, ermia::str_arena *arena)
          : _index(index), _txn(t), _arena(arena) {}
  virtual bool Invoke(const char *keyp, size_t keylen, const varstr &value) {
    MARK_REFERENCED(value);
    /*
    _arena->reset();

    varstr *k = _arena->next(keylen);
    ASSERT(k);
    k->copy_from(keyp, keylen);
    */

    return true;
  }
  std::vector<varstr *> output;
  OrderedIndex *_index;
  transaction *_txn;
  ermia::str_arena *_arena;
  rc_t invoke_status = rc_t{RC_TRUE};
};

rc_t ConcurrentMasstreeIndex::CheckNormalTable(str_arena *arena, OrderedIndex *index, transaction *t) {
  rc_t r;

  ddl_add_constraint_scan_callback c_add_constraint(this, t, arena);

  char str1[sizeof(uint64_t)];
  uint64_t start = 0;
  memcpy(str1, &start, sizeof(str1));
  varstr *const start_key = arena->next(sizeof(str1));
  start_key->copy_from(str1, sizeof(str1));

  r = index->Scan(t, *start_key, nullptr, c_add_constraint, arena);
  if (r._val != RC_TRUE) {
    printf("DDL scan false\n");
    return r;
  }

  return c_add_constraint.invoke_status;
}

// Engine initialization, including creating the OID, log, and checkpoint
// managers and recovery if needed.
Engine::Engine() {
  config::sanity_check();
  ALWAYS_ASSERT(config::log_dir.size());
  ALWAYS_ASSERT(!oidmgr);
  sm_oid_mgr::create();
  ALWAYS_ASSERT(oidmgr);
}

TableDescriptor *Engine::CreateTable(const char *name) {
  auto *td = Catalog::NewTable(name);

  if (true) { //!sm_log::need_recovery) {
    // Note: this will insert to the log and therefore affect min_flush_lsn,
    // so must be done in an sm-thread which must be created by the user
    // application (not here in ERMIA library).
    //ASSERT(ermia::logmgr);

    // TODO(tzwang): perhaps make this transactional to allocate it from
    // transaction string arena to avoid malloc-ing memory (~10k size).
    //char *log_space = (char *)malloc(sizeof(sm_tx_log));
    //ermia::sm_tx_log *log = ermia::logmgr->new_tx_log(log_space);
    td->Initialize();
    //log->log_table(td->GetTupleFid(), td->GetKeyFid(), td->GetName());
    //log->commit(nullptr);
    //free(log_space);
  }
  return td;
}

void Engine::LogIndexCreation(bool primary, FID table_fid, FID index_fid, const std::string &index_name) {
  /*
  if (!sm_log::need_recovery) {
    // Note: this will insert to the log and therefore affect min_flush_lsn,
    // so must be done in an sm-thread which must be created by the user
    // application (not here in ERMIA library).
    ASSERT(ermia::logmgr);

    // TODO(tzwang): perhaps make this transactional to allocate it from
    // transaction string arena to avoid malloc-ing memory (~10k size).
    char *log_space = (char *)malloc(sizeof(sm_tx_log));
    ermia::sm_tx_log *log = ermia::logmgr->new_tx_log(log_space);
    log->log_index(table_fid, index_fid, index_name, primary);
    log->commit(nullptr);
    free(log_space);
  }
  */
}

void Engine::CreateIndex(const char *table_name, const std::string &index_name, bool is_primary) {
  auto *td = Catalog::GetTable(table_name);
  ALWAYS_ASSERT(td);
  auto *index = new ConcurrentMasstreeIndex(table_name, is_primary);
  if (is_primary) {
    td->SetPrimaryIndex(index, index_name);
  } else {
    td->AddSecondaryIndex(index, index_name);
  }
  FID index_fid = index->GetIndexFid();
  LogIndexCreation(is_primary, td->GetTupleFid(), index_fid, index_name);
}

PROMISE(rc_t) ConcurrentMasstreeIndex::Scan(transaction *t, const varstr &start_key,
                                   const varstr *end_key, ScanCallback &callback, str_arena *arena) {
  SearchRangeCallback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);

  t->ensure_active();
  if (end_key) {
    VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                      << ")::search_range_call [" << util::hexify(start_key)
                      << ", " << util::hexify(*end_key) << ")" << std::endl);
  } else {
    VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                      << ")::search_range_call [" << util::hexify(start_key)
                      << ", +inf)" << std::endl);
  }

  if (!unlikely(end_key && *end_key <= start_key)) {
    XctSearchRangeCallback cb(t, &c);
    AWAIT masstree_.search_range_call(start_key, end_key ? end_key : nullptr, cb, t->xc);
  }
  RETURN c.return_code;
}

PROMISE(rc_t) ConcurrentMasstreeIndex::ReverseScan(transaction *t,
                                          const varstr &start_key,
                                          const varstr *end_key,
                                          ScanCallback &callback,
					  str_arena *arena) {
  SearchRangeCallback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);

  t->ensure_active();
  if (!unlikely(end_key && start_key <= *end_key)) {
    XctSearchRangeCallback cb(t, &c);

    varstr lowervk;
    if (end_key) {
      lowervk = *end_key;
    }
    AWAIT masstree_.rsearch_range_call(start_key, end_key ? &lowervk : nullptr, cb,
                                 t->xc);
  }
  RETURN c.return_code;
}

std::map<std::string, uint64_t> ConcurrentMasstreeIndex::Clear() {
  PurgeTreeWalker w;
  masstree_.tree_walk(w);
  masstree_.clear();
  return std::map<std::string, uint64_t>();
}

PROMISE(void) ConcurrentMasstreeIndex::GetRecord(transaction *t, rc_t &rc, const varstr &key,
                                        varstr &value, OID *out_oid) {
  OID oid = INVALID_OID;
  rc = {RC_INVALID};
  ConcurrentMasstree::versioned_node_t sinfo;

  if (!t) {
    auto e = MM::epoch_enter();
    rc._val = AWAIT masstree_.search(key, oid, e, &sinfo) ? RC_TRUE : RC_FALSE;
    MM::epoch_exit(0, e);
  } else {
    t->ensure_active();
    bool found = AWAIT masstree_.search(key, oid, t->xc->begin_epoch, &sinfo);

    dbtuple *tuple = nullptr;
    if (found) {
      // Key-OID mapping exists, now try to get the actual tuple to be sure
      tuple = AWAIT oidmgr->oid_get_version(table_descriptor->GetTupleArray(), oid, t->xc);
      if (!tuple) {
        found = false;
      }
    }

    if (found) {
      volatile_write(rc._val, t->DoTupleRead(tuple, &value)._val);
    } else if (config::phantom_prot) {
      volatile_write(rc._val, DoNodeRead(t, sinfo.first, sinfo.second)._val);
    } else {
      volatile_write(rc._val, RC_FALSE);
    }
#ifndef SSN
    ASSERT(rc._val == RC_FALSE || rc._val == RC_TRUE);
#endif
  }

  if (out_oid) {
    *out_oid = oid;
  }
}

void ConcurrentMasstreeIndex::PurgeTreeWalker::on_node_begin(
    const typename ConcurrentMasstree::node_opaque_t *n) {
  ASSERT(spec_values.empty());
  spec_values = ConcurrentMasstree::ExtractValues(n);
}

void ConcurrentMasstreeIndex::PurgeTreeWalker::on_node_success() {
  spec_values.clear();
}

void ConcurrentMasstreeIndex::PurgeTreeWalker::on_node_failure() {
  spec_values.clear();
}

PROMISE(bool) ConcurrentMasstreeIndex::InsertIfAbsent(transaction *t, const varstr &key,
                                             OID oid) {
  typename ConcurrentMasstree::insert_info_t ins_info;
  bool inserted = AWAIT masstree_.insert_if_absent(key, oid, t->xc, &ins_info);

  if (!inserted) {
    RETURN false;
  }

  if (config::phantom_prot && !t->masstree_absent_set.empty()) {
    // Update node version number
    ASSERT(ins_info.node);
    auto it = t->masstree_absent_set.find(ins_info.node);
    if (it != t->masstree_absent_set.end()) {
      if (unlikely(it->second != ins_info.old_version)) {
        // Important: caller should unlink the version, otherwise we risk
        // leaving a dead version at chain head -> infinite loop or segfault...
        RETURN false;
      }
      // otherwise, bump the version
      it->second = ins_info.new_version;
    }
  }
  RETURN true;
}

////////////////// Index interfaces /////////////////

PROMISE(bool) ConcurrentMasstreeIndex::InsertOID(transaction *t, const varstr &key, OID oid) {
  bool inserted = AWAIT InsertIfAbsent(t, key, oid);
  if (inserted) {
    t->LogIndexInsert(this, oid, &key);
    if (config::enable_chkpt) {
      auto *key_array = GetTableDescriptor()->GetKeyArray();
      volatile_write(key_array->get(oid)->_ptr, 0);
    }
  }
  RETURN inserted;
}

PROMISE(rc_t) ConcurrentMasstreeIndex::InsertRecord(transaction *t, const varstr &key, varstr &value, OID *out_oid) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  ASSERT((char *)key.data() == (char *)&key + sizeof(varstr));
  t->ensure_active();

  // Insert to the table first
  dbtuple *tuple = nullptr;
  OID oid = t->Insert(table_descriptor, &value, &tuple);

  // Done with table record, now set up index
  ASSERT((char *)key.data() == (char *)&key + sizeof(varstr));
  if (!AWAIT InsertOID(t, key, oid)) {
    if (config::enable_chkpt) {
      volatile_write(table_descriptor->GetKeyArray()->get(oid)->_ptr, 0);
    }
    RETURN rc_t{RC_ABORT_INTERNAL};
  }

  // Succeeded, now put the key there if we need it
  if (config::enable_chkpt) {
    // XXX(tzwang): only need to install this key if we need chkpt; not a
    // realistic setting here to not generate it, the purpose of skipping
    // this is solely for benchmarking CC.
    varstr *new_key =
        (varstr *)MM::allocate(sizeof(varstr) + key.size());
    new (new_key) varstr((char *)new_key + sizeof(varstr), 0);
    new_key->copy_from(&key);
    auto *key_array = table_descriptor->GetKeyArray();
    key_array->ensure_size(oid);
    oidmgr->oid_put(key_array, oid,
                    fat_ptr::make((void *)new_key, INVALID_SIZE_CODE));
  }

  if (out_oid) {
    *out_oid = oid;
  }

  RETURN rc_t{RC_TRUE};
}

PROMISE(rc_t) ConcurrentMasstreeIndex::UpdateRecord(transaction *t, const varstr &key, varstr &value) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  // Search for OID
  OID oid = 0;
  rc_t rc = {RC_INVALID};
  AWAIT GetOID(key, rc, t->xc, oid);

  if (rc._val == RC_TRUE) {
    rc_t rc = t->Update(table_descriptor, oid, &key, &value);
    RETURN rc;
  } else {
    RETURN rc_t{RC_ABORT_INTERNAL};
  }
}

PROMISE(rc_t) ConcurrentMasstreeIndex::RemoveRecord(transaction *t, const varstr &key) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  // Search for OID
  OID oid = 0;
  rc_t rc = {RC_INVALID};
  AWAIT GetOID(key, rc, t->xc, oid);

  if (rc._val == RC_TRUE) {
    // Allocate an empty record version as the "new" version
    varstr *null_val = t->string_allocator().next(0);
    RETURN t->Update(table_descriptor, oid, &key, null_val);
  } else {
    RETURN rc_t{RC_ABORT_INTERNAL};
  }
}

rc_t ConcurrentMasstreeIndex::DoNodeRead(
    transaction *t, const ConcurrentMasstree::node_opaque_t *node,
    uint64_t version) {
  ALWAYS_ASSERT(config::phantom_prot);
  ASSERT(node);
  auto it = t->masstree_absent_set.find(node);
  if (it == t->masstree_absent_set.end()) {
    t->masstree_absent_set[node] = version;
  } else if (it->second != version) {
    return rc_t{RC_ABORT_PHANTOM};
  }
  return rc_t{RC_TRUE};
}

void ConcurrentMasstreeIndex::XctSearchRangeCallback::on_resp_node(
    const typename ConcurrentMasstree::node_opaque_t *n, uint64_t version) {
  VERBOSE(std::cerr << "on_resp_node(): <node=0x" << util::hexify(intptr_t(n))
                    << ", version=" << version << ">" << std::endl);
  VERBOSE(std::cerr << "  " << ConcurrentMasstree::NodeStringify(n)
                    << std::endl);
  if (config::phantom_prot) {
#ifdef SSN
    if (t->flags & transaction::TXN_FLAG_READ_ONLY) {
      return;
    }
#endif
    rc_t rc = DoNodeRead(t, n, version);
    if (rc.IsAbort()) {
      caller_callback->return_code = rc;
    }
  }
}

bool ConcurrentMasstreeIndex::XctSearchRangeCallback::invoke(
    const ConcurrentMasstree *btr_ptr,
    const typename ConcurrentMasstree::string_type &k, dbtuple *v,
    const typename ConcurrentMasstree::node_opaque_t *n, uint64_t version) {
  MARK_REFERENCED(btr_ptr);
  MARK_REFERENCED(n);
  MARK_REFERENCED(version);
  t->ensure_active();
  VERBOSE(std::cerr << "search range k: " << util::hexify(k) << " from <node=0x"
                    << util::hexify(n) << ", version=" << version << ">"
                    << std::endl
                    << "  " << *((dbtuple *)v) << std::endl);
  varstr vv;
  caller_callback->return_code = t->DoTupleRead(v, &vv);
  if (caller_callback->return_code._val == RC_TRUE) {
    return caller_callback->Invoke(k, vv);
  } else if (caller_callback->return_code.IsAbort()) {
    // don't continue the read if the tx should abort
    // ^^^^^ note: see masstree_scan.hh, whose scan() calls
    // visit_value(), which calls this function to determine
    // if it should stop reading.
    return false; // don't continue the read if the tx should abort
  }
  return true;
}

////////////////// End of index interfaces //////////

////////////////// Table interfaces /////////////////
rc_t Table::Insert(transaction &t, varstr *value, OID *out_oid) {
  t.ensure_active();
  OID oid = t.Insert(td, value);
  if (out_oid) {
    *out_oid = oid;
  }
  return oid == INVALID_OID ? rc_t{RC_FALSE} : rc_t{RC_FALSE};
}

rc_t Table::Read(transaction &t, OID oid, varstr *out_value) {
  auto *tuple = sync_wait_coro(oidmgr->oid_get_version(td->GetTupleArray(), oid, t.GetXIDContext()));
  rc_t rc = {RC_INVALID};
  if (tuple) {
    // Record exists
    volatile_write(rc._val, t.DoTupleRead(tuple, out_value)._val);
  } else {
    volatile_write(rc._val, RC_FALSE);
  }
  ASSERT(rc._val == RC_FALSE || rc._val == RC_TRUE);
  return rc;
}

rc_t Table::Update(transaction &t, OID oid, varstr &value) {
  return t.Update(td, oid, &value);
}

rc_t Table::Remove(transaction &t, OID oid) {
  return t.Update(td, oid, nullptr);
}

////////////////// End of Table interfaces //////////

OrderedIndex::OrderedIndex(std::string table_name, bool is_primary) : is_primary(is_primary) {
  table_descriptor = Catalog::GetTable(table_name);
  self_fid = oidmgr->create_file(true);
}

} // namespace ermia
