#include "engine.h"

#include "dbcore/rcu.h"
#include "dbcore/sm-thread.h"
#include "txn.h"

namespace ermia {

TableDescriptor *schema_td = NULL;

thread_local dlog::tls_log tlog;
std::mutex tlog_lock;

dlog::tls_log *GetLog(uint32_t logid) {
  // XXX(tzwang): this lock may become a problem; should be safe to not use it -
  // the set of tlogs are stable before the system starts to run, i.e., only
  // needed when creating logs
  std::lock_guard<std::mutex> guard(tlog_lock);
  return dlog::tlogs[logid];
}

dlog::tls_log *GetLog() {
  thread_local bool initialized = false;
  if (!initialized) {
    std::lock_guard<std::mutex> guard(tlog_lock);
    tlog.initialize(config::log_dir.c_str(), dlog::tlogs.size(),
                    numa_node_of_cpu(sched_getcpu()), config::log_buffer_kb,
                    config::log_segment_mb);
    initialized = true;
    dlog::tlogs.push_back(&tlog);
  }
  return &tlog;
}

// Engine initialization, including creating the OID, log, and checkpoint
// managers and recovery if needed.
Engine::Engine() {
  config::sanity_check();
  ALWAYS_ASSERT(config::log_dir.size());
  ALWAYS_ASSERT(!oidmgr);
  sm_oid_mgr::create();
  ALWAYS_ASSERT(oidmgr);
  ermia::dlog::initialize();
}

Engine::~Engine() { ermia::dlog::uninitialize(); }

TableDescriptor *Engine::CreateTable(const char *name, bool modify_hash_table) {
  auto *td = Catalog::NewTable(name);
  td->Initialize(modify_hash_table);
  return td;
}

void Engine::LogIndexCreation(bool primary, FID table_fid, FID index_fid,
                              const std::string &index_name) {
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

void Engine::CreateIndex(const char *table_name, const std::string &index_name,
                         bool is_primary) {
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

PROMISE(rc_t)
ConcurrentMasstreeIndex::Scan(transaction *t, const varstr &start_key,
                              const varstr *end_key, ScanCallback &callback,
                              schema_record *schema) {
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
    XctSearchRangeCallback cb(t, &c, schema, table_descriptor);
    // TODO: support table scan on secondary keys
    if (GetIsPrimary() && schema && !schema->show_index) {
      t->table_scan_multi(table_descriptor, &start_key, end_key, cb);
    } else {
      AWAIT masstree_.search_range_call(start_key, end_key ? end_key : nullptr,
                                        cb, t->xc);
    }
  }
  RETURN c.return_code;
}

PROMISE(rc_t)
ConcurrentMasstreeIndex::ReverseScan(transaction *t, const varstr &start_key,
                                     const varstr *end_key,
                                     ScanCallback &callback,
                                     schema_record *schema) {
  SearchRangeCallback c(callback);
  ASSERT(c.return_code._val == RC_FALSE);

  t->ensure_active();
  if (!unlikely(end_key && start_key <= *end_key)) {
    XctSearchRangeCallback cb(t, &c, schema, table_descriptor);

    varstr lowervk;
    if (end_key) {
      lowervk = *end_key;
    }
    // TODO: support table scan on secondary keys
    if (GetIsPrimary() && schema && !schema->show_index) {
      t->table_rscan_multi(table_descriptor, &start_key,
                           end_key ? &lowervk : nullptr, cb);
    } else {
      AWAIT masstree_.rsearch_range_call(
          start_key, end_key ? &lowervk : nullptr, cb, t->xc);
    }
  }
  RETURN c.return_code;
}

std::map<std::string, uint64_t> ConcurrentMasstreeIndex::Clear() {
  PurgeTreeWalker w;
  masstree_.tree_walk(w);
  masstree_.clear();
  return std::map<std::string, uint64_t>();
}

PROMISE(void)
ConcurrentMasstreeIndex::GetRecord(transaction *t, rc_t &rc, const varstr &key,
                                   varstr &value, OID *out_oid,
                                   schema_record *schema) {
  OID oid = INVALID_OID;
  rc = {RC_INVALID};
  ConcurrentMasstree::versioned_node_t sinfo;

  if (!t) {
    auto e = MM::epoch_enter();
    rc._val = AWAIT masstree_.search(key, oid, e, &sinfo) ? RC_TRUE : RC_FALSE;
    MM::epoch_exit(0, e);
  } else {
    t->ensure_active();
    bool found = false;
    if (schema && !schema->show_index) {
      t->table_scan_single(table_descriptor, &key, oid);
      found = oid ? true : false;
    } else {
      found = AWAIT masstree_.search(key, oid, t->xc->begin_epoch, &sinfo);
    }

    dbtuple *tuple = nullptr;

#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
    if (schema && schema->old_index && !found) {
    lazy_retry:
      varstr old_value;
      ((ConcurrentMasstreeIndex *)(schema->old_index))
          ->GetRecord(t, rc, key, old_value, &oid);
      if (rc._val == RC_TRUE) {
        auto *key_array = schema->old_td->GetKeyArray();
        fat_ptr *entry =
            config::enable_ddl_keys ? key_array->get(oid) : nullptr;
        varstr *k = entry ? (varstr *)((*entry).offset()) : nullptr;
        varstr *new_tuple_value = ddl::reformats[schema->reformat_idx](
            k, old_value, &(t->string_allocator()), schema->v,
            schema->old_td->GetTupleFid(), oid);
        if (!new_tuple_value) {
          volatile_write(rc._val, RC_ABORT_INTERNAL);
          RETURN;
        }
        rc = AWAIT InsertRecord(t, key, *new_tuple_value, &oid, schema);
        if (rc._val == RC_TRUE) {
          rc = AWAIT LazyBuildSecondaryIndex(t, key, oid, schema);
        }
        if (rc._val == RC_TRUE) {
          tuple = AWAIT oidmgr->oid_get_version(schema->td->GetTupleArray(),
                                                oid, t->xc);
          if (tuple && t->DoTupleRead(tuple, &value)._val == RC_TRUE) {
            volatile_write(rc._val, RC_TRUE);
            RETURN;
          }
        }
      }
      volatile_write(rc._val, RC_ABORT_INTERNAL);
      RETURN;
    }
#endif

    if (found) {
      // Key-OID mapping exists, now try to get the actual tuple to be sure
      tuple = AWAIT oidmgr->oid_get_version(table_descriptor->GetTupleArray(),
                                            oid, t->xc);

#if defined(COPYDDL) && !defined(LAZYDDL)
      if (t->IsWaitForNewSchema() && tuple) {
	ermia::transaction::table_record_t *table_record = t->find_in_table_set(table_descriptor->GetTupleFid());
	if (table_record && !table_record->schema_ready && AWAIT t->OverlapCheck(table_descriptor, table_record->old_table_desc, oid)) {
          volatile_write(rc._val, RC_ABORT_INTERNAL);
          RETURN;
        }
      }
#endif

      if (schema && table_descriptor != schema->td) {
	volatile_write(rc._val, RC_ABORT_INTERNAL);
        RETURN;
      }

      if (!tuple) {
        found = false;
#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
        goto lazy_retry;
#endif
      }
    }

#ifdef OPTLAZYDDL
    if (!found && schema && oid != INVALID_OID) {
      int total = schema->v;
      if (!total) {
        volatile_write(rc._val, RC_ABORT_INTERNAL);
        RETURN;
      }
      TableDescriptor *old_table_descriptor = nullptr;
    retry:
      old_table_descriptor = schema->old_tds[total - 1];
      if (old_table_descriptor == nullptr) {
        if (--total > 0) {
          goto retry;
        }
        volatile_write(rc._val, RC_ABORT_INTERNAL);
        RETURN;
      }
      old_table_descriptor->GetTupleArray()->ensure_size(oid);
      tuple = AWAIT oidmgr->oid_get_version(
          old_table_descriptor->GetTupleArray(), oid, t->xc);

      if (!tuple) {
        if (--total > 0) {
          goto retry;
        }
        volatile_write(rc._val, RC_ABORT_INTERNAL);
        RETURN;
      }

      if (t->DoTupleRead(tuple, &value)._val == RC_TRUE) {
        auto *key_array = old_table_descriptor->GetKeyArray();
        fat_ptr *entry =
            config::enable_ddl_keys ? key_array->get(oid) : nullptr;
        varstr *key = entry ? (varstr *)((*entry).offset()) : nullptr;
        varstr *new_tuple_value = ddl::reformats[schema->reformat_idx](
            key, value, &(t->string_allocator()), schema->v,
            old_table_descriptor->GetTupleFid(), oid);
        if (!new_tuple_value) {
          volatile_write(rc._val, RC_ABORT_INTERNAL);
          RETURN;
        }
        rc_t r = AWAIT t->DDLInsert(schema->td, oid, new_tuple_value, 0);
        if (r._val != RC_TRUE || table_descriptor != schema->td) {
          volatile_write(rc._val, RC_ABORT_INTERNAL);
          RETURN;
        }
        tuple = AWAIT oidmgr->oid_get_version(schema->td->GetTupleArray(), oid,
                                              t->xc);
        if (tuple && t->DoTupleRead(tuple, &value)._val == RC_TRUE) {
          found = true;
        } else {
          volatile_write(rc._val, RC_ABORT_INTERNAL);
          RETURN;
        }
      }
    }
#endif

    if (found) {
      volatile_write(rc._val, t->DoTupleRead(tuple, &value)._val);
#ifdef SIDDL
      // Normally, SI DDL would fail, however, when there is no writes during
      // DDL, SI DDL can proceed. After post-commit, there is an issue: A
      // transaction with a begin timestamp (which is equal to DDL csn) can read
      // an old schema record, but it also can read a latest normal table
      // record, causing inconsistency. Thus, when it happens, just abort.
      if (rc._val == RC_TRUE && tuple->GetCSN() == t->xc->begin) {
        volatile_write(rc._val, RC_ABORT_INTERNAL);
        RETURN;
      }
#endif
      if (rc._val == RC_TRUE && schema &&
          schema->ddl_type == ddl::ddl_type::NO_COPY_VERIFICATION &&
          tuple->GetCSN() <= schema->csn) {
        auto *key_array = table_descriptor->GetKeyArray();
        fat_ptr *entry =
            config::enable_ddl_keys ? key_array->get(oid) : nullptr;
        varstr *key = entry ? (varstr *)((*entry).offset()) : nullptr;
        varstr *new_tuple_value = &value;
        for (int i = 0; i < schema->reformats_total; i++) {
          new_tuple_value = ddl::reformats[schema->reformats[i]](
              key, *new_tuple_value, &(t->string_allocator()), schema->v,
              table_descriptor->GetTupleFid(), oid);
        }
        if (!new_tuple_value) {
          volatile_write(rc._val, RC_ABORT_INTERNAL);
          RETURN;
        }
        value.p = new_tuple_value->p;
        value.l = new_tuple_value->l;
      }
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

PROMISE(bool)
ConcurrentMasstreeIndex::InsertIfAbsent(transaction *t, const varstr &key,
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

PROMISE(bool)
ConcurrentMasstreeIndex::InsertOID(transaction *t, const varstr &key, OID oid) {
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

#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
PROMISE(rc_t)
ConcurrentMasstreeIndex::LazyBuildSecondaryIndex(transaction *t,
                                                 const varstr &key, OID oid,
                                                 schema_record *schema) {
  if (table_descriptor->GetSecIndexes().size() && schema &&
      schema->secondary_index_key_create_idx != -1 && schema->old_index) {
    rc_t rc = {RC_INVALID};
    varstr old_value;
    ((ConcurrentMasstreeIndex *)(schema->old_index))
        ->GetRecord(t, rc, key, old_value);
    if (rc._val == RC_TRUE) {
      ConcurrentMasstreeIndex *secondary_index =
          (ConcurrentMasstreeIndex *)(table_descriptor->GetSecIndexes()
                                          .front());
      varstr *k = new varstr(key.data(), key.size());
      varstr *new_secondary_index_key =
          ddl::reformats[schema->secondary_index_key_create_idx](
              k, old_value, &(t->string_allocator()), schema->v, 0, oid);
      if (!AWAIT secondary_index->InsertOID(t, *new_secondary_index_key, oid)) {
        RETURN rc_t{RC_ABORT_INTERNAL};
      }
    } else {
      RETURN rc_t{RC_ABORT_INTERNAL};
    }
  }
  RETURN rc_t{RC_TRUE};
}
#endif

PROMISE(rc_t)
ConcurrentMasstreeIndex::InsertRecord(transaction *t, const varstr &key,
                                      varstr &value, OID *out_oid,
                                      schema_record *schema) {
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
  if (config::enable_chkpt || config::enable_ddl_keys) {
    // XXX(tzwang): only need to install this key if we need chkpt; not a
    // realistic setting here to not generate it, the purpose of skipping
    // this is solely for benchmarking CC.
    varstr *new_key = (varstr *)MM::allocate(sizeof(varstr) + key.size());
    new (new_key) varstr((char *)new_key + sizeof(varstr), 0);
    new_key->copy_from(&key);
    auto *key_array = table_descriptor->GetKeyArray();
    key_array->ensure_size(oid);
    oidmgr->oid_put(key_array, oid,
                    fat_ptr::make((void *)new_key, INVALID_SIZE_CODE));
  }

  if (schema && table_descriptor != schema->td) {
    RETURN rc_t{RC_ABORT_INTERNAL};
  }

  if (out_oid) {
    *out_oid = oid;
  }

  RETURN rc_t{RC_TRUE};
}

PROMISE(rc_t)
ConcurrentMasstreeIndex::UpdateRecord(transaction *t, const varstr &key,
                                      varstr &value, schema_record *schema) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  // Search for OID
  OID oid = 0;
  rc_t rc = {RC_INVALID};
  if (schema && !schema->show_index) {
    rc = AWAIT t->table_scan_single(table_descriptor, &key, oid);
  } else {
    AWAIT GetOID(key, rc, t->xc, oid);
  }

#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
  if (schema && schema->old_index && rc._val != RC_TRUE) {
    OID out_oid = INVALID_OID;
    varstr old_value;
    ((ConcurrentMasstreeIndex *)(schema->old_index))
        ->GetRecord(t, rc, key, old_value, &out_oid);
    if (rc._val == RC_TRUE) {
      rc = AWAIT InsertRecord(t, key, value, &out_oid, schema);
      if (rc._val == RC_TRUE) {
        rc = AWAIT LazyBuildSecondaryIndex(t, key, out_oid, schema);
      }
    } else {
      rc = rc_t{RC_ABORT_INTERNAL};
    }
    RETURN rc;
  }
#endif

#ifdef OPTLAZYDDL
  if (rc._val == RC_TRUE && schema &&
      *(table_descriptor->GetTupleArray()->get(oid)) == NULL_PTR) {
    int total = schema->v;
    if (!total) {
      RETURN rc_t{RC_ABORT_INTERNAL};
    }
    TableDescriptor *old_table_descriptor = nullptr;
  retry:
    old_table_descriptor = schema->old_tds[total - 1];
    if (old_table_descriptor == nullptr) {
      if (--total > 0) {
        goto retry;
      }
      RETURN rc_t{RC_ABORT_INTERNAL};
    }
    dbtuple *tuple = AWAIT oidmgr->oid_get_version(
        old_table_descriptor->GetTupleArray(), oid, t->xc);

    if (!tuple) {
      if (--total > 0) {
        goto retry;
      }
      RETURN rc_t{RC_ABORT_INTERNAL};
    }
    rc_t rc = rc_t{RC_INVALID};
    rc = AWAIT t->DDLInsert(schema->td, oid, &value, 0);
    if (table_descriptor != schema->td) {
      RETURN rc_t{RC_ABORT_INTERNAL};
    }
    RETURN rc;
  }
#endif

  if (rc._val == RC_TRUE) {
    rc_t rc = rc_t{RC_INVALID};
    rc = AWAIT t->Update(table_descriptor, oid, &key, &value);
    if (schema && table_descriptor != schema->td) {
      RETURN rc_t{RC_ABORT_INTERNAL};
    }
    RETURN rc;
  } else {
    RETURN rc_t{RC_ABORT_INTERNAL};
  }
}

PROMISE(rc_t)
ConcurrentMasstreeIndex::RemoveRecord(transaction *t, const varstr &key,
                                      schema_record *schema) {
  // For primary index only
  ALWAYS_ASSERT(IsPrimary());

  // Search for OID
  OID oid = 0;
  rc_t rc = {RC_INVALID};
  if (schema && !schema->show_index) {
    rc = AWAIT t->table_scan_single(table_descriptor, &key, oid);
  } else {
    AWAIT GetOID(key, rc, t->xc, oid);
  }

#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
  if (schema && schema->old_index && rc._val != RC_TRUE) {
    RETURN rc_t{RC_TRUE};
  }
#endif

#ifdef OPTLAZYDDL
  if (rc._val != RC_TRUE && schema->old_td) {
    return rc_t{RC_TRUE};
  }
#endif

  if (rc._val == RC_TRUE) {
    // Allocate an empty record version as the "new" version
    varstr *null_val = t->string_allocator().next(0);
    rc_t rc = AWAIT t->Update(table_descriptor, oid, &key, null_val);
    if (schema && table_descriptor != schema->td) {
      RETURN rc_t{RC_ABORT_INTERNAL};
    }
    RETURN rc;
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
    const typename ConcurrentMasstree::node_opaque_t *n, uint64_t version,
    OID oid) {
  MARK_REFERENCED(btr_ptr);
  MARK_REFERENCED(n);
  MARK_REFERENCED(version);
  t->ensure_active();
  VERBOSE(std::cerr << "search range k: " << util::hexify(k) << " from <node=0x"
                    << util::hexify(n) << ", version=" << version << ">"
                    << std::endl
                    << "  " << *((dbtuple *)v) << std::endl);
  varstr vv;
#ifdef OPTLAZYDDL
  caller_callback->return_code =
      v ? t->DoTupleRead(v, &vv) : rc_t{RC_ABORT_INTERNAL};
#else
  caller_callback->return_code = t->DoTupleRead(v, &vv);
#endif
  if (schema && table_descriptor != schema->td) {
    caller_callback->return_code = rc_t{RC_ABORT_SI_CONFLICT};
    return false;
  }
  if (caller_callback->return_code._val == RC_TRUE) {
#if defined(COPYDDL) && !defined(LAZYDDL)
    if (t->IsWaitForNewSchema()) {
      ermia::transaction::table_record_t *table_record = t->find_in_table_set(table_descriptor->GetTupleFid());
      if (table_record && !table_record->schema_ready && AWAIT t->OverlapCheck(table_descriptor, table_record->old_table_desc, oid)) {
        caller_callback->return_code = rc_t{RC_ABORT_SI_CONFLICT};
        return false;
      }
    }
#endif
    if (schema && schema->ddl_type == ddl::ddl_type::NO_COPY_VERIFICATION &&
        v->GetCSN() < schema->csn) {
      auto *key_array = table_descriptor->GetKeyArray();
      fat_ptr *entry = config::enable_ddl_keys ? key_array->get(oid) : nullptr;
      varstr *key = entry ? (varstr *)((*entry).offset()) : nullptr;
      varstr *new_tuple_value = &vv;
      for (int i = 0; i < schema->reformats_total; i++) {
        new_tuple_value = ddl::reformats[schema->reformats[i]](
            key, *new_tuple_value, &(t->string_allocator()), schema->v,
            table_descriptor->GetTupleFid(), oid);
      }
      if (!new_tuple_value) {
        return false;
      }
      vv.p = new_tuple_value->p;
      vv.l = new_tuple_value->l;
    }
    return caller_callback->Invoke(k, vv);
  } else if (caller_callback->return_code.IsAbort()) {
#ifdef OPTLAZYDDL
    if (schema && oid != INVALID_OID) {
      int total = schema->v;
      if (!total) {
        return false;
      }
      TableDescriptor *old_table_descriptor = nullptr;
    retry:
      old_table_descriptor = schema->old_tds[total - 1];
      if (old_table_descriptor == nullptr) {
        if (--total > 0) {
          goto retry;
        }
        return false;
      }
      old_table_descriptor->GetTupleArray()->ensure_size(oid);
      dbtuple *tuple = AWAIT oidmgr->oid_get_version(
          old_table_descriptor->GetTupleArray(), oid, t->xc);

      if (!tuple) {
        if (--total > 0) {
          goto retry;
        }
        return false;
      }

      if (t->DoTupleRead(tuple, &vv)._val == RC_TRUE) {
        auto *key_array = old_table_descriptor->GetKeyArray();
        fat_ptr *entry =
            config::enable_ddl_keys ? key_array->get(oid) : nullptr;
        varstr *key = entry ? (varstr *)((*entry).offset()) : nullptr;
        varstr *new_tuple_value = ddl::reformats[schema->reformat_idx](
            key, vv, &(t->string_allocator()), schema->v,
            old_table_descriptor->GetTupleFid(), oid);
        rc_t rc = AWAIT t->DDLInsert(schema->td, oid, new_tuple_value, 0);
        if (rc._val != RC_TRUE) {
          return false;
        }
        tuple = AWAIT oidmgr->oid_get_version(schema->td->GetTupleArray(), oid,
                                              t->xc);
        if (table_descriptor != schema->td) {
          caller_callback->return_code = rc_t{RC_ABORT_SI_CONFLICT};
          return false;
        }
        if (tuple && t->DoTupleRead(tuple, &vv)._val == RC_TRUE) {
          caller_callback->return_code = rc_t{RC_TRUE};
          return caller_callback->Invoke(k, vv);
        }
      }
    }
#endif
    // don't continue the read if the tx should abort
    // ^^^^^ note: see masstree_scan.hh, whose scan() calls
    // visit_value(), which calls this function to determine
    // if it should stop reading.
    return false;  // don't continue the read if the tx should abort
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
  auto *tuple = sync_wait_coro(
      oidmgr->oid_get_version(td->GetTupleArray(), oid, t.GetXIDContext()));
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

PROMISE(rc_t) Table::Update(transaction &t, OID oid, varstr &value) {
  rc_t rc = AWAIT t.Update(td, oid, &value);
  RETURN rc;
}

PROMISE(rc_t) Table::Remove(transaction &t, OID oid) {
  rc_t rc = AWAIT t.Update(td, oid, nullptr);
  RETURN rc;
}

////////////////// End of Table interfaces //////////

OrderedIndex::OrderedIndex(std::string table_name, bool is_primary)
    : is_primary(is_primary) {
  table_descriptor = Catalog::GetTable(table_name);
  self_fid = oidmgr->create_file(true);
}

OrderedIndex::OrderedIndex(std::string table_name, bool is_primary,
                           FID self_fid)
    : is_primary(is_primary), self_fid(self_fid) {
  table_descriptor = Catalog::GetTable(table_name);
}

}  // namespace ermia
