#include "txn.h"

#include "dbcore/rcu.h"
#include "dbcore/serial.h"
#include "engine.h"
#include "macros.h"

extern thread_local ermia::epoch_num coroutine_batch_end_epoch;

namespace ermia {

#ifdef BLOCKDDL
std::unordered_map<FID, pthread_rwlock_t *> transaction::lock_map;
std::mutex transaction::map_rw_latch;
#endif

transaction::transaction(uint64_t flags, str_arena &sa, uint32_t coro_batch_idx)
    : flags(flags),
      log(nullptr),
      log_size(0),
      sa(&sa),
      coro_batch_idx(coro_batch_idx) {
  if (config::phantom_prot) {
    masstree_absent_set.set_empty_key(NULL);  // google dense map
    masstree_absent_set.clear();
  }
  wait_for_new_schema = false;
  if (is_ddl()) {
    ddl::ddl_running = true;
    ddl::ddl_td_set = false;
#if defined(SIDDL)
    write_set.init_large_write_set();
#endif
  }
  write_set.clear();
#if defined(SSN) || defined(SSI) || defined(MVOCC)
  read_set.clear();
#endif
  xid = TXN::xid_alloc();
  xc = TXN::xid_get_context(xid);
  xc->xct = this;

  if (!(flags & TXN_FLAG_CSWITCH)) {
    // "Normal" transactions
    xc->begin_epoch = config::tls_alloc ? MM::epoch_enter() : 0;
  }
#if defined(SSN) || defined(SSI)
  // If there's a safesnap, then SSN treats the snapshot as a transaction
  // that has read all the versions, which means every update transaction
  // should have a initial pstamp of the safesnap.
  //
  // Readers under SSI using safesnap are free from SSI checks, but writers
  // will have to see whether they have a ct3 that's before the safesnap lsn
  // (ie the safesnap is T1, updater is T2). So for SSI updaters also needs
  // to take a look at the safesnap lsn.

  // Take a safe snapshot if read-only.
  if (config::enable_safesnap && (flags & TXN_FLAG_READ_ONLY)) {
    ASSERT(MM::safesnap_lsn);
    xc->begin = volatile_read(MM::safesnap_lsn);
  } else {
    TXN::serial_register_tx(coro_batch_idx, xid);
    log = logmgr->new_tx_log(
        (char *)string_allocator().next(sizeof(sm_tx_log))->data());
    // Must +1: a tx T can only update a tuple if its latest version was
    // created before T's begin timestamp (i.e., version.clsn < T.begin,
    // note the range is exclusive; see first updater wins rule in
    // oid_put_update() in sm-oid.cpp). Otherwise we risk making no
    // progress when retrying an aborted transaction: everyone is trying
    // to update the same tuple with latest version stamped at cur_lsn()
    // but no one can succeed (because version.clsn == cur_lsn == t.begin).
    xc->begin = logmgr->cur_lsn().offset() + 1;
#ifdef SSN
    xc->pstamp = volatile_read(MM::safesnap_lsn);
#elif defined(SSI)
    xc->last_safesnap = volatile_read(MM::safesnap_lsn);
#endif
  }
#elif defined(MVOCC)
  log = logmgr->new_tx_log(
      (char *)string_allocator().next(sizeof(sm_tx_log))->data());
  xc->begin = logmgr->cur_lsn().offset() + 1;
#else
  // Give a log regardless - with pipelined commit, read-only tx needs
  // to go through the queue as well
  if (ermia::config::pcommit) {
    log = GetLog();
    if (is_ddl()) {
      log->set_doing_ddl(true);
#if defined(COPYDDL)
      for (uint32_t i = 0; i < ermia::dlog::tlogs.size(); i++) {
        dlog::tls_log *tlog = dlog::tlogs[i];
        if (tlog && tlog != log &&
            volatile_read(pcommit::_tls_durable_csn[i])) {
          ddl::_tls_durable_lsn[i] = tlog->get_durable_lsn();
        }
      }
#endif
    }
  }

  xc->begin = dlog::current_csn.load(std::memory_order_relaxed);
  if (config::enable_dml_slow_down && ddl::ddl_running && is_dml()) {
    util::fast_random r(xc->begin);
    if (r.next_uniform() >= config::dml_slow_down_prob) {
      usleep(1);
    }
  }
#endif
}

void transaction::uninitialize() {
  // transaction shouldn't fall out of scope w/o resolution
  // resolution means TXN_CMMTD, and TXN_ABRTD
  ASSERT(state() != TXN::TXN_ACTIVE && state() != TXN::TXN_COMMITTING);
#if defined(SSN) || defined(SSI)
  if (!config::enable_safesnap || (!(flags & TXN_FLAG_READ_ONLY))) {
    TXN::serial_deregister_tx(coro_batch_idx, xid);
  }
#endif
  if (config::tls_alloc) {
    if (flags & TXN_FLAG_CSWITCH) {
      if (xc->end > coroutine_batch_end_epoch) {
        coroutine_batch_end_epoch = xc->end;
      }
    } else if (config::enable_safesnap && (flags & TXN_FLAG_READ_ONLY)) {
      MM::epoch_exit(0, xc->begin_epoch);
    } else {
      MM::epoch_exit(xc->end, xc->begin_epoch);
    }
  }
  TXN::xid_free(xid);  // must do this after epoch_exit, which uses xc.end
}

void transaction::Abort() {
  // Mark the dirty tuple as invalid, for oid_get_version to
  // move on more quickly.
  volatile_write(xc->state, TXN::TXN_ABRTD);

#if defined(SSN) || defined(SSI)
  // Go over the read set first, to deregister from the tuple
  // asap so the updater won't wait for too long.
  for (uint32_t i = 0; i < read_set.size(); ++i) {
    auto &r = read_set[i];
    ASSERT(r->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_LOG);
    // remove myself from reader list
    serial_deregister_reader_tx(coro_batch_idx, &r->readers_bitmap);
  }
#endif

  for (uint32_t i = 0; i < write_set.size(); ++i) {
    write_record_t &w = write_set.get(is_ddl(), i);
    dbtuple *tuple = (dbtuple *)w.get_object()->GetPayload();
    ASSERT(tuple);
#if defined(SSI) || defined(SSN) || defined(MVOCC)
    ASSERT(XID::from_ptr(tuple->GetObject()->GetClsn()) == xid);
    if (tuple->NextVolatile()) {
      volatile_write(tuple->NextVolatile()->sstamp, NULL_PTR);
#ifdef SSN
      tuple->NextVolatile()->welcome_read_mostly_tx();
#endif
    }
#endif

    Object *obj = w.get_object();
    fat_ptr entry = *w.entry;
    obj->SetCSN(NULL_PTR);
    oidmgr->UnlinkTuple(w.entry);
    ASSERT(obj->GetAllocateEpoch() == xc->begin_epoch);
    MM::deallocate(entry);
  }

#ifdef BLOCKDDL
  UnlockAll();
#endif
}

rc_t transaction::commit() {
  ALWAYS_ASSERT(state() == TXN::TXN_ACTIVE);
  volatile_write(xc->state, TXN::TXN_COMMITTING);
  rc_t ret;
#if defined(SSN) || defined(SSI)
  // Safe snapshot optimization for read-only transactions:
  // Use the begin ts as cstamp if it's a read-only transaction
  // This is the same for both SSN and SSI.
  if (config::enable_safesnap && (flags & TXN_FLAG_READ_ONLY)) {
    ASSERT(!log);
    ASSERT(write_set.size() == 0);
    xc->end = xc->begin;
    volatile_write(xc->state, TXN::TXN_CMMTD);
    ret = {RC_TRUE};
  } else {
    ASSERT(log);
    xc->end = log->pre_commit().offset();
    if (xc->end == 0) {
      ret = rc_t{RC_ABORT_INTERNAL};
    }
#ifdef SSN
    ret = parallel_ssn_commit();
#elif defined SSI
    ret = parallel_ssi_commit();
#endif
  }
#elif defined(MVOCC)
  ret = mvocc_commit();
#else
  ret = si_commit();
#endif

  // Enqueue to pipelined commit queue, if enabled
  if (ret._val == RC_TRUE) {
    // Keep end CSN before xc is recycled by uninitialize()
    auto end = xc->end;
    uninitialize();
    if (log && ermia::config::pcommit) {
      end = !end || write_set.size() ? end : end - 1;
      log->enqueue_committed_xct(end);
    }
  }

#ifdef BLOCKDDL
  UnlockAll();
#endif

  return ret;
}

#if !defined(SSI) && !defined(SSN) && !defined(MVOCC)
rc_t transaction::si_commit() {
  if (!log && ((flags & TXN_FLAG_READ_ONLY) || write_set.size() == 0)) {
    volatile_write(xc->state, TXN::TXN_CMMTD);
    return rc_t{RC_TRUE};
  }

  if (config::phantom_prot && !MasstreeCheckPhantom()) {
    return rc_t{RC_ABORT_PHANTOM};
  }

  ASSERT(log);
  // Precommit: obtain a CSN
  xc->end = write_set.size() ? dlog::current_csn.fetch_add(1) : xc->begin;

#if defined(COPYDDL)
  if (is_dml() && DMLConsistencyHandler()) {
    DLOG(INFO) << "DML failed with begin: " << xc->begin
               << ", end: " << xc->end;
    return rc_t{RC_ABORT_SI_CONFLICT};
  }
#endif

  dlog::log_block *lb = nullptr;
  dlog::tlog_lsn lb_lsn = dlog::INVALID_TLOG_LSN;
  uint64_t segnum = -1;
  uint64_t logbuf_size = log->get_logbuf_size() - sizeof(dlog::log_block);
  // Generate a log block if not read-only
  if (write_set.size()) {
    if (log_size > logbuf_size) {
      lb = log->allocate_log_block(logbuf_size, &lb_lsn, &segnum, xc->end);
    } else {
      lb = log->allocate_log_block(log_size, &lb_lsn, &segnum, xc->end);
    }
  }

  if (is_ddl()) {
    DLOG(INFO) << "DDL txn end: " << xc->end;
    // If txn is DDL, commit schema record(s) first before CDC
    for (uint32_t i = 0; i < write_set.size(); ++i) {
      write_record_t &w = write_set.get(is_ddl(), i);
      Object *object = w.get_object();
      dbtuple *tuple = (dbtuple *)object->GetPayload();

      // Populate log block and obtain persistent address
      uint32_t off = lb->payload_size;
      if (w.type == dlog::log_record::logrec_type::INSERT) {
        auto ret_off =
            dlog::log_insert(lb, w.fid, w.oid, (char *)tuple, w.size);
        ALWAYS_ASSERT(ret_off == off);
      } else if (w.type == dlog::log_record::logrec_type::UPDATE) {
        auto ret_off =
            dlog::log_update(lb, w.fid, w.oid, (char *)tuple, w.size);
        ALWAYS_ASSERT(ret_off == off);
      }
      ALWAYS_ASSERT(lb->payload_size <= lb->capacity);

      // This aligned_size should match what was calculated during
      // add_to_write_set, and the size_code calculated based on this aligned
      // size will be part of the persistent address, which a read can directly
      // use to load the log record from the log (i.e., knowing how many bytes
      // to read to obtain the log record header + dbtuple header + record
      // data).
      auto aligned_size = align_up(w.size + sizeof(dlog::log_record));
      auto size_code = encode_size_aligned(aligned_size);

      // lb_lsn points to the start of the log block which has a header,
      // followed by individual log records, so the log record's direct address
      // would be lb_lsn + sizeof(log_block) + off
      fat_ptr pdest =
          LSN::make(log->get_id(), lb_lsn + sizeof(dlog::log_block) + off,
                    segnum, size_code)
              .to_ptr();
      object->SetPersistentAddress(pdest);
      ASSERT(object->GetPersistentAddress().asi_type() == fat_ptr::ASI_LOG);

      // Set CSN
#ifdef BLOCKDDL
      fat_ptr csn_ptr = object->GenerateCsnPtr(xc->begin);
#else
      fat_ptr csn_ptr = object->GenerateCsnPtr(xc->end);
#endif
      object->SetCSN(csn_ptr);
      ASSERT(tuple->GetObject()->GetCSN().asi_type() == fat_ptr::ASI_CSN);
    }
    DLOG(INFO) << "DDL schema commit with size " << write_set.size();

#ifdef COPYDDL
    if (ddl_exe->get_ddl_type() != ddl::ddl_type::NO_COPY_VERIFICATION) {
      for (auto &v : new_td_map) {
        // Fix new table file's marks
        auto *alloc = oidmgr->get_allocator(this->old_td->GetTupleFid());
        uint32_t himark = alloc->head.hiwater_mark;
        auto *new_tuple_array = v.second->GetTupleArray();
        new_tuple_array->ensure_size(himark - 64);
        oidmgr->recreate_allocator(v.second->GetTupleFid(), himark - 64);
        auto *new_alloc = oidmgr->get_allocator(v.second->GetTupleFid());
        himark = new_alloc->head.hiwater_mark;

        // Switch table descriptor for primary index
        OrderedIndex *index = v.second->GetPrimaryIndex();
        index->SetTableDescriptor(v.second);
        index->SetArrays(true);

        // Switch table descriptor for secondary index(es)
        std::vector<OrderedIndex *> sec_indexes = v.second->GetSecIndexes();
        for (std::vector<OrderedIndex *>::const_iterator it =
                 sec_indexes.begin();
             it != sec_indexes.end(); ++it) {
          (*it)->SetTableDescriptor(v.second);
          (*it)->SetArrays(false);
        }
      }
      ddl::ddl_td_set = true;
    }
#endif
  }

  if (is_ddl()) {
#if defined(COPYDDL) && !defined(LAZYDDL)
    if (ddl_exe->get_ddl_type() != ddl::ddl_type::NO_COPY_VERIFICATION) {
      // Start the second round of CDC
      DLOG(INFO) << "Second CDC begins";
      ddl::cdc_second_phase = true;
      ddl::cdc_end_total.store(0);
      uint32_t cdc_threads = ddl_exe->changed_data_capture(this);
      while (ddl::cdc_end_total.load() != cdc_threads && !ddl::ddl_failed) {
      }
      ddl::cdc_running = false;
      ddl_exe->join_cdc_workers();
      ddl::cdc_second_phase = false;
      DLOG(INFO) << "Second CDC ends";
      if (config::enable_late_scan_join) {
        ddl_exe->join_scan_workers();
      }
      if (ddl::ddl_failed) {
        DLOG(INFO) << "DDL failed";
        for (auto &v : new_td_map) {
          OrderedIndex *index = v.second->GetPrimaryIndex();
          index->SetTableDescriptor(this->old_td);
          index->SetArrays(true);
          v.second->GetTupleArray()->destroy(v.second->GetTupleArray());
        }
        return rc_t{RC_ABORT_INTERNAL};
      }
    }
#endif

    for (uint32_t i = 0; i < write_set.size(); ++i) {
      write_record_t w = write_set.get(is_ddl(), i);
      Object *object = w.get_object();
      dbtuple *tuple = (dbtuple *)object->GetPayload();

      varstr value(tuple->get_value_start(), tuple->size);
      schema_kv::value schema_value_temp;
      const schema_kv::value *schema_not_ready =
          Decode(value, schema_value_temp);
      schema_kv::value schema_ready(*schema_not_ready);
      schema_ready.state = ddl::schema_state_type::READY;
      schema_ready.csn = xc->end;

      string_allocator().reset();
      varstr *new_value = string_allocator().next(Size(schema_ready));

      ALWAYS_ASSERT(DDLSchemaUnblock(schema_td, w.oid,
                                     &Encode(*new_value, schema_ready), xc->end)
                        ._val == RC_TRUE);
    }
  }

  if (is_ddl()) {
    ddl::ddl_running = false;
    volatile_write(xc->state, TXN::TXN_CMMTD);
#ifdef LAZYDDL
    // Background migration
    if (!config::enable_lazy_background) {
      log->set_dirty(false);
      log->set_doing_ddl(false);
      return rc_t{RC_TRUE};
    }
    ddl_exe->scan(this, &(string_allocator()));
#endif

    for (auto &v : new_td_map) {
      // FID fid = v.second->GetTupleFid();
      FID fid = old_td->GetTupleFid();
      auto *new_alloc = oidmgr->get_allocator(fid);
      uint32_t himark = new_alloc->head.hiwater_mark;
      auto *new_tuple_array = v.second->GetTupleArray();

      uint32_t ddl_log_threads = config::scan_threads + config::cdc_threads - 1;
      uint32_t num_per_scan_thread = himark / ddl_log_threads;

      std::vector<thread::Thread *> ddl_log_workers;

      for (uint32_t i = 0; i < ddl_log_threads; i++) {
        uint32_t begin = i * num_per_scan_thread;
        uint32_t end = (i + 1) * num_per_scan_thread;
        if (i == ddl_log_threads - 1) end = himark;

        thread::Thread *thread =
            thread::GetThread(config::scan_physical_workers_only);
        ALWAYS_ASSERT(thread);
        ddl_log_workers.push_back(thread);
        auto ddl_log = [=](char *) {
          dlog::tls_log *log = GetLog();
          log->set_normal(false);
          log->resize_logbuf(50);
          dlog::log_block *lb = nullptr;
          dlog::tlog_lsn lb_lsn = dlog::INVALID_TLOG_LSN;
          uint64_t segnum = -1;
          uint64_t logbuf_size =
              log->get_logbuf_size() - sizeof(dlog::log_block);

          lb = log->allocate_log_block(logbuf_size, &lb_lsn, &segnum, xc->end);
          for (uint32_t oid = 0; oid <= himark; ++oid) {
            if (oid % ddl_log_threads != i) continue;
            // for (uint32_t oid = begin; oid <= end; ++oid) {
            fat_ptr *entry = new_tuple_array->get(oid);
            fat_ptr ptr = volatile_read(*entry);
            while (ptr.offset()) {
              fat_ptr tentative_next = NULL_PTR;
              Object *cur_obj = (Object *)ptr.offset();
              tentative_next = cur_obj->GetNextVolatile();
              fat_ptr csn = cur_obj->GetCSN();
              if (csn.asi_type() == fat_ptr::ASI_CSN &&
                  CSN::from_ptr(csn).offset() > xc->end) {
                break;
              } else if (csn.asi_type() == fat_ptr::ASI_CSN &&
#ifdef LAZYDDL
                         CSN::from_ptr(csn).offset() <= xc->end &&
#else
                         CSN::from_ptr(csn).offset() < xc->end &&
#endif
                         CSN::from_ptr(csn).offset() >= xc->begin) {
                dbtuple *tuple = (dbtuple *)cur_obj->GetPayload();
                uint64_t log_tuple_size = sizeof(dbtuple) + tuple->size;
                uint32_t off = lb->payload_size;
                if (off + align_up(log_tuple_size + sizeof(dlog::log_record)) >
                    lb->capacity) {
                  lb = log->allocate_log_block(logbuf_size, &lb_lsn, &segnum,
                                               xc->end);
                  off = lb->payload_size;
                }
                auto ret_off = dlog::log_insert(lb, fid, oid, (char *)tuple,
                                                log_tuple_size);
                ALWAYS_ASSERT(ret_off == off);

                auto aligned_size =
                    align_up(log_tuple_size + sizeof(dlog::log_record));
                auto size_code = encode_size_aligned(aligned_size);

                fat_ptr pdest =
                    LSN::make(log->get_id(),
                              lb_lsn + sizeof(dlog::log_block) + off, segnum,
                              size_code)
                        .to_ptr();
                cur_obj->SetPersistentAddress(pdest);
                ASSERT(cur_obj->GetPersistentAddress().asi_type() ==
                       fat_ptr::ASI_LOG);

                break;
              }
              ptr = tentative_next;
            }
          }
        };
        thread->StartTask(ddl_log);
      }

      for (std::vector<thread::Thread *>::const_iterator it =
               ddl_log_workers.begin();
           it != ddl_log_workers.end(); ++it) {
        (*it)->Join();
        thread::PutThread(*it);
      }
    }
    log->set_dirty(false);
    log->set_doing_ddl(false);
    return rc_t{RC_TRUE};
  }

  // Normally, we'd generate each version's persitent address along the way or
  // here first before toggling the CSN's "committed" bit. But we can actually
  // do it first, and generate the log block as we scan the write set once,
  // leveraging pipelined commit!

  // Post-commit: install CSN to tuples (traverse write-tuple), generate log
  // records, etc.
  uint32_t current_log_size = 0;
  for (uint32_t i = 0; i < write_set.size(); ++i) {
    auto &w = write_set.get(is_ddl(), i);
    Object *object = w.get_object();
    dbtuple *tuple = (dbtuple *)object->GetPayload();

    uint64_t log_tuple_size = w.size;

    // Populate log block and obtain persistent address
    uint32_t off = lb->payload_size;
    if (lb->payload_size + align_up(log_tuple_size + sizeof(dlog::log_record)) >
        lb->capacity) {
      if (log_size - current_log_size > logbuf_size) {
        lb = log->allocate_log_block(logbuf_size, &lb_lsn, &segnum, xc->end);
      } else {
        lb = log->allocate_log_block(log_size - current_log_size, &lb_lsn,
                                     &segnum, xc->end);
      }
      off = lb->payload_size;
    }
    if (w.type == dlog::log_record::logrec_type::INSERT) {
      auto ret_off =
          dlog::log_insert(lb, w.fid, w.oid, (char *)tuple, log_tuple_size);
      ALWAYS_ASSERT(ret_off == off);
    } else if (w.type == dlog::log_record::logrec_type::UPDATE) {
      auto ret_off =
          dlog::log_update(lb, w.fid, w.oid, (char *)tuple, log_tuple_size);
      ALWAYS_ASSERT(ret_off == off);
    }
    ALWAYS_ASSERT(lb->payload_size <= lb->capacity);
    current_log_size += align_up(log_tuple_size + sizeof(dlog::log_record));

    // This aligned_size should match what was calculated during
    // add_to_write_set, and the size_code calculated based on this aligned size
    // will be part of the persistent address, which a read can directly use to
    // load the log record from the log (i.e., knowing how many bytes to read to
    // obtain the log record header + dbtuple header + record data).
    auto aligned_size = align_up(w.size + sizeof(dlog::log_record));
    auto size_code = encode_size_aligned(aligned_size);

    // lb_lsn points to the start of the log block which has a header, followed
    // by individual log records, so the log record's direct address would be
    // lb_lsn + sizeof(log_block) + off
    fat_ptr pdest =
        LSN::make(log->get_id(), lb_lsn + sizeof(dlog::log_block) + off, segnum,
                  size_code)
            .to_ptr();
    object->SetPersistentAddress(pdest);
    ASSERT(object->GetPersistentAddress().asi_type() == fat_ptr::ASI_LOG);

    // Set CSN
    fat_ptr csn_ptr = object->GenerateCsnPtr(xc->end);
    object->SetCSN(csn_ptr);
    ASSERT(tuple->GetObject()->GetCSN().asi_type() == fat_ptr::ASI_CSN);
  }
  // ALWAYS_ASSERT(!lb || lb->payload_size == lb->capacity);
  log->set_dirty(false);

  // NOTE: make sure this happens after populating log block,
  // otherwise readers will see inconsistent data!
  // This is when (committed) tuple data are made visible to readers
  volatile_write(xc->state, TXN::TXN_CMMTD);
  return rc_t{RC_TRUE};
}
#endif

bool transaction::DMLConsistencyHandler() {
  TXN::xid_context *tmp_xc = TXN::xid_get_context(xid);
  uint64_t begin = xc->begin;
  tmp_xc->begin = xc->end;

  for (auto &v : schema_read_map) {
    dbtuple *tuple =
        oidmgr->oid_get_version(schema_td->GetTupleArray(), v.first, tmp_xc);
    if (!tuple) {
      tmp_xc->begin = begin;
      return true;
    } else {
      varstr tuple_v;
      if (DoTupleRead(tuple, &tuple_v)._val != RC_TRUE) {
        tmp_xc->begin = begin;
        return true;
      }
      schema_kv::value schema_value_temp;
      const schema_kv::value *schema = Decode(tuple_v, schema_value_temp);
      if (schema->version != v.second) {
        tmp_xc->begin = begin;
        return true;
      }
    }
  }

  tmp_xc->begin = begin;
  return false;
}

// returns true if btree versions have changed, ie there's phantom
bool transaction::MasstreeCheckPhantom() {
  for (auto &r : masstree_absent_set) {
    const uint64_t v = ConcurrentMasstree::ExtractVersionNumber(r.first);
    if (unlikely(v != r.second)) return false;
  }
  return true;
}

PROMISE(rc_t)
transaction::Update(TableDescriptor *td, OID oid, const varstr *k, varstr *v) {
  oid_array *tuple_array = td->GetTupleArray();
  FID tuple_fid = td->GetTupleFid();

  // first *updater* wins
  fat_ptr new_obj_ptr = NULL_PTR;
  fat_ptr prev_obj_ptr =
      oidmgr->UpdateTuple(tuple_array, oid, v, xc, &new_obj_ptr);
  Object *prev_obj = (Object *)prev_obj_ptr.offset();

  if (prev_obj) {  // succeeded
    dbtuple *tuple = AWAIT((Object *)new_obj_ptr.offset())->GetPinnedTuple();
    ASSERT(tuple);
    dbtuple *prev = AWAIT prev_obj->GetPinnedTuple();
    ASSERT((uint64_t)prev->GetObject() == prev_obj_ptr.offset());
    ASSERT(xc);
#ifdef SSI
    ASSERT(prev->sstamp == NULL_PTR);
    if (xc->ct3) {
      // Check if we are the T2 with a committed T3 earlier than a safesnap
      // (being T1)
      if (xc->ct3 <= xc->last_safesnap) RETURN{RC_ABORT_SERIAL};

      if (volatile_read(prev->xstamp) >= xc->ct3 or
          not prev->readers_bitmap.is_empty(coro_batch_idx, true)) {
        // Read-only optimization: safe if T1 is read-only (so far) and T1's
        // begin ts
        // is before ct3.
        if (config::enable_ssi_read_only_opt) {
          TXN::readers_bitmap_iterator readers_iter(&prev->readers_bitmap);
          while (true) {
            int32_t xid_idx = readers_iter.next(xc, true);
            if (xid_idx == -1) break;

            XID rxid = volatile_read(TXN::rlist.xids[xid_idx]);
            ASSERT(rxid != xc->owner);
            if (rxid == INVALID_XID)  // reader is gone, check xstamp in the end
              continue;

            XID reader_owner = INVALID_XID;
            uint64_t reader_begin = 0;
            TXN::xid_context *reader_xc = NULL;
            reader_xc = TXN::xid_get_context(rxid);
            if (not reader_xc)  // context change, consult xstamp later
              continue;

            // copy everything before doing anything
            reader_begin = volatile_read(reader_xc->begin);
            reader_owner = volatile_read(reader_xc->owner);
            if (reader_owner != rxid)  // consult xstamp later
              continue;

            // we're safe if the reader is read-only (so far) and started after
            // ct3
            if (reader_xc->xct->write_set.size() > 0 and
                reader_begin <= xc->ct3) {
              oidmgr->UpdateTuple(tuple_array, oid);
              RETURN{RC_ABORT_SERIAL};
            }
          }
        } else {
          oidmgr->UnlinkTuple(tuple_array, oid);
          RETURN{RC_ABORT_SERIAL};
        }
      }
    }
#endif
#ifdef SSN
    // update hi watermark
    // Overwriting a version could trigger outbound anti-dep,
    // i.e., I'll depend on some tx who has read the version that's
    // being overwritten by me. So I'll need to see the version's
    // access stamp to tell if the read happened.
    ASSERT(prev->sstamp == NULL_PTR);
    auto prev_xstamp = volatile_read(prev->xstamp);
    if (xc->pstamp < prev_xstamp) xc->pstamp = prev_xstamp;

#ifdef EARLY_SSN_CHECK
    if (not ssn_check_exclusion(xc)) {
      // unlink the version here (note abort_impl won't be able to catch
      // it because it's not yet in the write set)
      oidmgr->UnlinkTuple(tuple_array, oid);
      RETURN rc_t{RC_ABORT_SERIAL};
    }
#endif

    // copy access stamp to new tuple from overwritten version
    // (no need to copy sucessor lsn (slsn))
    volatile_write(tuple->xstamp, prev->xstamp);
#endif

    // read prev's CSN first, in case it's a committing XID, the CSN's state
    // might change to ASI_CSN anytime
    ASSERT((uint64_t)prev->GetObject() == prev_obj_ptr.offset());
    fat_ptr prev_csn = prev->GetObject()->GetCSN();
    fat_ptr prev_persistent_ptr = NULL_PTR;
    if (prev_csn.asi_type() == fat_ptr::ASI_XID and
        XID::from_ptr(prev_csn) == xid) {
      // updating my own updates!
      // prev's prev: previous *committed* version
      ASSERT(((Object *)prev_obj_ptr.offset())->GetAllocateEpoch() ==
             xc->begin_epoch);
      prev_persistent_ptr = prev_obj->GetNextPersistent();
      // FIXME(tzwang): 20190210: seems the deallocation here is too early,
      // causing readers to not find any visible version. Fix this together with
      // GC later.
      // MM::deallocate(prev_obj_ptr);
    } else {  // prev is committed (or precommitted but in post-commit now) head
#if defined(SSI) || defined(SSN) || defined(MVOCC)
      volatile_write(prev->sstamp, xc->owner.to_ptr());
      ASSERT(prev->sstamp.asi_type() == fat_ptr::ASI_XID);
      ASSERT(XID::from_ptr(prev->sstamp) == xc->owner);
      ASSERT(tuple->NextVolatile() == prev);
#endif

#if defined(SIDDL)
      add_to_write_set(true, tuple_array->get(oid), tuple_fid, oid, tuple->size,
                       dlog::log_record::logrec_type::UPDATE);
#else
      add_to_write_set(tuple_fid == schema_td->GetTupleFid(),
                       tuple_array->get(oid), tuple_fid, oid, tuple->size,
                       dlog::log_record::logrec_type::UPDATE);
#endif
      prev_persistent_ptr = prev_obj->GetPersistentAddress();
    }

    ASSERT(tuple->GetObject()->GetCSN().asi_type() == fat_ptr::ASI_XID);
    ASSERT(sync_wait_coro(oidmgr->oid_get_version(tuple_fid, oid, xc)) ==
           tuple);
    ASSERT(log);

    // FIXME(tzwang): mark deleted in all 2nd indexes as well?
    RETURN rc_t{RC_TRUE};
  } else {  // somebody else acted faster than we did
    RETURN rc_t{RC_ABORT_SI_CONFLICT};
  }
}

OID transaction::Insert(TableDescriptor *td, varstr *value,
                        dbtuple **out_tuple) {
  auto *tuple_array = td->GetTupleArray();
  FID tuple_fid = td->GetTupleFid();

  fat_ptr new_head = Object::Create(value, xc->begin_epoch);
  ASSERT(new_head.size_code() != INVALID_SIZE_CODE);
  ASSERT(new_head.asi_type() == 0);
  auto *tuple = (dbtuple *)((Object *)new_head.offset())->GetPayload();
  ASSERT(decode_size_aligned(new_head.size_code()) >= tuple->size);
  tuple->GetObject()->SetCSN(xid.to_ptr());
  OID oid = oidmgr->alloc_oid(tuple_fid);
  ALWAYS_ASSERT(oid != INVALID_OID);
  tuple_array->ensure_size(oid);
  oidmgr->oid_put_new(tuple_array, oid, new_head);

  ASSERT(tuple->size == value->size());
#if defined(SIDDL)
  add_to_write_set(true, tuple_array->get(oid), tuple_fid, oid, tuple->size,
                   dlog::log_record::logrec_type::INSERT);
#else
  add_to_write_set(tuple_fid == schema_td->GetTupleFid(), tuple_array->get(oid),
                   tuple_fid, oid, tuple->size,
                   dlog::log_record::logrec_type::INSERT);
#endif

  if (out_tuple) {
    *out_tuple = tuple;
  }
  return oid;
}

OID transaction::DDLInsert(TableDescriptor *td, varstr *value,
                           fat_ptr **out_entry) {
  auto *tuple_array = td->GetTupleArray();
  FID tuple_fid = td->GetTupleFid();

  fat_ptr new_head = Object::Create(value, xc->begin_epoch);
  ASSERT(new_head.size_code() != INVALID_SIZE_CODE);
  ASSERT(new_head.asi_type() == 0);
  Object *new_object = (Object *)new_head.offset();
  auto *tuple = (dbtuple *)new_object->GetPayload();
  ASSERT(decode_size_aligned(new_head.size_code()) >= tuple->size);
  // tuple->GetObject()->SetCSN(xid.to_ptr());
  ALWAYS_ASSERT(xc->end != 0);
  tuple->GetObject()->SetCSN(new_object->GenerateCsnPtr(xc->end));
  OID oid = oidmgr->alloc_oid(tuple_fid);
  ALWAYS_ASSERT(oid != INVALID_OID);
  tuple_array->ensure_size(oid);

retry:
  fat_ptr *entry_ptr = tuple_array->get(oid);
  fat_ptr expected = *entry_ptr;
  Object *obj = (Object *)expected.offset();
  bool overwrite = true;
  if (expected == NULL_PTR) {
    overwrite = false;
    if (!__sync_bool_compare_and_swap(&entry_ptr->_ptr, expected._ptr,
                                      new_head._ptr)) {
      goto retry;
    }
  } else {
    MM::deallocate(new_head);
    return 0;
  }

  if (!overwrite) {
    ASSERT(tuple->size == value->size());
    add_to_write_set(tuple_fid == schema_td->GetTupleFid(),
                     tuple_array->get(oid), tuple_fid, oid, tuple->size,
                     dlog::log_record::logrec_type::INSERT);
  }

  if (out_entry) {
    *out_entry = tuple_array->get(oid);
  }
  return oid;
}

PROMISE(rc_t)
transaction::DDLCDCUpdate(TableDescriptor *td, OID oid, varstr *value,
                          uint64_t tuple_csn, dlog::log_block *block) {
  auto *tuple_array = td->GetTupleArray();
  FID tuple_fid = td->GetTupleFid();
  tuple_array->ensure_size(oid);

  fat_ptr new_head = Object::Create(value, xc->begin_epoch);
  ASSERT(new_head.size_code() != INVALID_SIZE_CODE);
  ASSERT(new_head.asi_type() == 0);
  Object *new_object = (Object *)new_head.offset();
  auto *new_tuple = (dbtuple *)new_object->GetPayload();
  if (tuple_csn)
    new_object->SetCSN(new_object->GenerateCsnPtr(tuple_csn));
  else
    new_object->SetCSN(xid.to_ptr());

retry:
  fat_ptr *entry_ptr = tuple_array->get(oid);
  fat_ptr expected = *entry_ptr;
  if (!tuple_csn) new_object->SetNextVolatile(expected);
  Object *obj = (Object *)expected.offset();
  fat_ptr csn = NULL_PTR;
  bool overwrite = true;
  if (expected == NULL_PTR) {
    overwrite = false;
  } else {
    csn = obj->GetCSN();
  }
  if (csn != NULL_PTR && csn.asi_type() == fat_ptr::ASI_XID) {
    auto holder_xid = XID::from_ptr(csn);
    TXN::xid_context *holder = TXN::xid_get_context(holder_xid);
    if (holder && volatile_read(holder->begin) >= tuple_csn) {
      MM::deallocate(new_head);
      RETURN rc_t{RC_ABORT_SI_CONFLICT};
    }
  }
  if (expected == NULL_PTR || (csn.asi_type() == fat_ptr::ASI_CSN &&
                               CSN::from_ptr(csn).offset() < tuple_csn)) {
    if (!__sync_bool_compare_and_swap(&entry_ptr->_ptr, expected._ptr,
                                      new_head._ptr)) {
      // goto retry;
    }
  } else {
    MM::deallocate(new_head);
    RETURN rc_t{RC_ABORT_SI_CONFLICT};
  }

  if (!overwrite) {
    ASSERT(new_tuple->size == value->size());
    add_to_write_set(tuple_fid == schema_td->GetTupleFid(),
                     tuple_array->get(oid), tuple_fid, oid, new_tuple->size,
                     dlog::log_record::logrec_type::INSERT);
  } else {
    MM::deallocate(new_head);
  }

  RETURN rc_t{RC_TRUE};
}

void transaction::DDLScanInsert(TableDescriptor *td, OID oid, varstr *value,
                                dlog::log_block *block) {
  auto *tuple_array = td->GetTupleArray();
  FID tuple_fid = td->GetTupleFid();
  tuple_array->ensure_size(oid);

  fat_ptr new_head = Object::Create(value, xc->begin_epoch);
  ASSERT(new_head.size_code() != INVALID_SIZE_CODE);
  ASSERT(new_head.asi_type() == 0);
  auto *new_tuple = (dbtuple *)((Object *)new_head.offset())->GetPayload();
  // new_tuple->GetObject()->SetCSN(xid.to_ptr());
  new_tuple->GetObject()->SetCSN(
      new_tuple->GetObject()->GenerateCsnPtr(xc->begin));
  oidmgr->oid_put_new(tuple_array, oid, new_head);

  ASSERT(new_tuple->size == value->size());
  add_to_write_set(tuple_fid == schema_td->GetTupleFid(), tuple_array->get(oid),
                   tuple_fid, oid, new_tuple->size,
                   dlog::log_record::logrec_type::INSERT);
}

void transaction::DDLScanUpdate(TableDescriptor *td, OID oid, varstr *value,
                                dlog::log_block *block) {
  auto *tuple_array = td->GetTupleArray();
  FID tuple_fid = td->GetTupleFid();
  tuple_array->ensure_size(oid);

  fat_ptr new_head = Object::Create(value, xc->begin_epoch);
  ASSERT(new_head.size_code() != INVALID_SIZE_CODE);
  ASSERT(new_head.asi_type() == 0);
  auto *new_tuple = (dbtuple *)((Object *)new_head.offset())->GetPayload();
  Object *new_object = (Object *)new_head.offset();
  new_object->SetCSN(new_object->GenerateCsnPtr(xc->begin));

  fat_ptr *entry_ptr = tuple_array->get(oid);
  fat_ptr expected = *entry_ptr;
  new_object->SetNextVolatile(expected);

  oidmgr->oid_put_new(tuple_array, oid, new_head);

  ASSERT(new_tuple->size == value->size());
  add_to_write_set(tuple_fid == schema_td->GetTupleFid(), tuple_array->get(oid),
                   tuple_fid, oid, new_tuple->size,
                   dlog::log_record::logrec_type::UPDATE);
}

PROMISE(rc_t)
transaction::DDLCDCInsert(TableDescriptor *td, OID oid, varstr *value,
                          uint64_t tuple_csn, dlog::log_block *lb) {
  auto *tuple_array = td->GetTupleArray();
  FID tuple_fid = td->GetTupleFid();
  tuple_array->ensure_size(oid);

  fat_ptr new_head = Object::Create(value, xc->begin_epoch);
  ASSERT(new_head.size_code() != INVALID_SIZE_CODE);
  ASSERT(new_head.asi_type() == 0);
  Object *new_object = (Object *)new_head.offset();
  auto *new_tuple = (dbtuple *)new_object->GetPayload();
  if (tuple_csn)
    new_object->SetCSN(new_object->GenerateCsnPtr(tuple_csn));
  else
    new_object->SetCSN(xid.to_ptr());

retry:
  fat_ptr *entry_ptr = tuple_array->get(oid);
  fat_ptr expected = *entry_ptr;
  Object *obj = (Object *)expected.offset();
  bool overwrite = true;
  if (expected == NULL_PTR) {
    overwrite = false;
    if (!__sync_bool_compare_and_swap(&entry_ptr->_ptr, expected._ptr,
                                      new_head._ptr)) {
      goto retry;
    }
  } else {
    MM::deallocate(new_head);
    RETURN rc_t{RC_ABORT_SI_CONFLICT};
  }

  if (!overwrite) {
    ASSERT(new_tuple->size == value->size());
    add_to_write_set(tuple_fid == schema_td->GetTupleFid(),
                     tuple_array->get(oid), tuple_fid, oid, new_tuple->size,
                     dlog::log_record::logrec_type::INSERT);
  }

  if (lb) {
    dlog::tlog_lsn lb_lsn = dlog::INVALID_TLOG_LSN;
    uint64_t segnum = -1;
    uint64_t logbuf_size = log->get_logbuf_size() - sizeof(dlog::log_block);
    expected = *entry_ptr;
    obj = (Object *)expected.offset();
    dbtuple *tuple = (dbtuple *)obj->GetPayload();
    uint64_t log_tuple_size = sizeof(dbtuple) + tuple->size;
    uint32_t off = lb->payload_size;
    if (off + align_up(log_tuple_size + sizeof(dlog::log_record)) >
        lb->capacity) {
      lb = log->allocate_log_block(logbuf_size, &lb_lsn, &segnum, xc->end);
      off = lb->payload_size;
    }
    auto ret_off =
        dlog::log_insert(lb, tuple_fid, oid, (char *)tuple, log_tuple_size);
  }

  RETURN rc_t{RC_TRUE};
}

PROMISE(rc_t)
transaction::DDLSchemaUnblock(TableDescriptor *td, OID oid, varstr *value,
                              uint64_t tuple_csn) {
  auto *tuple_array = td->GetTupleArray();
  FID tuple_fid = td->GetTupleFid();
  tuple_array->ensure_size(oid);

  fat_ptr new_head = Object::Create(value, xc->begin_epoch);
  ASSERT(new_head.size_code() != INVALID_SIZE_CODE);
  ASSERT(new_head.asi_type() == 0);
  Object *new_object = (Object *)new_head.offset();
  auto *new_tuple = (dbtuple *)new_object->GetPayload();
  new_object->SetCSN(new_object->GenerateCsnPtr(tuple_csn));

retry:
  fat_ptr *entry_ptr = tuple_array->get(oid);
  fat_ptr expected = *entry_ptr;
  new_object->SetNextVolatile(expected);
  Object *obj = (Object *)expected.offset();
  fat_ptr csn = NULL_PTR;
  bool overwrite = true;
  if (expected == NULL_PTR) {
    overwrite = false;
  } else {
    csn = obj->GetCSN();
  }
  if (expected == NULL_PTR || csn.asi_type() == fat_ptr::ASI_XID ||
      CSN::from_ptr(csn).offset() <= tuple_csn) {
    if (!__sync_bool_compare_and_swap(&entry_ptr->_ptr, expected._ptr,
                                      new_head._ptr)) {
      goto retry;
    }
  } else {
    MM::deallocate(new_head);
    RETURN rc_t{RC_ABORT_SI_CONFLICT};
  }

  if (!overwrite) {
    ASSERT(new_tuple->size == value->size());
    add_to_write_set(tuple_fid == schema_td->GetTupleFid(),
                     tuple_array->get(oid), tuple_fid, oid, new_tuple->size,
                     dlog::log_record::logrec_type::INSERT);
  }

  RETURN rc_t{RC_TRUE};
}

PROMISE(bool)
transaction::OverlapCheck(TableDescriptor *new_td, TableDescriptor *old_td,
                          OID oid, bool read_only) {
  if (!read_only) return false;
  auto *new_tuple_array = new_td->GetTupleArray();
  auto *old_tuple_array = old_td->GetTupleArray();
  new_tuple_array->ensure_size(oid);
  old_tuple_array->ensure_size(oid);

  fat_ptr *new_entry_ptr = new_tuple_array->get(oid);
  fat_ptr new_expected = *new_entry_ptr;
  Object *new_obj = (Object *)new_expected.offset();
  fat_ptr new_csn = new_obj->GetCSN();

  fat_ptr *old_entry_ptr = old_tuple_array->get(oid);
  fat_ptr old_expected = *old_entry_ptr;
  Object *old_obj = (Object *)old_expected.offset();
  fat_ptr old_csn = old_obj->GetCSN();

  if (new_expected == NULL_PTR && old_expected != NULL_PTR) {
    RETURN true;
  }
  if (new_expected != NULL_PTR && old_expected != NULL_PTR &&
      (old_csn.asi_type() == fat_ptr::ASI_XID ||
       new_csn.asi_type() == fat_ptr::ASI_XID ||
       (old_csn.asi_type() == fat_ptr::ASI_CSN &&
        new_csn.asi_type() == fat_ptr::ASI_CSN &&
        CSN::from_ptr(old_csn).offset() > CSN::from_ptr(new_csn).offset()))) {
    RETURN true;
  }

  RETURN false;
}

OID transaction::table_scan(TableDescriptor *td, const varstr *key, OID oid) {
  auto *key_array = td->GetKeyArray();
  if (oid == 0) {
    auto *alloc = oidmgr->get_allocator(td->GetTupleFid());
    oid = alloc->head.hiwater_mark;
  }
  for (OID o = 1; o <= oid; o++) {
    fat_ptr *entry = key_array->get(o);
    varstr *k = entry ? (varstr *)((*entry).offset()) : nullptr;
    if (k && key && key->size() == k->size() &&
        memcmp(k->data(), key->data(), key->size()) == 0) {
      return o;
    }
  }
  return 0;
}

void transaction::LogIndexInsert(OrderedIndex *index, OID oid,
                                 const varstr *key) {
  /*
  // Note: here we log the whole key varstr so that recovery can figure out the
  // real key length with key->size(), otherwise it'll have to use the decoded
  // (inaccurate) size (and so will build a different index).
  auto record_size = align_up(sizeof(varstr) + key->size());
  ASSERT((char *)key->data() == (char *)key + sizeof(varstr));
  auto size_code = encode_size_aligned(record_size);
  log->log_insert_index(index->GetIndexFid(), oid,
                        fat_ptr::make((void *)key, size_code),
                        DEFAULT_ALIGNMENT_BITS, NULL);
  */
  if (!index->IsPrimary()) {
  }
}

rc_t transaction::DoTupleRead(dbtuple *tuple, varstr *out_v) {
  ASSERT(tuple);
  ASSERT(xc);
  bool read_my_own =
      (tuple->GetObject()->GetCSN().asi_type() == fat_ptr::ASI_XID);
  ASSERT(!read_my_own ||
         (read_my_own &&
          XID::from_ptr(tuple->GetObject()->GetCSN()) == xc->owner));
  ASSERT(!read_my_own || !(flags & TXN_FLAG_READ_ONLY));

#if defined(SSI) || defined(SSN) || defined(MVOCC)
  if (!read_my_own) {
    rc_t rc = {RC_INVALID};
    if (flags & TXN_FLAG_READ_ONLY) {
#if defined(SSI) || defined(SSN)
      if (config::enable_safesnap) {
        return rc_t{RC_TRUE};
      }
#endif
    } else {
#ifdef SSN
      rc = ssn_read(tuple);
#elif defined(SSI)
      rc = ssi_read(tuple);
#else
      rc = mvocc_read(tuple);
#endif
    }
    if (rc.IsAbort()) {
      return rc;
    }
  }  // otherwise it's my own update/insert, just read it
#endif

  // do the actual tuple read
  return tuple->DoRead(out_v);
}

}  // namespace ermia
