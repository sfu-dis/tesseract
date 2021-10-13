#include "macros.h"
#include "txn.h"
#include "dbcore/rcu.h"
#include "dbcore/serial.h"
#include "engine.h"

extern thread_local ermia::epoch_num coroutine_batch_end_epoch;

volatile bool ddl_running_1 = false;
volatile bool ddl_running_2 = false;
volatile bool cdc_running = false;
std::atomic<uint64_t> ddl_end(0);
uint64_t *_tls_durable_lsn =
    (uint64_t *)malloc(sizeof(uint64_t) * (ermia::config::worker_threads - 1));

namespace ermia {

transaction::transaction(uint64_t flags, str_arena &sa, uint32_t coro_batch_idx)
    : flags(flags), log(nullptr), log_size(0), sa(&sa), coro_batch_idx(coro_batch_idx) {
  if (config::phantom_prot) {
    masstree_absent_set.set_empty_key(NULL);  // google dense map
    masstree_absent_set.clear();
  }
  if (is_ddl()) {
    ddl_running_1 = true;
    write_set.init_large_write_set();
#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
    uint32_t j = 0;
    for (uint32_t i = 0; i < ermia::dlog::tlogs.size(); i++) {
      dlog::tls_log *tlog = dlog::tlogs[i];
      if (tlog && tlog != log && volatile_read(pcommit::_tls_durable_csn[i])) {
        _tls_durable_lsn[j++] = tlog->get_durable_lsn();
      }
    }
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
    log = logmgr->new_tx_log((char*)string_allocator().next(sizeof(sm_tx_log))->data());
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
  log = logmgr->new_tx_log((char*)string_allocator().next(sizeof(sm_tx_log))->data());
  xc->begin = logmgr->cur_lsn().offset() + 1;
#else
  // Give a log regardless - with pipelined commit, read-only tx needs
  // to go through the queue as well
  if (ermia::config::pcommit || !(flags & TXN_FLAG_READ_ONLY)) {
    log = GetLog();
  }

#ifdef DCOPYDDL
  if (is_ddl()) {
    volatile_write(xc->state, TXN::TXN_DDL);
  }
#endif

  xc->begin = dlog::current_csn.load(std::memory_order_relaxed);
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
    write_record_t w = write_set.get(is_ddl(), i);
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
}

rc_t transaction::commit() {
  ALWAYS_ASSERT(state() == TXN::TXN_ACTIVE || state() == TXN::TXN_DDL);
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

#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
  if (is_ddl()) {
    std::vector<ermia::thread::Thread *> cdc_workers = changed_data_capture();
    while (get_cdc_largest_csn() <
           dlog::current_csn.load(std::memory_order_relaxed) - 100) {
    }

    join_changed_data_capture_threads(cdc_workers);
  }
#endif

  ASSERT(log);
  // Precommit: obtain a CSN
  xc->end = write_set.size() ? dlog::current_csn.fetch_add(1) : xc->begin;

#if defined(COPYDDL)
  if (ddl_running_1 && is_dml() && DMLConsistencyHandler()) {
    printf("DML failed\n");
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
    // log->set_dirty(true);
  }

#ifdef COPYDDL
  if (is_ddl()) {
    printf("DDL txn end: %lu\n", xc->end);
    // If txn is DDL, commit schema record first before CDC
    write_record_t w = write_set.get(is_ddl(), 0);
    Object *object = w.get_object();
    dbtuple *tuple = (dbtuple *)object->GetPayload();

    uint64_t log_tuple_size = sizeof(dbtuple) + tuple->size;
    uint64_t log_str_size = sizeof(varstr) + w.str->size();

    if (lb->payload_size + align_up(log_tuple_size + sizeof(dlog::log_record)) + align_up(log_str_size + sizeof(dlog::log_record)) > lb->capacity) { 
      lb = log->allocate_log_block(logbuf_size, &lb_lsn, &segnum, xc->end);
    }

    // Populate log block and obtain persistent address
    uint32_t str_off = lb->payload_size, tuple_off = str_off + align_up(log_str_size + sizeof(dlog::log_record));
    if (w.type == dlog::log_record::logrec_type::INSERT) {
      dlog::log_insert_key(lb, w.fid, w.oid, (char *)w.str, log_str_size);
      dlog::log_insert(lb, w.fid, w.oid, (char *)tuple, log_tuple_size);
    } else if (w.type == dlog::log_record::logrec_type::UPDATE) {
      dlog::log_update_key(lb, w.fid, w.oid, (char *)w.str, log_str_size);
      dlog::log_update(lb, w.fid, w.oid, (char *)tuple, log_tuple_size);
    }
    ALWAYS_ASSERT(lb->payload_size <= lb->capacity);

    // Set persistent address
    auto tuple_size_code = encode_size_aligned(w.size);
    fat_ptr tuple_pdest =
        LSN::make(log->get_id(), lb_lsn + sizeof(dlog::log_block) + tuple_off,
                  segnum, tuple_size_code)
            .to_ptr();
    object->SetPersistentAddress(tuple_pdest);
    ASSERT(object->GetPersistentAddress().asi_type() == fat_ptr::ASI_LOG);

    // Set CSN
    fat_ptr csn_ptr = object->GenerateCsnPtr(xc->end);
    object->SetCSN(csn_ptr);
    ASSERT(tuple->GetObject()->GetCSN().asi_type() == fat_ptr::ASI_CSN);
  }
#endif

#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
  if (is_ddl()) {
    ddl_running_2 = true;
    ddl_end.store(0);
    std::vector<ermia::thread::Thread *> cdc_workers = changed_data_capture();
    while (ddl_end.load() != ermia::config::cdc_threads) {
    }
    join_changed_data_capture_threads(cdc_workers);
  }
#endif

  if (is_ddl()) {
    // ddl_running_1 = false;
    ddl_running_2 = false;
  }

  volatile_write(xc->state, TXN::TXN_CMMTD);

  // Normally, we'd generate each version's persitent address along the way or
  // here first before toggling the CSN's "committed" bit. But we can actually
  // do it first, and generate the log block as we scan the write set once,
  // leveraging pipelined commit!

  // Post-commit: install CSN to tuples (traverse write-tuple), generate log
  // records, etc.
  uint32_t start = 0;
#ifdef COPYDDL
  if (is_ddl()) {
    start = 1;
  }
#endif
  for (uint32_t i = start; i < write_set.size(); ++i) {
    write_record_t w = write_set.get(is_ddl(), i);
   
    uint64_t log_str_size = sizeof(varstr) + w.str->size();
    if (w.type == dlog::log_record::logrec_type::OID_KEY) {
      if (lb->payload_size + align_up(log_str_size + sizeof(dlog::log_record)) > lb->capacity) {
        lb = log->allocate_log_block(logbuf_size, &lb_lsn, &segnum, xc->end);
      }
      dlog::log_oid_key(lb, w.fid, w.oid, (char *)w.str, log_str_size);
      ALWAYS_ASSERT(lb->payload_size <= lb->capacity);
      continue;
    }
   
    ALWAYS_ASSERT(w.type != dlog::log_record::logrec_type::OID_KEY);
    Object *object = w.get_object();
    dbtuple *tuple = (dbtuple *)object->GetPayload();

    uint64_t log_tuple_size = sizeof(dbtuple) + tuple->size;
    if (lb->payload_size + align_up(log_tuple_size + sizeof(dlog::log_record)) + align_up(log_str_size + sizeof(dlog::log_record)) > lb->capacity) {
      lb = log->allocate_log_block(logbuf_size, &lb_lsn, &segnum, xc->end);
    }

    // Populate log block and obtain persistent address
    uint32_t str_off = lb->payload_size, tuple_off = str_off + align_up(log_str_size + sizeof(dlog::log_record));
    if (w.type == dlog::log_record::logrec_type::INSERT) {
      dlog::log_insert_key(lb, w.fid, w.oid, (char *)w.str, log_str_size);
      dlog::log_insert(lb, w.fid, w.oid, (char *)tuple, log_tuple_size);
    } else if (w.type == dlog::log_record::logrec_type::UPDATE) {
      dlog::log_update_key(lb, w.fid, w.oid, (char *)w.str, log_str_size);
      dlog::log_update(lb, w.fid, w.oid, (char *)tuple, log_tuple_size);
    }
    ALWAYS_ASSERT(lb->payload_size <= lb->capacity);

    // Set persistent address

    // This aligned_size should match what was calculated during
    // add_to_write_set, and the size_code calculated based on this aligned size
    // will be part of the persistent address, which a read can directly use to
    // load the log record from the log (i.e., knowing how many bytes to read to
    // obtain the log record header + dbtuple header + record data).
    auto tuple_size_code = encode_size_aligned(w.size);

    // lb_lsn points to the start of the log block which has a header, followed
    // by individual log records, so the log record's direct address would be
    // lb_lsn + sizeof(log_block) + off
    fat_ptr tuple_pdest =
        LSN::make(log->get_id(), lb_lsn + sizeof(dlog::log_block) + tuple_off,
                  segnum, tuple_size_code)
            .to_ptr();
    object->SetPersistentAddress(tuple_pdest);
    ASSERT(object->GetPersistentAddress().asi_type() == fat_ptr::ASI_LOG);

    // Set CSN
    fat_ptr csn_ptr = object->GenerateCsnPtr(xc->end);
    object->SetCSN(csn_ptr);
    ASSERT(tuple->GetObject()->GetCSN().asi_type() == fat_ptr::ASI_CSN);
  }
  // ALWAYS_ASSERT(!lb || lb->payload_size == lb->capacity);

  // if (write_set.size())
  //   log->set_dirty(false);

  if (is_ddl()) {
    ddl_running_1 = false;
  }

  // NOTE: make sure this happens after populating log block,
  // otherwise readers will see inconsistent data!
  // This is when (committed) tuple data are made visible to readers
  return rc_t{RC_TRUE};
}
#endif

#ifdef COPYDDL
#if !defined(LAZYDDL) && !defined(DCOPYDDL)
std::vector<ermia::thread::Thread *> transaction::changed_data_capture() {
  cdc_running = true;
  transaction *txn = this;
  uint32_t cdc_threads = ermia::config::cdc_threads;
  uint32_t logs_per_cdc_thread =
      // (ermia::config::worker_threads - 1 - cdc_threads) / cdc_threads;
      (ermia::config::worker_threads - 1) / cdc_threads;

  uint32_t normal_workers[cdc_threads];
  uint j = 0;
  for (uint32_t i = 0; i < ermia::dlog::tlogs.size(); i++) {
    dlog::tls_log *tlog = dlog::tlogs[i];
    if (tlog && tlog != log && volatile_read(pcommit::_tls_durable_csn[i])) {
      normal_workers[j++] = i;
    }
  }

  std::vector<ermia::thread::Thread *> cdc_workers;
  for (uint32_t i = 0; i < cdc_threads; i++) {
    uint32_t begin_log = normal_workers[i * logs_per_cdc_thread];
    uint32_t end_log = normal_workers[(i + 1) * logs_per_cdc_thread - 1];

    thread::Thread *thread =
        thread::GetThread(config::cdc_physical_workers_only);
    ALWAYS_ASSERT(thread);
    cdc_workers.push_back(thread);

    auto parallel_changed_data_capture = [=](char *) {
      bool ddl_end_local = false;
      uint64_t cdc_offset = _tls_durable_lsn[i];
      // str_arena *arena = new str_arena(config::arena_size_mb);
      while (cdc_running) {
        ddl_end_local = txn->new_td->GetPrimaryIndex()->changed_data_capture(
            txn, i, txn->GetXIDContext()->begin,
            ermia::volatile_read(txn->GetXIDContext()->end), &cdc_offset,
            begin_log, end_log, nullptr);
        if (ddl_end_local && ddl_running_2)
          break;
        // usleep(10);
      }
      ddl_end.fetch_add(1);
      volatile_write(_tls_durable_lsn[i], cdc_offset);
    };

    thread->StartTask(parallel_changed_data_capture);
  }

  return cdc_workers;
}

void transaction::join_changed_data_capture_threads(
    std::vector<ermia::thread::Thread *> cdc_workers) {
  cdc_running = false;
  for (std::vector<ermia::thread::Thread *>::const_iterator it =
           cdc_workers.begin();
       it != cdc_workers.end(); ++it) {
    (*it)->Join();
    ermia::thread::PutThread(*it);
  }
}

uint64_t transaction::get_cdc_largest_csn() {
  uint64_t max = 0;
  for (uint32_t i = 0; i < config::cdc_threads; i++) {
    max = std::max(volatile_read(_cdc_last_csn[i]), max);
  }
  return max;
}
#endif

bool transaction::DMLConsistencyHandler() {
  TXN::xid_context *tmp_xc = TXN::xid_get_context(xid);
  tmp_xc->begin_epoch = xc->begin_epoch;
  tmp_xc->xct = this;
  tmp_xc->begin = xc->end;

  for (auto& v : schema_read_map) {
    dbtuple *tuple =
            oidmgr->oid_get_version(schema_td->GetTupleArray(), v.second, tmp_xc);
    if (!tuple) {
      tmp_xc->begin = xc->begin;
      return true;
    } else {
      varstr tuple_v;
      if (DoTupleRead(tuple, &tuple_v)._val != RC_TRUE) {
        tmp_xc->begin = xc->begin;
        return true;
      }
      struct Schema_record schema;
      memcpy(&schema, (char *)tuple_v.data(), sizeof(schema));
      if (schema.td != v.first) {
        tmp_xc->begin = xc->begin;
        return true;
      }
    }
  }

  tmp_xc->begin = xc->begin;
  return false;
}
#endif

// returns true if btree versions have changed, ie there's phantom
bool transaction::MasstreeCheckPhantom() {
  for (auto &r : masstree_absent_set) {
    const uint64_t v = ConcurrentMasstree::ExtractVersionNumber(r.first);
    if (unlikely(v != r.second)) return false;
  }
  return true;
}

rc_t transaction::Update(TableDescriptor *td, OID oid, const varstr *k, varstr *v) {
  oid_array *tuple_array = td->GetTupleArray();
  FID tuple_fid = td->GetTupleFid();

  // first *updater* wins
  fat_ptr new_obj_ptr = NULL_PTR;
  fat_ptr prev_obj_ptr =
      oidmgr->UpdateTuple(tuple_array, oid, v, xc, &new_obj_ptr);
  Object *prev_obj = (Object *)prev_obj_ptr.offset();

  if (prev_obj) {  // succeeded
    Object *new_obj = (Object *)new_obj_ptr.offset();
    dbtuple *tuple = new_obj->GetPinnedTuple();
    ASSERT(tuple);
    dbtuple *prev = prev_obj->GetPinnedTuple();
    ASSERT((uint64_t)prev->GetObject() == prev_obj_ptr.offset());
    ASSERT(xc);
#ifdef SSI
    ASSERT(prev->sstamp == NULL_PTR);
    if (xc->ct3) {
      // Check if we are the T2 with a committed T3 earlier than a safesnap
      // (being T1)
      if (xc->ct3 <= xc->last_safesnap) return {RC_ABORT_SERIAL};

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
              return {RC_ABORT_SERIAL};
            }
          }
        } else {
          oidmgr->UnlinkTuple(tuple_array, oid);
          return {RC_ABORT_SERIAL};
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
      return rc_t{RC_ABORT_SERIAL};
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
      // add_to_write_set(is_ddl(), tuple_array->get(oid), tuple_fid, oid,
      //                  tuple->size, dlog::log_record::logrec_type::UPDATE,
      //                  k);
    } else { // prev is committed (or precommitted but in post-commit now) head
#if defined(SSI) || defined(SSN) || defined(MVOCC)
      volatile_write(prev->sstamp, xc->owner.to_ptr());
      ASSERT(prev->sstamp.asi_type() == fat_ptr::ASI_XID);
      ASSERT(XID::from_ptr(prev->sstamp) == xc->owner);
      ASSERT(tuple->NextVolatile() == prev);
#endif
      add_to_write_set(is_ddl(), tuple_array->get(oid), tuple_fid, oid, tuple->size, dlog::log_record::logrec_type::UPDATE, k);
      prev_persistent_ptr = prev_obj->GetPersistentAddress();
    }

    ASSERT(tuple->GetObject()->GetCSN().asi_type() == fat_ptr::ASI_XID);
    ASSERT(sync_wait_coro(oidmgr->oid_get_version(tuple_fid, oid, xc)) == tuple);
    ASSERT(log);

    // FIXME(tzwang): mark deleted in all 2nd indexes as well?
    return rc_t{RC_TRUE};
  } else {  // somebody else acted faster than we did
    return rc_t{RC_ABORT_SI_CONFLICT};
  }
}

OID transaction::Insert(TableDescriptor *td, const varstr *k, varstr *value, dbtuple **out_tuple) {
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
  oidmgr->oid_put_new(tuple_array, oid, new_head);

  ASSERT(tuple->size == value->size());
  add_to_write_set(is_ddl(), tuple_array->get(oid), tuple_fid, oid, tuple->size, dlog::log_record::logrec_type::INSERT, k);

  if (out_tuple) {
    *out_tuple = tuple;
  }
  return oid;
}

void transaction::LogIndexInsert(OrderedIndex *index, OID oid, const varstr *key) {
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
    // add_to_write_set(is_ddl(), nullptr, index->GetTableDescriptor()->GetTupleFid(), oid, 0, dlog::log_record::logrec_type::OID_KEY, key);
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
