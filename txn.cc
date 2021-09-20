#include "macros.h"
#include "txn.h"
#include "dbcore/rcu.h"
#include "dbcore/serial.h"
#include "engine.h"

extern thread_local ermia::epoch_num coroutine_batch_end_epoch;
extern volatile bool ddl_running_1;
extern volatile bool ddl_running_2;
extern std::atomic<uint64_t> ddl_end;

namespace ermia {

uint64_t *_tls_commit_csn =
    (uint64_t *)malloc(sizeof(uint64_t) * config::MAX_THREADS);

uint64_t *_tls_begin_csn =
    (uint64_t *)malloc(sizeof(uint64_t) * config::MAX_THREADS);

uint64_t get_lowest_tls_commit_csn() {
  uint64_t min = std::numeric_limits<uint64_t>::max();
  for (uint32_t i = 0; i < config::MAX_THREADS; i++) {
    uint64_t csn = volatile_read(_tls_commit_csn[i]);
    if (csn) {
      min = std::min(csn, min);
    }
  }
  return min;
}

transaction::transaction(uint64_t flags, str_arena &sa, uint32_t coro_batch_idx)
    : flags(flags), log_size(0), sa(&sa), coro_batch_idx(coro_batch_idx) {
  if (config::phantom_prot) {
    masstree_absent_set.set_empty_key(NULL);  // google dense map
    masstree_absent_set.clear();
  }
  if (is_ddl()) {
    write_set.init_large_write_set();
#if defined(COPYDDL) && !defined(LAZYDDL)
    for (uint i = 0; i < config::worker_threads; i++) {
      cdc_offsets.push_back(0);
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
    log = NULL;
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
  // SI - see if it's read only. If so, skip logging etc.
  if (flags & TXN_FLAG_READ_ONLY) {
    log = nullptr;
  } else {
    log = GetLog();
  }
  
  // xc->begin = dlog::current_csn.load(std::memory_order_relaxed);
  if (config::state == config::kStateLoading) {
    xc->begin = dlog::current_csn.load(std::memory_order_relaxed);
  /*} else if (log) {
    xc->begin = log->get_committer()->get_lowest_tls_durable_csn();
  */} else {
    xc->begin = dlog::current_csn.load(std::memory_order_relaxed);
    volatile_write(_tls_begin_csn[thread::MyId()], xc->begin);
    // xc->begin = get_lowest_tls_commit_csn();
    // xc->begin = pcommit::lowest_csn.load(std::memory_order_relaxed);
    // xc->begin = log->get_committer()->get_lowest_tls_durable_csn();
    // printf("xc->begin: %lu\n", xc->begin);
  }
#endif
}

transaction::~transaction() {
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
    }
    else if (config::enable_safesnap && (flags & TXN_FLAG_READ_ONLY)) {
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
    oidmgr->UnlinkTuple(w.entry);
    obj->SetCSN(NULL_PTR);
    ASSERT(obj->GetAllocateEpoch() == xc->begin_epoch);
    MM::deallocate(entry);
  }
}

rc_t transaction::commit() {
  ALWAYS_ASSERT(state() == TXN::TXN_ACTIVE);
  volatile_write(xc->state, TXN::TXN_COMMITTING);
#if defined(SSN) || defined(SSI)
  // Safe snapshot optimization for read-only transactions:
  // Use the begin ts as cstamp if it's a read-only transaction
  // This is the same for both SSN and SSI.
  if (config::enable_safesnap && (flags & TXN_FLAG_READ_ONLY)) {
    ASSERT(!log);
    ASSERT(write_set.size() == 0);
    xc->end = xc->begin;
    volatile_write(xc->state, TXN::TXN_CMMTD);
    return {RC_TRUE};
  } else {
    ASSERT(log);
    xc->end = log->pre_commit().offset();
    if (xc->end == 0) {
      return rc_t{RC_ABORT_INTERNAL};
    }
#ifdef SSN
    return parallel_ssn_commit();
#elif defined SSI
    return parallel_ssi_commit();
#endif
  }
#elif defined(MVOCC)
  return mvocc_commit();
#else
  return si_commit();
#endif
}

#if !defined(SSI) && !defined(SSN) && !defined(MVOCC)
rc_t transaction::si_commit() {
  if ((flags & TXN_FLAG_READ_ONLY)) {
    volatile_write(xc->state, TXN::TXN_CMMTD);
    return rc_t{RC_TRUE};
  }

  if (config::phantom_prot && !MasstreeCheckPhantom()) {
    return rc_t{RC_ABORT_PHANTOM};
  }

  ASSERT(log);
  // Precommit: obtain a CSN
  xc->end = dlog::current_csn.load(std::memory_order_relaxed) + 1;
  // xc->end = dlog::current_csn.fetch_add(1);

#ifdef COPYDDL
  if (is_dml() && DMLConsistencyHandler()) {
    printf("DML failed\n");
    return rc_t{RC_ABORT_SI_CONFLICT};
  }
#endif

  xc->end = dlog::current_csn.fetch_add(1);

  /*#ifdef COPYDDL
  #if !defined(LAZYDDL)
      std::vector<ermia::thread::Thread *> cdc_workers;
      uint32_t cdc_threads = ermia::config::cdc_threads;
      if (is_ddl()) {
        ddl_running_1 = true;
        ermia::transaction *txn = this;
        ALWAYS_ASSERT(txn->GetXIDContext()->end == xc->end);
        uint32_t logs_per_cdc_thread =
            // (ermia::config::worker_threads - 1 - cdc_threads) / cdc_threads;
            (ermia::config::worker_threads - 1) / cdc_threads;
        //if (logs_per_cdc_thread == 1)
        //  logs_per_cdc_thread++;
        uint32_t thread_id = ermia::thread::MyId();
        std::vector<ermia::thread::Thread *> cdc_workers;
        bool exit = false;
        uint32_t increment = 0;
        printf("thread_id: %u\n", thread_id);
        for (uint i = 0; i < cdc_threads; i++) {
          uint32_t begin_log = i * logs_per_cdc_thread + increment;
          uint32_t end_log = (i + 1) * logs_per_cdc_thread - 1 + increment;
          if (thread_id == begin_log) {
            increment++;
            begin_log += increment;
            end_log += increment;
          } else if (thread_id > begin_log && thread_id <= end_log) {
            increment++;
            end_log += increment;
          }
          if (begin_log >= ermia::config::worker_threads - 1) {
            begin_log = ermia::config::worker_threads - 1;
            end_log = ermia::config::worker_threads - 1;
            exit = true;
          }
          if (end_log >= ermia::config::worker_threads - 1 ||
              i == cdc_threads - 1) {
            end_log = ermia::config::worker_threads - 1;
            exit = true;
          }
          printf("begin_log: %u, end_log: %u\n", begin_log, end_log);

          ermia::thread::Thread *thread = ermia::thread::GetThread(true);
          ALWAYS_ASSERT(thread);
          cdc_workers.push_back(thread);

          auto parallel_changed_data_capture = [=](char *) {
            int count = 0;
            bool ddl_end_local = false;
            while (ddl_running_1) {
              ddl_end_local =
  txn->new_td->GetPrimaryIndex()->changed_data_capture( txn,
  txn->GetXIDContext()->begin, txn->GetXIDContext()->end, thread_id, begin_log,
  end_log);
              // usleep(100);
              if (ddl_end_local)
                count++;
              if (ddl_end_local && ddl_running_2)
                break;
            }
            printf("cdc thread %u finishes with %d counts\n",
  ermia::thread::MyId(), count); ddl_end.fetch_add(1);
          };

          thread->StartTask(parallel_changed_data_capture);

          if (exit) {
            ermia::config::cdc_threads = i + 1;
            break;
          }
        }
      }
  #endif
  #endif*/

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
    // tuple->DoWrite();

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
    fat_ptr tuple_pdest = LSN::make(log->get_id(), lb_lsn + tuple_off, segnum, tuple_size_code).to_ptr();
    object->SetPersistentAddress(tuple_pdest);
    ASSERT(object->GetPersistentAddress().asi_type() == fat_ptr::ASI_LOG);

    // Set CSN
    fat_ptr csn_ptr = object->GenerateCsnPtr(xc->end);
    object->SetCSN(csn_ptr);
    ASSERT(tuple->GetObject()->GetCSN().asi_type() == fat_ptr::ASI_CSN);
  }
#endif

#if defined(COPYDDL) && !defined(LAZYDDL)
  if (is_ddl()) {
    uint32_t threads_use_latest_csn = config::worker_threads - 1;
    //    config::worker_threads - config::cdc_threads - 1;
    uint32_t count = 0;
    while (true) {
      for (uint32_t i = 0; i < config::MAX_THREADS; i++) {
        uint64_t csn = volatile_read(_tls_begin_csn[i]);
        if (csn && csn >= xc->end) {
          count++;
        }
      }
      if (threads_use_latest_csn == count)
        break;
      count = 0;
    }
    printf("lowest_csn: %lu\n", pcommit::lowest_csn.load());
    ddl_running_2 = true;
    while (ddl_end.load() != ermia::config::cdc_threads) {
    }
    /*for (std::vector<ermia::thread::Thread *>::const_iterator it =
             cdc_workers.begin();
         it != cdc_workers.end(); ++it) {
      (*it)->Join();
      ermia::thread::PutThread(*it);
      // printf("thread joined\n");
    }*/
  }
#endif

  volatile_write(xc->state, TXN::TXN_CMMTD);

  /*#ifdef COPYDDL
    if (is_ddl()) {
      // Do CDC
  #if !defined(LAZYDDL)
      // new_td->GetPrimaryIndex()->changed_data_capture(this, xc->begin,
  xc->end,
      // 1000);
  #endif
    }
  #endif*/

  /*#if defined(COPYDDL) && !defined(LAZYDDL)
    if (is_ddl()) {
      while (!ddl_end) {}
      ddl_running_2 = false;

      thread->Join();
      ermia::thread::PutThread(thread);
      printf("thread joined\n");
    }
  #endif*/

  // volatile_write(xc->state, TXN::TXN_CMMTD);

  // Normally, we'd generate it along the way or here first before toggling the
  // CSN's "committed" bit. But we can actually do it first, and generate the
  // log block as we scan the write set once, leveraging pipelined commit!

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
      // if (is_dml()) printf("id %u oid key commit starts\n", thread::MyId());
      if (lb->payload_size + align_up(log_str_size + sizeof(dlog::log_record)) > lb->capacity) {
        lb = log->allocate_log_block(logbuf_size, &lb_lsn, &segnum, xc->end);
      }
      dlog::log_oid_key(lb, w.fid, w.oid, (char *)w.str, log_str_size);
      // if (is_dml()) printf("id %u oid key commit ends\n", thread::MyId());
      ALWAYS_ASSERT(lb->payload_size <= lb->capacity);
      continue;
    }
   
    ALWAYS_ASSERT(w.type != dlog::log_record::logrec_type::OID_KEY);
    Object *object = w.get_object();
    dbtuple *tuple = (dbtuple *)object->GetPayload();
    // tuple->DoWrite();

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
    auto tuple_size_code = encode_size_aligned(w.size);
    fat_ptr tuple_pdest = LSN::make(log->get_id(), lb_lsn + tuple_off, segnum, tuple_size_code).to_ptr();
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

#if defined(COPYDDL) && !defined(LAZYDDL)
  if (is_ddl()) {
    /*ddl_running_1 = false;
    for (std::vector<ermia::thread::Thread *>::const_iterator it =
             cdc_workers.begin();
         it != cdc_workers.end(); ++it) {
      (*it)->Join();
      ermia::thread::PutThread(*it);
      // printf("thread joined\n");
    }*/

    //printf("free bufs\n");
    for (std::vector<char *>::iterator buf = bufs.begin(); buf != bufs.end(); buf++) {  
      free(*buf);
    }
  }
#endif

  // NOTE: make sure this happens after populating log block,
  // otherwise readers will see inconsistent data!
  // This is when (committed) tuple data are made visible to readers
  // volatile_write(xc->state, TXN::TXN_CMMTD);
  /*if (config::state == config::kStateForwardProcessing) {
    volatile_write(_tls_commit_csn[thread::MyId()], xc->end);
  }*/
  return rc_t{RC_TRUE};
}
#endif

#ifdef COPYDDL
void transaction::changed_data_capture() {
  for (uint i = 0; i < config::MAX_THREADS; i++) {
    dlog::tls_log *tlog = dlog::tlogs[i];
    uint64_t csn = volatile_read(pcommit::_tls_durable_csn[i]);
    if (tlog && i != thread::MyId() && csn) {
      tlog->cdc(this, xc->begin, xc->end, bufs);
    }
  }
}

bool transaction::DMLConsistencyHandler() {
  TXN::xid_context *tmp_xc = TXN::xid_get_context(xid);
  tmp_xc->begin_epoch = xc->begin_epoch;
  tmp_xc->xct = this;
  tmp_xc->begin = xc->end;

  for (auto& v : schema_read_map) {
    dbtuple *tuple =
            oidmgr->oid_get_version(schema_td->GetTupleArray(), v.second, tmp_xc);
    if (!tuple) {
      return true;
    } else {
      varstr tuple_v;
      if (DoTupleRead(tuple, &tuple_v)._val != RC_TRUE) {
        return true;
      }
      struct Schema_record schema;
      memcpy(&schema, (char *)tuple_v.data(), sizeof(schema));
      if (schema.td != v.first) {
        return true;
      }
    }
  }

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
    }      /*else if (prev_csn.asi_type() == fat_ptr::ASI_XID and
                    XID::from_ptr(prev_csn) != xid) {
           prev_persistent_ptr = prev_obj->GetPersistentAddress();
           TXN::xid_context *prev_holder =
         TXN::xid_get_context(XID::from_ptr(prev_csn));      auto state =
         volatile_read(prev_holder->state);      if (state != TXN::TXN_CMMTD) {
             MM::deallocate(new_obj_ptr);
             return rc_t{RC_ABORT_SI_CONFLICT};
           } else {
             add_to_write_set(is_ddl(), tuple_array->get(oid), tuple_fid, oid,
         tuple->size, dlog::log_record::logrec_type::UPDATE, k);
           }
         }*/
    else { // prev is committed (or precommitted but in post-commit now) head
#if defined(SSI) || defined(SSN) || defined(MVOCC)
      volatile_write(prev->sstamp, xc->owner.to_ptr());
      ASSERT(prev->sstamp.asi_type() == fat_ptr::ASI_XID);
      ASSERT(XID::from_ptr(prev->sstamp) == xc->owner);
      ASSERT(tuple->NextVolatile() == prev);
#endif
      add_to_write_set(is_ddl(), tuple_array->get(oid), tuple_fid, oid, tuple->size, dlog::log_record::logrec_type::UPDATE, k);
      prev_persistent_ptr = prev_obj->GetPersistentAddress();
    }

    ASSERT(not tuple->pvalue or tuple->pvalue->size() == tuple->size);
    ASSERT(tuple->GetObject()->GetCSN().asi_type() == fat_ptr::ASI_XID);
    ASSERT(sync_wait_coro(oidmgr->oid_get_version(tuple_fid, oid, xc)) == tuple);
    ASSERT(log);

    // FIXME(tzwang): mark deleted in all 2nd indexes as well?

    // The varstr also encodes the pdest of the overwritten version.
    // FIXME(tzwang): the pdest of the overwritten version doesn't belong to
    // varstr.

/*
    bool is_delete = !v;
    if (!v) {
      // Get an empty varstr just to store the overwritten tuple's
      // persistent address
      v = string_allocator().next(0);
      v->p = nullptr;
      v->l = 0;
    }
    ASSERT(v);
    v->ptr = prev_persistent_ptr;
    ASSERT(is_delete || (v->ptr.offset() && v->ptr.asi_type() == fat_ptr::ASI_LOG));

    // log the whole varstr so that recovery can figure out the real size
    // of the tuple, instead of using the decoded (larger-than-real) size.
    size_t data_size = v->size() + sizeof(varstr);
    auto size_code = encode_size_aligned(data_size);

    if (is_delete) {
      log->log_enhanced_delete(tuple_fid, oid,
                                 fat_ptr::make((void *)v, size_code),
                                 DEFAULT_ALIGNMENT_BITS);
    } else {
      log->log_update(tuple_fid, oid, fat_ptr::make((void *)v, size_code),
                        DEFAULT_ALIGNMENT_BITS,
                        tuple->GetObject()->GetPersistentAddressPtr());

      if (config::log_key_for_update) {
        ALWAYS_ASSERT(k);
        auto key_size = align_up(k->size() + sizeof(varstr));
        auto key_size_code = encode_size_aligned(key_size);
        log->log_update_key(tuple_fid, oid,
                              fat_ptr::make((void *)k, key_size_code),
                              DEFAULT_ALIGNMENT_BITS);
      }
    }
    */
    return rc_t{RC_TRUE};
  } else {  // somebody else acted faster than we did
    return rc_t{RC_ABORT_SI_CONFLICT};
  }
}

OID transaction::Insert(TableDescriptor *td, const varstr *k, varstr *value, dbtuple **out_tuple) {
  auto *tuple_array = td->GetTupleArray();
  FID tuple_fid = td->GetTupleFid();

  fat_ptr new_head = Object::Create(value, true, xc->begin_epoch);
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
  if (not read_my_own) {
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
    if (rc.IsAbort()) return rc;
  }  // otherwise it's my own update/insert, just read it
#endif

  // do the actual tuple read
  return tuple->DoRead(out_v, !read_my_own);
}

void transaction::enqueue_committed_xct() {
  log->enqueue_committed_xct(log->get_latest_csn(), t.get_start());
}

}  // namespace ermia
