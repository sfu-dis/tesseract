#include "macros.h"
#include "txn.h"
#include "dbcore/rcu.h"
#include "dbcore/serial.h"
#include "engine.h"

extern thread_local ermia::epoch_num coroutine_batch_end_epoch;

namespace ermia {

volatile bool ddl_running_1 = false;
volatile bool ddl_running_2 = false;
volatile bool cdc_running = false;
volatile bool cdc_failed = false;
volatile bool cdc_test = false;
std::atomic<uint64_t> ddl_end(0);
uint64_t *_tls_durable_lsn =
    (uint64_t *)malloc(sizeof(uint64_t) * ermia::config::MAX_THREADS);

transaction::transaction(uint64_t flags, str_arena &sa, uint32_t coro_batch_idx)
    : flags(flags), log(nullptr), log_size(0), sa(&sa), coro_batch_idx(coro_batch_idx) {
  if (config::phantom_prot) {
    masstree_absent_set.set_empty_key(NULL);  // google dense map
    masstree_absent_set.clear();
  }
  wait_for_new_schema = false;
  if (is_ddl()) {
    ddl_running_1 = true;
    // write_set.init_large_write_set();
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
  if (ermia::config::pcommit) {
    log = GetLog();
#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
    if (is_ddl()) {
      for (uint32_t i = 0; i < ermia::dlog::tlogs.size(); i++) {
        dlog::tls_log *tlog = dlog::tlogs[i];
        if (tlog && tlog != log &&
            volatile_read(pcommit::_tls_durable_csn[i])) {
          _tls_durable_lsn[i] = tlog->get_durable_lsn();
        }
      }
    }
#endif
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
    write_record_t w = write_set.get(i);
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

  ASSERT(log);
  // Precommit: obtain a CSN
  xc->end = write_set.size() ? dlog::current_csn.fetch_add(1) : xc->begin;

#if defined(COPYDDL)
  if (is_dml() && DMLConsistencyHandler()) {
    // printf("DML failed with begin: %lu, end: %lu\n", xc->begin, xc->end);
    std::cerr << "DML failed" << std::endl;
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

#ifdef COPYDDL
  if (is_ddl()) {
    printf("DDL txn end: %lu\n", xc->end);
    // If txn is DDL, commit schema record(s) first before CDC
    for (uint32_t i = 0; i < write_set.size(); ++i) {
      write_record_t w = write_set.get(i);
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
      fat_ptr csn_ptr = object->GenerateCsnPtr(xc->end);
      object->SetCSN(csn_ptr);
      ASSERT(tuple->GetObject()->GetCSN().asi_type() == fat_ptr::ASI_CSN);
      printf("DDL schema commit with size %d\n", write_set.size());
    }
    if (config::enable_cdc_verification_test) {
      cdc_test = false;
    }
  }
#endif

#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
  if (is_ddl()) {
    // Fix new table file's marks
    auto *alloc = oidmgr->get_allocator(this->old_td->GetTupleFid());
    uint32_t himark = alloc->head.hiwater_mark;
    // printf("old td sz: %d\n", himark);
    auto *new_tuple_array = this->new_td->GetTupleArray();
    new_tuple_array->ensure_size(new_tuple_array->alloc_size(himark - 64));
    oidmgr->recreate_allocator(this->new_td->GetTupleFid(), himark - 64);
    auto *new_alloc = oidmgr->get_allocator(this->new_td->GetTupleFid());
    himark = new_alloc->head.hiwater_mark;
    // printf("new td sz: %d\n", himark);

    // Switch table descriptor for index
    OrderedIndex *index = this->new_td->GetPrimaryIndex();
    index->SetTableDescriptor(this->new_td);
    index->SetArrays(true);

    // Start the second round of CDC
    printf("Second CDC begins\n");
    ddl_running_2 = true;
    ddl_end.store(0);
    std::vector<ermia::thread::Thread *> cdc_workers = changed_data_capture();
    while (ddl_end.load() != ermia::config::cdc_threads && !cdc_failed) {
    }
    join_changed_data_capture_threads(cdc_workers);
    ddl_running_2 = false;
    printf("Second CDC ends\n");
    if (cdc_failed) {
      printf("DDL failed\n");
      index->SetTableDescriptor(this->old_td);
      index->SetArrays(true);
      this->new_td->GetTupleArray()->destroy(this->new_td->GetTupleArray());
      return rc_t{RC_ABORT_INTERNAL};
    } else {
      printf("cdc largest csn: %lu\n", get_cdc_largest_csn());
      ALWAYS_ASSERT(get_cdc_largest_csn() == xc->end - 1);
    }

    for (uint32_t i = 0; i < write_set.size(); ++i) {
      write_record_t w = write_set.get(i);
      Object *object = w.get_object();
      dbtuple *tuple = (dbtuple *)object->GetPayload();

      varstr value(tuple->get_value_start(), tuple->size);
      struct Schema_record schema;
      memcpy(&schema, (char *)value.data(), sizeof(schema));
      schema.state = 0;

      char schema_str[sizeof(Schema_record)];
      memcpy(schema_str, &schema, sizeof(schema_str));
      string_allocator().reset();
      varstr *new_value = string_allocator().next(sizeof(schema_str));
      new_value->copy_from(schema_str, sizeof(schema_str));

      /*if (ddl_exe->get_ddl_type() == ddl::ddl_type::VERIFICATION_ONLY
        || ddl_exe->get_ddl_type() == ddl::ddl_type::COPY_VERIFICATION) {
        fat_ptr new_head = ddl_exe->get_new_schema_fat_ptr();
        Object *new_object = (Object *)new_head.offset();
        ALWAYS_ASSERT(new_object != nullptr);
        new_object->SetCSN(new_object->GenerateCsnPtr(xc->end));
        fat_ptr *entry_ptr = schema_td->GetTupleArray()->get(w.oid);
        fat_ptr expected = *entry_ptr;
        new_object->SetNextVolatile(expected);
      retry:
        if (!__sync_bool_compare_and_swap(&entry_ptr->_ptr, expected._ptr,
                                          new_head._ptr)) {
          goto retry;
        }
        printf("Commit new schema\n");
      } else {*/
      ALWAYS_ASSERT(
          DDLSchemaUnblock(schema_td, w.oid, new_value, xc->end)._val ==
          RC_TRUE);
      //}
    }
  }
#endif

  if (is_ddl()) {
    ddl_running_1 = false;
#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
    goto exit;
#endif
  }

  // Normally, we'd generate each version's persitent address along the way or
  // here first before toggling the CSN's "committed" bit. But we can actually
  // do it first, and generate the log block as we scan the write set once,
  // leveraging pipelined commit!

  // Post-commit: install CSN to tuples (traverse write-tuple), generate log
  // records, etc.
  for (uint32_t i = 0; i < write_set.size(); ++i) {
    write_record_t w = write_set.get(i);
    Object *object = w.get_object();
    dbtuple *tuple = (dbtuple *)object->GetPayload();

    uint64_t log_tuple_size = sizeof(dbtuple) + tuple->size;
    if (lb->payload_size + align_up(log_tuple_size + sizeof(dlog::log_record)) >
        lb->capacity) {
      lb = log->allocate_log_block(logbuf_size, &lb_lsn, &segnum, xc->end);
    }

    // Populate log block and obtain persistent address
    uint32_t off = lb->payload_size;
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

exit:
  // NOTE: make sure this happens after populating log block,
  // otherwise readers will see inconsistent data!
  // This is when (committed) tuple data are made visible to readers
  volatile_write(xc->state, TXN::TXN_CMMTD);
  return rc_t{RC_TRUE};
}
#endif

#ifdef COPYDDL
#if !defined(LAZYDDL) && !defined(DCOPYDDL)
std::vector<ermia::thread::Thread *> transaction::changed_data_capture() {
  cdc_failed = false;
  cdc_running = true;
  transaction *txn = this;
  uint32_t cdc_threads = ermia::config::cdc_threads;
  uint32_t logs_per_cdc_thread =
      (ermia::config::worker_threads - 1) / cdc_threads;

  uint32_t normal_workers[ermia::config::worker_threads - 1];
  uint32_t j = 0;
  uint32_t ddl_thread_id = -1;
  for (uint32_t i = 0; i < ermia::dlog::tlogs.size(); i++) {
    dlog::tls_log *tlog = dlog::tlogs[i];
    if (tlog && tlog != log && volatile_read(pcommit::_tls_durable_csn[i])) {
      // printf("normal_workers[%d]: %d\n", j, i);
      normal_workers[j++] = i;
      tlog->last_flush();
    } else if (tlog == log) {
      ddl_thread_id = i;
    }
  }
  // printf("ddl_thread_id: %d\n", ddl_thread_id);

  std::vector<ermia::thread::Thread *> cdc_workers;
  for (uint32_t i = 0; i < cdc_threads; i++) {
    uint32_t begin_log = normal_workers[i * logs_per_cdc_thread];
    uint32_t end_log = normal_workers[(i + 1) * logs_per_cdc_thread - 1];
    if (i == cdc_threads - 1)
      end_log = normal_workers[--j];
    // printf("%d, %d\n", begin_log, end_log);

    thread::Thread *thread =
        thread::GetThread(config::cdc_physical_workers_only);
    ALWAYS_ASSERT(thread);
    cdc_workers.push_back(thread);

    auto parallel_changed_data_capture = [=](char *) {
      bool ddl_end_local = false;
      str_arena *arena = new str_arena(config::arena_size_mb);
      rc_t rc;
      while (cdc_running) {
        rc = ddl_exe->changed_data_capture_impl(
            this, i, ddl_thread_id, begin_log, end_log, arena, &ddl_end_local);
        if (rc._val != RC_TRUE) {
          cdc_failed = true;
          cdc_running = false;
        }
        if (ddl_end_local && ddl_running_2) {
          break;
        }
        // usleep(10);
      }
      ddl_end.fetch_add(1);
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
  for (uint32_t i = 0; i < config::MAX_THREADS; i++) {
    dlog::tls_log *tlog = dlog::tlogs[i];
    if (tlog && tlog != log && volatile_read(pcommit::_tls_durable_csn[i])) {
      max = std::max(volatile_read(_cdc_last_csn[i]), max);
    }
  }
  return max;
}

uint64_t transaction::get_cdc_smallest_csn() {
  uint64_t min = std::numeric_limits<uint64_t>::max();
  for (uint32_t i = 0; i < config::MAX_THREADS; i++) {
    dlog::tls_log *tlog = dlog::tlogs[i];
    if (tlog && tlog != log && volatile_read(pcommit::_tls_durable_csn[i])) {
      min = std::min(volatile_read(_cdc_last_csn[i]), min);
    }
  }
  return min;
}
#endif

bool transaction::DMLConsistencyHandler() {
  TXN::xid_context *tmp_xc = TXN::xid_get_context(xid);
  uint64_t begin = xc->begin;
  tmp_xc->begin = xc->end;

  for (auto& v : schema_read_map) {
    dbtuple *tuple =
            oidmgr->oid_get_version(schema_td->GetTupleArray(), v.second, tmp_xc);
    if (!tuple) {
      tmp_xc->begin = begin;
      return true;
    } else {
      varstr tuple_v;
      if (DoTupleRead(tuple, &tuple_v)._val != RC_TRUE) {
        tmp_xc->begin = begin;
        return true;
      }
      struct Schema_record schema;
      memcpy(&schema, (char *)tuple_v.data(), sizeof(schema));
      if (schema.td != v.first) {
        tmp_xc->begin = begin;
        return true;
      }
    }
  }

  tmp_xc->begin = begin;
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

PROMISE(rc_t)
transaction::Update(TableDescriptor *td, OID oid, const varstr *k, varstr *v) {
  oid_array *tuple_array = td->GetTupleArray();
  FID tuple_fid = td->GetTupleFid();

  // first *updater* wins
  fat_ptr new_obj_ptr = NULL_PTR;
  fat_ptr prev_obj_ptr = oidmgr->UpdateTuple(
      tuple_array, oid, v, xc, &new_obj_ptr, IsWaitForNewSchema());
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
      if (xc->ct3 <= xc->last_safesnap)
        RETURN{RC_ABORT_SERIAL};

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
    } else { // prev is committed (or precommitted but in post-commit now) head
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
    ASSERT(sync_wait_coro(oidmgr->oid_get_version(tuple_fid, oid, xc)) == tuple);
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

PROMISE(rc_t)
transaction::DDLCDCUpdate(TableDescriptor *td, OID oid, varstr *value,
                          uint64_t tuple_csn) {
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
  if (!tuple_csn)
    new_object->SetNextVolatile(expected);
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
  if (expected == NULL_PTR || CSN::from_ptr(csn).offset() < tuple_csn) {
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
  } else {
    // MM::deallocate(new_head);
  }

  RETURN rc_t{RC_TRUE};
}

void transaction::DDLScanInsert(TableDescriptor *td, OID oid, varstr *value) {
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

void transaction::DDLScanUpdate(TableDescriptor *td, OID oid, varstr *value) {
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
                   dlog::log_record::logrec_type::INSERT);
}

PROMISE(rc_t)
transaction::DDLCDCInsert(TableDescriptor *td, OID oid, varstr *value,
                          uint64_t tuple_csn) {
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

  RETURN rc_t{RC_TRUE};
}

/*PROMISE(rc_t)
transaction::DDLUpdate(TableDescriptor *td, OID oid, varstr *value) {
  auto *tuple_array = td->GetTupleArray();
  FID tuple_fid = td->GetTupleFid();
  tuple_array->ensure_size(oid);

  fat_ptr new_head = Object::Create(value, xc->begin_epoch);
  ASSERT(new_head.size_code() != INVALID_SIZE_CODE);
  ASSERT(new_head.asi_type() == 0);
  Object *new_object = (Object *)new_head.offset();
  auto *new_tuple = (dbtuple *)new_object->GetPayload();
  new_object->SetCSN(xid.to_ptr());

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
  if (csn != NULL_PTR && csn.asi_type() == fat_ptr::ASI_XID) {
    auto holder_xid = XID::from_ptr(csn);
    TXN::xid_context *holder = TXN::xid_get_context(holder_xid);
    if (holder && volatile_read(holder->begin) >= tuple_csn) {
      MM::deallocate(new_head);
      RETURN rc_t{RC_ABORT_SI_CONFLICT};
    }
  }
  if (expected == NULL_PTR || csn.asi_type() == fat_ptr::ASI_XID ||
      CSN::from_ptr(csn).offset() < tuple_csn) {
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
  } else {
    // MM::deallocate(new_head);
  }

  RETURN rc_t{RC_TRUE};
}*/

PROMISE(OID)
transaction::DDLInsert(TableDescriptor *td, varstr *value) {
  auto *tuple_array = td->GetTupleArray();
  auto *old_tuple_array = old_td->GetTupleArray();
  FID tuple_fid = td->GetTupleFid();

  fat_ptr new_head = Object::Create(value, xc->begin_epoch);
  ASSERT(new_head.size_code() != INVALID_SIZE_CODE);
  ASSERT(new_head.asi_type() == 0);
  auto *tuple = (dbtuple *)((Object *)new_head.offset())->GetPayload();
  ASSERT(decode_size_aligned(new_head.size_code()) >= tuple->size);
  tuple->GetObject()->SetCSN(xid.to_ptr());
get_oid:
  OID oid = oidmgr->alloc_oid(tuple_fid);
  ALWAYS_ASSERT(oid != INVALID_OID);
  if (IsWaitForNewSchema()) {
    if (oidmgr->oid_get_latest_version(old_tuple_array, oid)) {
      // goto get_oid;
      RETURN INVALID_OID;
    }
    if (oidmgr->oid_get_latest_version(tuple_array, oid)) {
      // goto get_oid;
      RETURN INVALID_OID;
    }
  }
  oidmgr->oid_put_new(tuple_fid, oid, new_head);

  ASSERT(tuple->size == value->size());
  add_to_write_set(tuple_fid == schema_td->GetTupleFid(), tuple_array->get(oid),
                   tuple_fid, oid, tuple->size,
                   dlog::log_record::logrec_type::INSERT);
  /*
    // tuple_array->ensure_size(oid);
    fat_ptr new_head = Object::Create(value, xc->begin_epoch);
    ASSERT(new_head.size_code() != INVALID_SIZE_CODE);
    ASSERT(new_head.asi_type() == 0);
    Object *new_object = (Object *)new_head.offset();
    auto *new_tuple = (dbtuple *)new_object->GetPayload();
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
      RETURN INVALID_OID;
    }
    // oidmgr->oid_put_new(tuple_array, oid, new_head);

    if (!overwrite) {
      ASSERT(new_tuple->size == value->size());
      add_to_write_set(tuple_fid == schema_td->GetTupleFid(),
                       tuple_array->get(oid), tuple_fid, oid, new_tuple->size,
                       dlog::log_record::logrec_type::INSERT);
    }
  */
  RETURN oid;
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
transaction::OverlapCheck(TableDescriptor *td, OID oid) {
  auto *tuple_array = td->GetTupleArray();
  tuple_array->ensure_size(oid);

  fat_ptr *entry_ptr = tuple_array->get(oid);
  fat_ptr expected = *entry_ptr;
  Object *obj = (Object *)expected.offset();
  fat_ptr csn = obj->GetCSN();
  if (expected == NULL_PTR)
    RETURN true;
  if (expected != NULL_PTR &&
      (csn.asi_type() == fat_ptr::ASI_XID ||
       CSN::from_ptr(csn).offset() >
           volatile_read(_cdc_last_csn[log->get_id()]))) {
    RETURN true;
  }

  RETURN false;
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

} // namespace ermia
