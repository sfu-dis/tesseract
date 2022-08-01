#include "ddl.h"

#include "../benchmarks/oddlb-schemas.h"
#include "../benchmarks/tpcc.h"
#include "../schema.h"
#include "../txn.h"

namespace ermia {

namespace ddl {

volatile bool ddl_start = false;
volatile bool cdc_test = false;

std::vector<Reformat> reformats;
std::vector<Constraint> constraints;
#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
mcs_lock *lazy_ddl_locks =
    (mcs_lock *)malloc(sizeof(mcs_lock) * 256);
std::vector<bitmap *> bitmaps;

bool migrate_record(FID fid, OID oid) {
  if (config::enable_lazy_on_conflict_do_nothing) {
    return true;
  }
  for (auto &bitmap : bitmaps) {
    if (bitmap->fid == fid) {
      if (oid >= bitmap->size && (oid < bitmap->size && bitmap->data[oid])) {
        return false;
      }
      CRITICAL_SECTION(cs, lazy_ddl_locks[oid % 256]);
      if (oid < bitmap->size && !bitmap->data[oid]) {
        bitmap->data[oid] = 1;
        return true;
      }
    }
  }
  return false;
}
#endif

rc_t ddl_executor::scan(str_arena *arena) {
#if defined(COPYDDL) && !defined(LAZYDDL)
  if (config::enable_parallel_scan_cdc) {
    DLOG(INFO) << "First CDC begins";
    changed_data_capture();
  }
#endif

  rc_t r;
  TXN::xid_context *xc = t->GetXIDContext();

  DLOG(INFO) << "DDL scan begins";
  uint64_t count = 0;
  FID fid = old_td->GetTupleFid();
  auto *alloc = oidmgr->get_allocator(fid);
  uint32_t himark = alloc->head.hiwater_mark;
  auto *old_tuple_array = old_td->GetTupleArray();
  auto *key_array = old_td->GetKeyArray();
  DLOG(INFO) << "himark: " << himark;

#if defined(COPYDDL) && !defined(LAZYDDL)
  uint32_t scan_threads;
  if (config::enable_parallel_scan_cdc || config::enable_late_scan_join) {
    scan_threads = config::scan_threads;
  } else {
    scan_threads = config::scan_threads + config::cdc_threads;
  }
#else
  uint32_t scan_threads = config::scan_threads + config::cdc_threads;
#endif

  uint32_t total_per_scan_thread = himark / scan_threads;
  DLOG(INFO) << "scan_threads: " << scan_threads << ", total_per_scan_thread: " << total_per_scan_thread;

  for (uint32_t i = 1; i < scan_threads; i++) {
    uint32_t begin = i * total_per_scan_thread;
    uint32_t end = (i + 1) * total_per_scan_thread;
    if (i == scan_threads - 1) {
      end = himark;
    }
    thread::Thread *thread = thread::GetThread(config::scan_physical_workers_only);
    ALWAYS_ASSERT(thread);
    scan_workers.push_back(thread);
    auto parallel_scan = [=](char *) {
      dlog::log_block *lb = nullptr;
      str_arena *arena = GetArena();
      TXN::xid_context local_xc = *xc;
#ifdef COPYDDL
      if (config::enable_large_ddl_begin_timestamp) {
        local_xc.begin += 100000000;
      }
#endif
      arena->reset();
      for (uint32_t oid = begin + 1; oid <= end; oid++) {
	rc_t r = scan_impl(arena, oid, fid, &local_xc, old_tuple_array, key_array, lb, i, this);
        if (r._val != RC_TRUE || flags.ddl_failed) {
          break;
        }
      }
    };
    thread->StartTask(parallel_scan);
  }

  dlog::log_block *lb = nullptr;
  TXN::xid_context local_xc = *xc;
#ifdef COPYDDL
  if (config::enable_large_ddl_begin_timestamp) {
    local_xc.begin += 100000000;
  }
#endif
  OID end = scan_threads == 1 ? himark : total_per_scan_thread;
  for (OID oid = 0; oid <= end; oid++) {
    r = scan_impl(arena, oid, fid, &local_xc, old_tuple_array, key_array, lb, 0, this);
    if (r._val != RC_TRUE || flags.ddl_failed) {
      break;
    }
  }

#if defined(COPYDDL) && !defined(LAZYDDL)
  if (!config::enable_late_scan_join || flags.ddl_failed) {
    join_scan_workers();
  }
  if (config::enable_parallel_scan_cdc) {
    flags.cdc_running = false;
    join_cdc_workers();
  }
  if (flags.ddl_failed) {
    DLOG(INFO) << "DDL failed";
    join_cdc_workers();
    for (auto &param : ddl_executor_paras_list) {
      if (param->new_td != param->old_td) {
        param->new_td->GetTupleArray()->destroy(param->new_td->GetTupleArray());
      }
    }
    dlog::tls_log *log = t->get_log();
    log->set_dirty(false);
    log->set_doing_ddl(false);
    return rc_t{RC_ABORT_INTERNAL};
  }
  DLOG(INFO) << "First CDC ends, now go to grab a t4";
  if (config::enable_cdc_verification_test) {
    cdc_test = true;
  }
#else
  join_scan_workers();
  if (flags.ddl_failed) {
    dlog::tls_log *log = t->get_log();
    log->set_dirty(false);
    log->set_doing_ddl(false);
    return rc_t{RC_ABORT_INTERNAL};
  }
#endif

  return rc_t{RC_TRUE};
}

rc_t ddl_executor::scan_impl(str_arena *arena, OID oid, FID old_fid,
                             TXN::xid_context *xc, oid_array *old_tuple_array,
                             oid_array *key_array, dlog::log_block *lb, int wid,
                             ddl_executor *ddl_exe) {
  dbtuple *tuple = AWAIT oidmgr->oid_get_version(old_tuple_array, oid, xc);
  varstr tuple_value;
  fat_ptr *entry = config::enable_ddl_keys ? key_array->get(oid) : nullptr;
  varstr *key = entry ? (varstr *)((*entry).offset()) : nullptr;
  if (!key && config::enable_ddl_keys) {
    return rc_t{RC_TRUE};
  }
  if (tuple && t->DoTupleRead(tuple, &tuple_value)._val == RC_TRUE) {
    for (auto &param : ddl_executor_paras_list) {
      if (param->old_td->GetTupleFid() != old_fid) {
        continue;
      }
      arena->reset();
      if (param->type == VERIFICATION_ONLY || param->type == COPY_VERIFICATION) {
        if (!constraints[param->constraint_idx](key, tuple_value, arena, param->new_v, xc->begin)) {
          DLOG(INFO) << "DDL failed";
          flags.ddl_failed = true;
          flags.cdc_running = false;
          return rc_t{RC_ABORT_INTERNAL};
        }
      }
      if (param->type == COPY_ONLY || param->type == COPY_VERIFICATION) {
        uint64_t reformat_idx = param->scan_reformat_idx == -1
                                    ? param->reformat_idx
                                    : param->scan_reformat_idx;
        varstr *new_tuple_value = reformats[reformat_idx](key, tuple_value, arena, param->new_v, old_fid, oid, t, xc->begin, true);
        if (!new_tuple_value) {
          continue;
        }
#ifdef COPYDDL
#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
        if (!migrate_record(param->new_td->GetTupleFid(), oid)) {
          continue;
        }
        OID o;
        rc_t rc;
        /*AWAIT param->index->GetOID(*key, rc, xc, o);
        if (rc._val == RC_TRUE) {
          continue;
        }*/
        fat_ptr *out_entry = nullptr;
        o = t->LazyDDLInsert(param->new_td, new_tuple_value, xc->end, &out_entry);
        if (!o) {
          continue;
        }
        if (!AWAIT param->index->InsertOID(t, *key, o)) {
        lazy_abort:
          Object *obj = (Object *)out_entry->offset();
          fat_ptr entry = *out_entry;
          obj->SetCSN(NULL_PTR);
          oidmgr->UnlinkTuple(out_entry);
          ASSERT(obj->GetAllocateEpoch() == xc->begin_epoch);
          MM::deallocate(entry);
          continue;
        } else {
          if (param->new_td->GetSecIndexes().size()) {
            auto *secondary_index = (ConcurrentMasstreeIndex *)(param->new_td->GetSecIndexes().front());
            varstr *new_secondary_index_key = reformats[param->secondary_index_key_create_idx](
                    key, tuple_value, arena, param->new_v, old_fid, oid, t, xc->begin, true);
            if (!AWAIT secondary_index->InsertOID(t, *new_secondary_index_key, o)) {
              goto lazy_abort;
	    }
          }
        }
#elif defined(OPTLAZYDDL)
        t->DDLInsert(param->new_td, oid, new_tuple_value, xc->end);
#else
        t->DDLInsert(param->new_td, oid, new_tuple_value, tuple->GetCSN());
#endif
#elif defined(BLOCKDDL)
	rc_t r;
	if (param->new_td == param->old_td) {
	  r = t->Update(param->new_td, oid, nullptr, new_tuple_value, wid, ddl_exe);
	} else {
	  r = t->DDLInsert(param->new_td, oid, new_tuple_value, 0, true, wid, ddl_exe);
	}
	ASSERT(r._val == RC_TRUE);
#elif defined(SIDDL)
        rc_t r;
        if (param->new_td == param->old_td) {
          r = t->Update(param->new_td, oid, nullptr, new_tuple_value, wid, ddl_exe);
        } else {
          r = t->DDLInsert(param->new_td, oid, new_tuple_value, 0, true, wid, ddl_exe);
        }
	if (r._val != RC_TRUE) {
          DLOG(INFO) << "DDL failed";
          flags.ddl_failed = true;
          return rc_t{RC_ABORT_INTERNAL};
        }
#endif
      }
    }
  }
  return rc_t{RC_TRUE};
}

#if defined(COPYDDL) && !defined(LAZYDDL)
uint32_t ddl_executor::changed_data_capture() {
  flags.ddl_failed = false;
  flags.cdc_running = true;
  dlog::tls_log *log = t->get_log();
  uint32_t cdc_threads;
  if ((config::enable_parallel_scan_cdc && !flags.cdc_second_phase) ||
      (config::enable_late_scan_join && flags.cdc_second_phase)) {
    cdc_threads = config::cdc_threads;
  } else {
    cdc_threads = config::cdc_threads + config::scan_threads - 1;
  }
  if (!cdc_threads) {
    return cdc_threads;
  }
  DLOG(INFO) << "cdc_threads: " << cdc_threads;
  uint32_t logs_per_cdc_thread = (config::worker_threads - 1) / cdc_threads;
  int rest_logs =
      config::worker_threads - 1 - logs_per_cdc_thread * cdc_threads;
  DLOG(INFO) << "logs_per_cdc_thread: " << logs_per_cdc_thread
             << ", rest_logs: " << rest_logs;

  uint32_t normal_workers[config::worker_threads - 1];
  uint32_t j = 0;
  uint32_t ddl_thread_id = -1;
  for (uint32_t i = 0; i < dlog::tlogs.size(); i++) {
    dlog::tls_log *tlog = dlog::tlogs[i];
    if (tlog && tlog != log && volatile_read(pcommit::_tls_durable_csn[i])) {
      normal_workers[j++] = i;
    } else if (tlog == log) {
      ddl_thread_id = i;
    }
  }

  int begin = 0, end = -1;
  for (uint32_t i = 0; i < cdc_threads; i++) {
    begin = end + 1;
    uint32_t begin_log = normal_workers[begin];
    uint32_t end_log;
    end = begin - 1 +
          (rest_logs-- > 0 ? logs_per_cdc_thread + 1 : logs_per_cdc_thread);
    if (i == cdc_threads - 1) {
      end_log = normal_workers[--j];
    } else {
      end_log = normal_workers[end];
    }
    DLOG(INFO) << i << ", " << begin << ", " << end;

    thread::Thread *thread =
        thread::GetThread(config::cdc_physical_workers_only);
    ALWAYS_ASSERT(thread);
    cdc_workers.push_back(thread);

    uint32_t count = 0;
    for (uint32_t i = begin_log; i <= end_log; i++) {
      dlog::tls_log *tlog = dlog::tlogs[i];
      uint64_t csn = volatile_read(pcommit::_tls_durable_csn[i]);
      if (tlog && csn && tlog != GetLog() && i != ddl_thread_id) {
        count++;
      }
    }

    if (i >= data_bufs.size()) {
      data_bufs.push_back(new std::vector<char *>);
      data_bufs[i]->reserve(1000);
    }

    auto parallel_changed_data_capture = [=](char *) {
      bool ddl_end_local = false;
      str_arena *arena = GetArena();
      arena->reset();
      rc_t rc;
      while (flags.cdc_running) {
        rc = changed_data_capture_impl(i, ddl_thread_id, begin_log, end_log,
                                       arena, &ddl_end_local, count);
        if (rc._val != RC_TRUE) {
          flags.ddl_failed = true;
          flags.cdc_running = false;
        }
        if (ddl_end_local && flags.cdc_second_phase) {
          break;
        }
      }
      flags.cdc_end_total.fetch_add(1);
    };

    thread->StartTask(parallel_changed_data_capture);
  }

  return cdc_threads;
}

rc_t ddl_executor::changed_data_capture_impl(
    uint32_t thread_id, uint32_t ddl_thread_id, uint32_t begin_log,
    uint32_t end_log, str_arena *arena, bool *ddl_end_local, uint32_t count) {
  RCU::rcu_enter();
  TXN::xid_context *xc = t->GetXIDContext();
  uint64_t begin_csn = xc->begin;
  uint64_t end_csn = xc->end;
  uint64_t block_sz = sizeof(dlog::log_block);
  for (uint32_t i = begin_log; i <= end_log; i++) {
    dlog::tls_log *tlog = dlog::tlogs[i];
    uint64_t csn = volatile_read(pcommit::_tls_durable_csn[i]);
    if (tlog && csn && tlog != GetLog() && i != ddl_thread_id) {
      bool re_check = false;
    double_check:
    uint32_t count_per_tlog = 0;
    retry_last_flush:
      if (!tlog->last_flush() && count_per_tlog++ < 2) {
        goto retry_last_flush;
      }
      std::vector<dlog::segment> *segments = tlog->get_segments();
      bool stop_log_scan = false;
      uint64_t offset_in_seg = volatile_read(flags._tls_durable_lsn[i]);
      uint64_t offset_increment = 0;
      int insert_total = 0, update_total = 0;
      int insert_fail = 0, update_fail = 0;
      for (std::vector<dlog::segment>::reverse_iterator seg = segments->rbegin();
           seg != segments->rend(); seg++) {
        uint64_t data_sz = seg->size;
        char *data_buf = nullptr;
        if (data_sz > offset_in_seg) {
          data_buf = (char *)RCU::rcu_alloc(data_sz - offset_in_seg);
          size_t m = os_pread(seg->fd, (char *)data_buf, data_sz - offset_in_seg,
                              offset_in_seg);
        }

        while (offset_increment < data_sz - offset_in_seg &&
               (flags.cdc_running || flags.cdc_second_phase) &&
               !flags.ddl_failed) {
          dlog::log_block *header =
              (dlog::log_block *)(data_buf + offset_increment);
          if ((end_csn && begin_csn <= header->csn && header->csn <= end_csn) ||
              (!end_csn && begin_csn <= header->csn)) {
            uint64_t offset_in_block = 0;
            while (offset_in_block < header->payload_size &&
                   !flags.ddl_failed) {
              dlog::log_record *logrec =
                  (dlog::log_record *)(data_buf + offset_increment + block_sz +
                                       offset_in_block);

              ALWAYS_ASSERT(header->csn == logrec->csn);

              FID f = logrec->fid;
              OID o = logrec->oid;

              if (!old_td_map[f]) {
                offset_in_block += logrec->rec_size;
                continue;
              }

              auto *key_array = old_td_map[f]->GetKeyArray();
              fat_ptr *entry =
                  config::enable_ddl_keys ? key_array->get(o) : nullptr;
              varstr *key = entry ? (varstr *)((*entry).offset()) : nullptr;

              if (!key && config::enable_ddl_keys) {
                offset_in_block += logrec->rec_size;
                continue;
              }

              if (logrec->type == dlog::log_record::logrec_type::INSERT) {
                dbtuple *tuple = (dbtuple *)(logrec->data);
                varstr tuple_value(tuple->get_value_start(), tuple->size);

                for (auto &param : ddl_executor_paras_list) {
                  if (!param->handle_insert ||
                      param->old_td->GetTupleFid() != f) {
                    continue;
                  }
		  arena->reset();
                  if (param->type == VERIFICATION_ONLY ||
                      param->type == COPY_VERIFICATION) {
                    if (!constraints[param->constraint_idx](key, tuple_value,
                                                            arena, param->new_v,
                                                            logrec->csn)) {
                      return rc_t{RC_ABORT_INTERNAL};
                    }
                  }
                  if (param->type == COPY_ONLY ||
                      param->type == COPY_VERIFICATION) {
                    varstr *new_value = reformats[param->reformat_idx](
                        key, tuple_value, arena, param->new_v, f, o, t, logrec->begin, true);

                    insert_total++;
                    if (!new_value) {
                      continue;
                    }
                    if (t->DDLInsert(param->new_td, o, new_value, logrec->csn)
                            ._val != RC_TRUE) {
                      insert_fail++;
                    }
                  }
                }
              } else if (logrec->type ==
                         dlog::log_record::logrec_type::UPDATE) {
                dbtuple *tuple = (dbtuple *)(logrec->data);
                varstr tuple_value(tuple->get_value_start(), tuple->size);

                for (auto &param : ddl_executor_paras_list) {
                  if (!param->handle_update ||
                      param->old_td->GetTupleFid() != f) {
                    continue;
                  }
		  arena->reset();
                  if (param->type == VERIFICATION_ONLY ||
                      param->type == COPY_VERIFICATION) {
                    if (!constraints[param->constraint_idx](key, tuple_value,
                                                            arena, param->new_v,
                                                            logrec->csn)) {
                      return rc_t{RC_ABORT_INTERNAL};
                    }
                  }
                  if (param->type == COPY_ONLY ||
                      param->type == COPY_VERIFICATION) {
                    varstr *new_value = reformats[param->reformat_idx](
                        key, tuple_value, arena, param->new_v, f, o, t, logrec->begin, false);

                    update_total++;
                    if (!new_value) {
                      continue;
                    }
                    if (t->DDLUpdate(param->new_td, o, new_value, logrec->csn)
                            ._val != RC_TRUE) {
                      update_fail++;
                    }
                  }
                }
              }

              offset_in_block += logrec->rec_size;
            }
          } else {
            if (end_csn && header->csn > end_csn) {
              stop_log_scan = true;
              break;
            }
          }
          offset_increment += header->total_size();
          volatile_write(flags._tls_durable_lsn[i],
                         offset_in_seg + offset_increment);
        }
        if (data_buf && data_sz > offset_in_seg) {
          data_bufs[thread_id]->push_back(data_buf);
        }
	if (stop_log_scan || (flags.cdc_second_phase && insert_total == 0 &&
                              update_total == 0)) {
          if (!stop_log_scan && !re_check) {
            re_check = true;
            goto double_check;
          }
          count--;
        }
      }
    }
    if ((!flags.cdc_running && !flags.cdc_second_phase) || flags.ddl_failed) {
      break;
    }
  }
  RCU::rcu_exit();
  *ddl_end_local = (count == 0);
  return rc_t{RC_TRUE};
}
#endif

#if defined(SIDDL) || defined(BLOCKDDL)
void ddl_executor::init_ddl_write_set() {
  ddl_write_set = new ddl_write_set_t();
  ddl_write_set->init();
}

void ddl_executor::free_ddl_write_set() {
  delete ddl_write_set;
}

void ddl_executor::ddl_write_set_commit(dlog::log_block *lb, uint64_t *lb_lsn,
                                        uint64_t *segnum) {
  TXN::xid_context *xc = t->GetXIDContext();
  dlog::tls_log *log = t->get_log();
  uint64_t max_log_size = log->get_logbuf_size() - sizeof(dlog::log_block);
  for (std::vector<write_record_block_info *>::const_iterator it =
           ddl_write_set->write_record_block_info_vec.begin();
       it != ddl_write_set->write_record_block_info_vec.end(); ++it) {
    write_record_block *cur = (*it)->first_block;
    while (cur) {
      uint32_t current_log_size = 0;
      for (uint32_t i = 0; i < cur->size(); ++i) {
        auto &w = (*cur)[i];
        Object *object = w.get_object();
        dbtuple *tuple = (dbtuple *)object->GetPayload();

        uint64_t log_tuple_size = w.size;

        // Populate log block and obtain persistent address
        uint32_t off = lb->payload_size;
        if (lb->payload_size +
                align_up(log_tuple_size + sizeof(dlog::log_record)) >
            lb->capacity) {
          lb = log->allocate_log_block(
              std::min<uint64_t>((*it)->log_size - current_log_size,
                                 max_log_size),
              lb_lsn, segnum, xc->end);
          off = lb->payload_size;
        }
        if (w.is_insert) {
          auto ret_off =
              dlog::log_insert(lb, w.fid, w.oid, (char *)tuple, log_tuple_size);
          ALWAYS_ASSERT(ret_off == off);
        } else {
          auto ret_off =
              dlog::log_update(lb, w.fid, w.oid, (char *)tuple, log_tuple_size);
          ALWAYS_ASSERT(ret_off == off);
        }
        ALWAYS_ASSERT(lb->payload_size <= lb->capacity);
        current_log_size += align_up(log_tuple_size + sizeof(dlog::log_record));

        // This aligned_size should match what was calculated during
        // add_to_write_set, and the size_code calculated based on this aligned
        // size will be part of the persistent address, which a read can
        // directly use to load the log record from the log (i.e., knowing how
        // many bytes to read to obtain the log record header + dbtuple header +
        // record data).
        auto aligned_size = align_up(w.size + sizeof(dlog::log_record));
        auto size_code = encode_size_aligned(aligned_size);

        // lb_lsn points to the start of the log block which has a header,
        // followed by individual log records, so the log record's direct
        // address would be lb_lsn + sizeof(log_block) + off
        fat_ptr pdest =
            LSN::make(log->get_id(), *lb_lsn + sizeof(dlog::log_block) + off,
                      *segnum, size_code)
                .to_ptr();
        object->SetPersistentAddress(pdest);
        ASSERT(object->GetPersistentAddress().asi_type() == fat_ptr::ASI_LOG);

        // Set CSN
        fat_ptr csn_ptr = object->GenerateCsnPtr(xc->end);
        object->SetCSN(csn_ptr);
        ASSERT(tuple->GetObject()->GetCSN().asi_type() == fat_ptr::ASI_CSN);
      }
      cur = cur->next;
    }
  }
}

void ddl_executor::ddl_write_set_abort() {
  TXN::xid_context *xc = t->GetXIDContext();
  for (std::vector<write_record_block_info *>::const_iterator it =
           ddl_write_set->write_record_block_info_vec.begin();
       it != ddl_write_set->write_record_block_info_vec.end(); ++it) {
    write_record_block *cur = (*it)->first_block;
    while (cur) {
      for (uint32_t i = 0; i < cur->size(); ++i) {
        auto &w = (*cur)[i];
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
#ifdef SIDDL
	if (obj) {
#endif
          obj->SetCSN(NULL_PTR);
          oidmgr->UnlinkTuple(w.entry);
          ASSERT(obj->GetAllocateEpoch() == xc->begin_epoch);
          MM::deallocate(entry);
#ifdef SIDDL
	}
#endif
      }
      cur = cur->next;
    }
  }
}
#endif

rc_t ddl_executor::commit_op(dlog::log_block *lb, uint64_t *lb_lsn,
                             uint64_t *segnum) {
  TXN::xid_context *xc = t->GetXIDContext();
  dlog::tls_log *log = t->get_log();
  write_set_t &write_set = t->get_write_set();

  DLOG(INFO) << "DDL txn end: " << xc->end;
  // If txn is DDL, commit schema record(s) first (before CDC), no logging
  // DMLs can proceed conditionally
  set_schema_state(lb, lb_lsn, segnum, schema_state_type::NOT_READY);
  DLOG(INFO) << "DDL schema commit with size " << write_set.size();

#ifdef COPYDDL
  if (dt != ddl_type::NO_COPY_VERIFICATION) {
    for (auto &v : new_td_map) {
      // Fix new table file's marks
      auto *alloc = oidmgr->get_allocator(old_td->GetTupleFid());
      uint32_t himark = alloc->head.hiwater_mark;
      auto *new_tuple_array = v.second->GetTupleArray();
      new_tuple_array->ensure_size(himark);
      oidmgr->recreate_allocator(v.second->GetTupleFid(), himark);
      auto *new_alloc = oidmgr->get_allocator(v.second->GetTupleFid());
      himark = new_alloc->head.hiwater_mark;

      // Add new table descriptor to fid_map
      Catalog::fid_map[v.second->GetTupleFid()] = v.second;

#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
      // Set td status to be ready
      v.second->SetReady(true);
#else
      // Switch table descriptor for primary index
      OrderedIndex *index = v.second->GetPrimaryIndex();
      index->SetTableDescriptor(v.second);
      index->SetArrays(true);

      // Switch table descriptor for secondary index(es)
      std::vector<OrderedIndex *> sec_indexes = v.second->GetSecIndexes();
      for (std::vector<OrderedIndex *>::const_iterator it = sec_indexes.begin();
           it != sec_indexes.end(); ++it) {
        (*it)->SetTableDescriptor(v.second);
        (*it)->SetArrays(false);
      }

      // Set td status to be ready
      v.second->SetReady(true);
#endif
    }
  }
#endif

#if defined(COPYDDL) && !defined(LAZYDDL)
  if (dt != ddl_type::NO_COPY_VERIFICATION) {
    // Start the second round of CDC
    DLOG(INFO) << "Second CDC begins";
    flags.cdc_second_phase = true;
    flags.cdc_end_total.store(0);
    uint32_t cdc_threads = changed_data_capture();
    if (config::enable_late_scan_join) {
      join_scan_workers();
    }
    while (flags.cdc_end_total.load() != cdc_threads && !flags.ddl_failed) {
    }
    flags.cdc_running = false;
    join_cdc_workers();
    flags.cdc_second_phase = false;
    DLOG(INFO) << "Second CDC ends";
    if (flags.ddl_failed) {
      DLOG(INFO) << "DDL failed";
      for (auto &v : new_td_map) {
        OrderedIndex *index = v.second->GetPrimaryIndex();
        index->SetTableDescriptor(old_td);
        index->SetArrays(true);
        v.second->GetTupleArray()->destroy(v.second->GetTupleArray());
      }
      return rc_t{RC_ABORT_INTERNAL};
    }
  }
#endif

  // Set states of schema records to be READY, logging enabled,
  // DMLs can proceed without conditions
  set_schema_state(lb, lb_lsn, segnum, schema_state_type::READY);

#if defined(COPYDDL) && !defined(LAZYDDL)
  if (dt != ddl_type::NO_COPY_VERIFICATION) {
    free_data_bufs();
  }
#endif

#if defined(SIDDL) || defined(BLOCKDDL)
  ddl_write_set_commit(lb, lb_lsn, segnum);
#endif

  volatile_write(xc->state, TXN::TXN_CMMTD);
#ifdef COPYDDL
#ifdef LAZYDDL
  if (!config::enable_lazy_background) {
    set_schema_state(lb, lb_lsn, segnum, schema_state_type::COMPLETE, true);
    log->set_dirty(false);
    log->set_doing_ddl(false);
    return rc_t{RC_TRUE};
  }
  // Let DMLs do some work first
  if (config::late_background_start_ms) {
    usleep(config::late_background_start_ms * 1000);
  }
  // Background migration
  rc_t rc = scan(&(t->string_allocator()));
  if (rc.IsAbort()) {
    log->set_dirty(false);
    log->set_doing_ddl(false);
    return rc;
  }
#endif

  // Traverse new table arrays to do logging
  for (auto &v : new_td_map) {
    FID fid = v.second->GetTupleFid();
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
        log->resize_logbuf(2);
        dlog::log_block *lb = nullptr;
        dlog::tlog_lsn lb_lsn = dlog::INVALID_TLOG_LSN;
        uint64_t segnum = -1;
        uint64_t max_log_size =
            log->get_logbuf_size() - sizeof(dlog::log_block);

        lb = log->allocate_log_block(max_log_size, &lb_lsn, &segnum, xc->end);
        for (uint32_t oid = 0; oid <= himark; ++oid) {
          if (oid % ddl_log_threads != i) continue;
          //for (uint32_t oid = begin; oid <= end; ++oid) {
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
                lb = log->allocate_log_block(max_log_size, &lb_lsn, &segnum,
                                             xc->end);
                off = lb->payload_size;
              }
              auto ret_off =
                  dlog::log_insert(lb, fid, oid, (char *)tuple, log_tuple_size);
              ALWAYS_ASSERT(ret_off == off);

              auto aligned_size =
                  align_up(log_tuple_size + sizeof(dlog::log_record));
              auto size_code = encode_size_aligned(aligned_size);

              fat_ptr pdest = LSN::make(log->get_id(),
                                        lb_lsn + sizeof(dlog::log_block) + off,
                                        segnum, size_code)
                                  .to_ptr();
              cur_obj->SetPersistentAddress(pdest);
              ASSERT(cur_obj->GetPersistentAddress().asi_type() ==
                     fat_ptr::ASI_LOG);

              break;
            }
            ptr = tentative_next;
          }
        }
	log->last_flush();
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
#endif
  // Set states of schema records to be COMPLETE, logging enabled,
  // other DDLs can proceed without conditions
  set_schema_state(lb, lb_lsn, segnum, schema_state_type::COMPLETE, true);
  log->set_dirty(false);
  log->set_doing_ddl(false);
  return rc_t{RC_TRUE};
}

void ddl_executor::set_schema_state(dlog::log_block *lb, uint64_t *lb_lsn, uint64_t *segnum, schema_state_type state, bool set_csn) {
  TXN::xid_context *xc = t->GetXIDContext();
  dlog::tls_log *log = t->get_log();
  write_set_t &write_set = t->get_write_set();
  uint64_t log_size = t->get_log_size();
  uint64_t max_log_size = log->get_logbuf_size() - sizeof(dlog::log_block);
  uint32_t current_log_size = 0;

  for (uint32_t i = 0; i < write_set.size(); ++i) {
    auto &w = write_set[i];
    Object *object = w.get_object();
    dbtuple *tuple = (dbtuple *)object->GetPayload();

    if (state != schema_state_type::NOT_READY) {
      varstr value(tuple->get_value_start(), tuple->size);
      schema_kv::value schema_value_temp;
      const schema_kv::value *old_schema = Decode(value, schema_value_temp);
      schema_kv::value new_schema(*old_schema);
      new_schema.state = state;
      new_schema.csn = xc->end;

      t->string_allocator().reset();
      varstr *new_value = t->string_allocator().next(Size(new_schema));

      rc_t rc = t->SetSchemaState(schema_td, w.oid, &Encode(*new_value, new_schema), set_csn);
      ALWAYS_ASSERT(rc._val == RC_TRUE);

      object = w.get_object();
      tuple = (dbtuple *)object->GetPayload();

      // Populate log block and obtain persistent address
      uint32_t off = lb->payload_size;
      if (lb->payload_size + align_up(w.size + sizeof(dlog::log_record)) > lb->capacity) {
        lb = log->allocate_log_block(
            std::min<uint64_t>(log_size - current_log_size, max_log_size), lb_lsn, segnum, xc->end);
        off = lb->payload_size;
      }
      if (w.is_insert) {
        auto ret_off = dlog::log_insert(lb, w.fid, w.oid, (char *)tuple, w.size);
        ALWAYS_ASSERT(ret_off == off);
      } else {
        auto ret_off = dlog::log_update(lb, w.fid, w.oid, (char *)tuple, w.size);
        ALWAYS_ASSERT(ret_off == off);
      }
      ALWAYS_ASSERT(lb->payload_size <= lb->capacity);
      current_log_size += align_up(w.size + sizeof(dlog::log_record));

      // This aligned_size should match what was calculated during
      // add_to_write_set, and the size_code calculated based on this aligned
      // size will be part of the persistent address, which a read can
      // directly use to load the log record from the log (i.e., knowing how
      // many bytes to read to obtain the log record header + dbtuple header +
      // record data).
      auto aligned_size = align_up(w.size + sizeof(dlog::log_record));
      auto size_code = encode_size_aligned(aligned_size);

      // lb_lsn points to the start of the log block which has a header,
      // followed by individual log records, so the log record's direct
      // address would be lb_lsn + sizeof(log_block) + off
      fat_ptr pdest =
          LSN::make(log->get_id(), *lb_lsn + sizeof(dlog::log_block) + off,
                    *segnum, size_code)
              .to_ptr();
      object->SetPersistentAddress(pdest);
      ASSERT(object->GetPersistentAddress().asi_type() == fat_ptr::ASI_LOG);
    }

    // Set CSN
    if (!set_csn) {
      fat_ptr csn_ptr = object->GenerateCsnPtr(xc->end);
      object->SetCSN(csn_ptr);
      ASSERT(tuple->GetObject()->GetCSN().asi_type() == fat_ptr::ASI_CSN);
    }
  }
}

}  // namespace ddl

}  // namespace ermia
