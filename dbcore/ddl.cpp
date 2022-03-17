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
std::vector<ddl_flags_wrapper *> ddl_flags_set;
mcs_lock lock;

ddl_flags *get_ddl_flags(OID oid, uint32_t version) {
  for (std::vector<ddl_flags_wrapper *>::const_iterator it =
           ddl_flags_set.begin();
       it != ddl_flags_set.end(); ++it) {
    if ((*it)->oid == oid && (*it)->version == version) {
      return (*it)->flags;
    }
  }
  return nullptr;
}

void ddl_executor::add_ddl_flags(OID oid, uint32_t version) {
  CRITICAL_SECTION(cs, lock);
  ddl_flags_set.push_back(new ddl_flags_wrapper(oid, version, flags));
}

rc_t ddl_executor::scan(transaction *t, str_arena *arena) {
#if defined(COPYDDL) && !defined(LAZYDDL)
  if (config::enable_parallel_scan_cdc) {
    DLOG(INFO) << "First CDC begins";
    flags->cdc_first_phase = true;
    changed_data_capture(t);
  }
#endif

  rc_t r;
  TXN::xid_context *xc = t->GetXIDContext();

  DLOG(INFO) << "DDL scan begins";
  uint64_t count = 0;
  TableDescriptor *old_td = t->get_old_td();
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
  DLOG(INFO) << "scan_threads: " << scan_threads
             << ", total_per_scan_thread: " << total_per_scan_thread;

  for (uint32_t i = 1; i < scan_threads; i++) {
    uint32_t begin = i * total_per_scan_thread;
    uint32_t end = (i + 1) * total_per_scan_thread;
    if (i == scan_threads - 1) {
      end = himark;
    }
    thread::Thread *thread =
        thread::GetThread(config::scan_physical_workers_only);
    ALWAYS_ASSERT(thread);
    scan_workers.push_back(thread);
    auto parallel_scan = [=](char *) {
      rc_t r;
      dlog::log_block *lb = nullptr;
      str_arena *arena = new str_arena(config::arena_size_mb);
      for (uint32_t oid = begin + 1; oid <= end; oid++) {
        // for (uint32_t oid = 0; oid <= himark; oid++) {
        //  if (oid % scan_threads != i) continue;
        r = scan_impl(t, arena, oid, fid, xc, old_tuple_array, key_array, lb,
                      i);
        if (r._val != RC_TRUE || flags->ddl_failed) {
          break;
        }
      }
    };
    thread->StartTask(parallel_scan);
  }

  dlog::log_block *lb = nullptr;
  OID end = scan_threads == 1 ? himark : total_per_scan_thread;
  for (OID oid = 0; oid <= end; oid++) {
    // for (uint32_t oid = 0; oid <= himark; oid++) {
    //  if (oid % scan_threads != 0) continue;
    r = scan_impl(t, arena, oid, fid, xc, old_tuple_array, key_array, lb, 0);
    if (r._val != RC_TRUE || flags->ddl_failed) {
      break;
    }
  }

  if (!config::enable_late_scan_join || flags->ddl_failed) {
    join_scan_workers();
  }

#if defined(COPYDDL) && !defined(LAZYDDL)
  if (config::enable_parallel_scan_cdc) {
    flags->cdc_running = false;
    join_cdc_workers();
  }
  if (flags->ddl_failed) {
    DLOG(INFO) << "DDL failed";
    join_cdc_workers();
    for (std::vector<struct ddl_executor_paras *>::const_iterator it =
             ddl_executor_paras_list.begin();
         it != ddl_executor_paras_list.end(); ++it) {
      if ((*it)->new_td != (*it)->old_td) {
        (*it)->new_td->GetTupleArray()->destroy((*it)->new_td->GetTupleArray());
      }
    }
    return rc_t{RC_ABORT_INTERNAL};
  }
  DLOG(INFO) << "First CDC ends, now go to grab a t4";
  if (config::enable_cdc_verification_test) {
    cdc_test = true;
  }
  flags->ddl_td_set = false;
  flags->cdc_first_phase = false;
#else
  if (flags->ddl_failed) {
    return rc_t{RC_ABORT_INTERNAL};
  }
#endif

  return rc_t{RC_TRUE};
}

rc_t ddl_executor::scan_impl(transaction *t, str_arena *arena, OID oid,
                             FID old_fid, TXN::xid_context *xc,
                             oid_array *old_tuple_array, oid_array *key_array,
                             dlog::log_block *lb, int wid) {
  dbtuple *tuple = AWAIT oidmgr->oid_get_version(old_tuple_array, oid, xc);
  varstr tuple_value;
  fat_ptr *entry = config::enable_ddl_keys ? key_array->get(oid) : nullptr;
  varstr *key = entry ? (varstr *)((*entry).offset()) : nullptr;
  if (tuple && t->DoTupleRead(tuple, &tuple_value)._val == RC_TRUE) {
    for (std::vector<struct ddl_executor_paras *>::const_iterator it =
             ddl_executor_paras_list.begin();
         it != ddl_executor_paras_list.end(); ++it) {
      if ((*it)->old_td->GetTupleFid() != old_fid) continue;
      if ((*it)->type == VERIFICATION_ONLY ||
          (*it)->type == COPY_VERIFICATION) {
        if (!constraints[(*it)->constraint_idx](tuple_value, (*it)->new_v)) {
          DLOG(INFO) << "DDL failed";
          flags->ddl_failed = true;
          flags->cdc_running = false;
          return rc_t{RC_ABORT_INTERNAL};
        }
      }
      if ((*it)->type == COPY_ONLY || (*it)->type == COPY_VERIFICATION) {
        arena->reset();
        uint64_t reformat_idx = (*it)->scan_reformat_idx == -1
                                    ? (*it)->reformat_idx
                                    : (*it)->scan_reformat_idx;
        varstr *new_tuple_value = reformats[reformat_idx](
            key, tuple_value, arena, (*it)->new_v, old_fid, oid);
        if (!new_tuple_value) continue;
#ifdef COPYDDL
#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
        fat_ptr *out_entry = nullptr;
        OID o = t->LazyDDLInsert((*it)->new_td, new_tuple_value, &out_entry);
        if (!o) {
          continue;
        }
        if (!AWAIT(*it)->index->InsertOID(t, *key, o)) {
          Object *obj = (Object *)out_entry->offset();
          fat_ptr entry = *out_entry;
          obj->SetCSN(NULL_PTR);
          ASSERT(obj->GetAllocateEpoch() == xc->begin_epoch);
          MM::deallocate(entry);
        } else {
          if ((*it)->new_td->GetSecIndexes().size()) {
            ConcurrentMasstreeIndex *secondary_index =
                (ConcurrentMasstreeIndex
                     *)((*it)->new_td->GetSecIndexes().front());
            varstr *new_secondary_index_key =
                reformats[(*it)->secondary_index_key_create_idx](
                    key, tuple_value, arena, (*it)->new_v, old_fid, oid);
            if (!AWAIT secondary_index->InsertOID(t, *new_secondary_index_key,
                                                  o)) {
              continue;
            }
          }
        }
#elif OPTLAZYDDL
        t->DDLInsert((*it)->new_td, oid, new_tuple_value, xc->end, lb);
#else
        t->DDLInsert((*it)->new_td, oid, new_tuple_value,
                     !xc->end ? xc->begin : xc->end, lb);
#endif
#elif BLOCKDDL
        rc_t r = t->Update((*it)->new_td, oid, nullptr, new_tuple_value, wid);
        ASSERT(r._val == RC_TRUE);
#elif SIDDL
        rc_t r = t->Update((*it)->new_td, oid, nullptr, new_tuple_value, wid);
        if (r._val != RC_TRUE) {
          DLOG(INFO) << "DDL failed";
          flags->ddl_failed = true;
          return rc_t{RC_ABORT_INTERNAL};
        }
#endif
      }
    }
  }
  return rc_t{RC_TRUE};
}

#if defined(COPYDDL) && !defined(LAZYDDL)
uint32_t ddl_executor::changed_data_capture(transaction *t) {
  flags->ddl_failed = false;
  flags->cdc_running = true;
  dlog::tls_log *log = t->get_log();
  uint32_t cdc_threads;
  if ((config::enable_parallel_scan_cdc && !flags->cdc_second_phase) ||
      (config::enable_late_scan_join && flags->cdc_second_phase)) {
    cdc_threads = config::cdc_threads;
  } else {
    cdc_threads = config::cdc_threads + config::scan_threads - 1;
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

    auto parallel_changed_data_capture = [=](char *) {
      bool ddl_end_local = false;
      str_arena *arena = new str_arena(config::arena_size_mb);
      rc_t rc;
      while (flags->cdc_running) {
        rc = changed_data_capture_impl(t, i, ddl_thread_id, begin_log, end_log,
                                       arena, &ddl_end_local, count);
        if (rc._val != RC_TRUE) {
          flags->ddl_failed = true;
          flags->cdc_running = false;
        }
        if (ddl_end_local && flags->cdc_second_phase) {
          break;
        }
      }
      flags->cdc_end_total.fetch_add(1);
    };

    thread->StartTask(parallel_changed_data_capture);
  }

  return cdc_threads;
}

rc_t ddl_executor::changed_data_capture_impl(transaction *t, uint32_t thread_id,
                                             uint32_t ddl_thread_id,
                                             uint32_t begin_log,
                                             uint32_t end_log, str_arena *arena,
                                             bool *ddl_end_local,
                                             uint32_t count) {
  RCU::rcu_enter();
  TXN::xid_context *xc = t->GetXIDContext();
  uint64_t begin_csn = xc->begin;
  uint64_t end_csn = xc->end;
  uint64_t block_sz = sizeof(dlog::log_block),
           logrec_sz = sizeof(dlog::log_record), tuple_sc = sizeof(dbtuple);
  for (uint32_t i = begin_log; i <= end_log; i++) {
    dlog::tls_log *tlog = dlog::tlogs[i];
    uint64_t csn = volatile_read(pcommit::_tls_durable_csn[i]);
    if (tlog && csn && tlog != GetLog() && i != ddl_thread_id) {
      bool re_check = false;
    double_check:
      tlog->last_flush();
      std::vector<dlog::segment> *segments = tlog->get_segments();
      bool stop_scan = false;
      uint64_t offset_in_seg = volatile_read(flags->_tls_durable_lsn[i]);
      uint64_t offset_increment = 0;
      uint64_t last_csn = 0;
      int insert_total = 0, update_total = 0;
      int insert_fail = 0, update_fail = 0;
      for (std::vector<dlog::segment>::reverse_iterator seg =
               segments->rbegin();
           seg != segments->rend(); seg++) {
        uint64_t data_sz = seg->size;
        char *data_buf = (char *)RCU::rcu_alloc(data_sz - offset_in_seg);
        size_t m = os_pread(seg->fd, (char *)data_buf, data_sz - offset_in_seg,
                            offset_in_seg);

        while (offset_increment < data_sz - offset_in_seg &&
               (flags->cdc_running || flags->cdc_second_phase) &&
               !flags->ddl_failed) {
          dlog::log_block *header =
              (dlog::log_block *)(data_buf + offset_increment);
          if ((end_csn && begin_csn <= header->csn && header->csn <= end_csn) ||
              (!end_csn && begin_csn <= header->csn)) {
            last_csn = header->csn;
            uint64_t offset_in_block = 0;
            while (offset_in_block < header->payload_size &&
                   !flags->ddl_failed) {
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

              if (logrec->type == dlog::log_record::logrec_type::INSERT) {
                dbtuple *tuple = (dbtuple *)(logrec->data);
                varstr tuple_value(tuple->get_value_start(), tuple->size);

                for (std::vector<struct ddl_executor_paras *>::const_iterator
                         it = ddl_executor_paras_list.begin();
                     it != ddl_executor_paras_list.end(); ++it) {
                  if (!(*it)->handle_insert ||
                      (*it)->old_td->GetTupleFid() != f) {
                    continue;
                  }
                  if ((*it)->type == VERIFICATION_ONLY ||
                      (*it)->type == COPY_VERIFICATION) {
                    if (!constraints[(*it)->constraint_idx](tuple_value,
                                                            (*it)->new_v)) {
                      return rc_t{RC_ABORT_INTERNAL};
                    }
                  }
                  if ((*it)->type == COPY_ONLY ||
                      (*it)->type == COPY_VERIFICATION) {
                    arena->reset();
                    varstr *new_value = reformats[(*it)->reformat_idx](
                        key, tuple_value, arena, (*it)->new_v, f, o);

                    insert_total++;
                    if (!new_value) {
                      continue;
                    }
                    if (t->DDLInsert((*it)->new_td, o, new_value, logrec->csn)
                            ._val != RC_TRUE) {
                      insert_fail++;
                    }
                  }
                }
              } else if (logrec->type ==
                         dlog::log_record::logrec_type::UPDATE) {
                dbtuple *tuple = (dbtuple *)(logrec->data);
                varstr tuple_value(tuple->get_value_start(), tuple->size);

                for (std::vector<struct ddl_executor_paras *>::const_iterator
                         it = ddl_executor_paras_list.begin();
                     it != ddl_executor_paras_list.end(); ++it) {
                  if (!(*it)->handle_update ||
                      (*it)->old_td->GetTupleFid() != f) {
                    continue;
                  }
                  if ((*it)->type == VERIFICATION_ONLY ||
                      (*it)->type == COPY_VERIFICATION) {
                    if (!constraints[(*it)->constraint_idx](tuple_value,
                                                            (*it)->new_v)) {
                      return rc_t{RC_ABORT_INTERNAL};
                    }
                  }
                  if ((*it)->type == COPY_ONLY ||
                      (*it)->type == COPY_VERIFICATION) {
                    arena->reset();
                    varstr *new_value = reformats[(*it)->reformat_idx](
                        key, tuple_value, arena, (*it)->new_v, f, o);

                    update_total++;
                    if (!new_value) {
                      continue;
                    }
                    if (t->DDLUpdate((*it)->new_td, o, new_value, logrec->csn)
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
              stop_scan = true;
              break;
            }
          }
          offset_increment += header->total_size();
          volatile_write(flags->_tls_durable_lsn[i],
                         offset_in_seg + offset_increment);
        }
        RCU::rcu_free(data_buf);
        if (stop_scan || (flags->cdc_second_phase && insert_total == 0 &&
                          update_total == 0)) {
          if (!re_check) {
            re_check = true;
            goto double_check;
          }
          count--;
        }
      }
    }
    if ((!flags->cdc_running && !flags->cdc_second_phase) ||
        flags->ddl_failed) {
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

void ddl_executor::ddl_write_set_commit(transaction *t, dlog::log_block *lb,
                                        uint64_t *lb_lsn, uint64_t *segnum) {
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

void ddl_executor::ddl_write_set_abort(transaction *t) {
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
        obj->SetCSN(NULL_PTR);
        oidmgr->UnlinkTuple(w.entry);
        ASSERT(obj->GetAllocateEpoch() == xc->begin_epoch);
        MM::deallocate(entry);
      }
      cur = cur->next;
    }
  }
}
#endif

}  // namespace ddl

}  // namespace ermia
