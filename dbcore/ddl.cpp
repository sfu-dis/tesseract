#include "ddl.h"
#include "../benchmarks/oddlb-schemas.h"
#include "../benchmarks/tpcc.h"
#include "../schema.h"

namespace ermia {

namespace ddl {

std::vector<Reformat> reformats;
std::vector<Constraint> constraints;
std::vector<uint32_t> ddl_worker_logical_threads;

ddl_type ddl_type_map(uint32_t type) {
  switch (type) {
  case 1:
    return COPY_ONLY;
  case 2:
    return VERIFICATION_ONLY;
  case 3:
    return COPY_VERIFICATION;
  case 4:
    return NO_COPY_VERIFICATION;
  default:
    LOG(FATAL) << "Not supported";
  }
}

rc_t ddl_executor::scan(transaction *t, str_arena *arena, varstr &value) {
#if defined(COPYDDL) && !defined(LAZYDDL)
  DLOG(INFO) << "First CDC begins";
  cdc_workers = t->changed_data_capture();
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

#ifdef LAZYDDL
  uint32_t scan_threads = config::scan_threads + config::cdc_threads;
#else
  uint32_t scan_threads = config::scan_threads;
#endif
  uint32_t num_per_scan_thread = himark / scan_threads;
  DLOG(INFO) << "scan_threads: " << scan_threads
             << ", num_per_scan_thread: " << num_per_scan_thread;

  for (uint32_t i = 1; i < scan_threads; i++) {
    uint32_t begin = i * num_per_scan_thread;
    uint32_t end = (i + 1) * num_per_scan_thread;
    if (i == scan_threads - 1)
      end = himark;
  retry:
    thread::Thread *thread = nullptr;
    if (config::enable_ddl_offloading) {
      thread = thread::GetThread(ermia::config::numa_nodes - 1,
                                 config::scan_physical_workers_only);
    } else {
      thread = thread::GetThread(config::scan_physical_workers_only);
    }
    ALWAYS_ASSERT(thread);
    scan_workers.push_back(thread);
    auto parallel_scan = [=](char *) {
      dlog::log_block *lb = nullptr;
      str_arena *arena = new str_arena(config::arena_size_mb);
      for (uint32_t oid = begin + 1; oid <= end; oid++) {
        dbtuple *tuple =
            AWAIT oidmgr->oid_get_version(old_tuple_array, oid, xc);
        varstr tuple_value;
        fat_ptr *entry =
            config::enable_ddl_keys ? key_array->get(oid) : nullptr;
        varstr *key = entry ? (varstr *)((*entry).offset()) : nullptr;
        if (tuple && t->DoTupleRead(tuple, &tuple_value)._val == RC_TRUE) {
          for (std::vector<struct ddl_executor_paras *>::const_iterator it =
                   ddl_executor_paras_list.begin();
               it != ddl_executor_paras_list.end(); ++it) {
            if ((*it)->old_td != old_td)
              continue;
            if ((*it)->type == VERIFICATION_ONLY ||
                (*it)->type == COPY_VERIFICATION) {
              if (!constraints[(*it)->constraint_idx](tuple_value,
                                                      (*it)->new_v)) {
                DLOG(INFO) << "DDL failed";
                return rc_t{RC_ABORT_INTERNAL};
              }
            }
            if ((*it)->type == COPY_ONLY || (*it)->type == COPY_VERIFICATION) {
              arena->reset();
              uint64_t reformat_idx = (*it)->scan_reformat_idx == -1
                                          ? (*it)->reformat_idx
                                          : (*it)->scan_reformat_idx;
              varstr *new_tuple_value = reformats[reformat_idx](
                  key, tuple_value, arena, (*it)->new_v, fid, oid);
              if (!new_tuple_value)
                continue;
#ifdef COPYDDL
#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
              fat_ptr *out_entry = nullptr;
              OID o = t->DDLInsert((*it)->new_td, new_tuple_value, &out_entry,
                                   (*it)->new_v);
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
                ConcurrentMasstreeIndex *secondary_index =
                    (ConcurrentMasstreeIndex
                         *)((*it)->new_td->GetSecIndexes().front());
                if ((*it)->new_td->GetSecIndexes().size()) {
                  varstr *new_secondary_index_key =
                      reformats[(*it)->secondary_index_key_create_idx](
                          key, tuple_value, arena, (*it)->new_v, fid, oid);
                  if (!AWAIT secondary_index->InsertOID(
                          t, *new_secondary_index_key, o)) {
                    continue;
                  }
                }
              }
#elif OPTLAZYDDL
              t->DDLCDCInsert((*it)->new_td, oid, new_tuple_value, xc->end, lb,
                              (*it)->new_v);
#else
              t->DDLCDCInsert((*it)->new_td, oid, new_tuple_value,
                              !xc->end ? xc->begin : xc->end, lb, (*it)->new_v);
#endif
#elif BLOCKDDL
              t->DDLScanUpdate((*it)->new_td, oid, new_tuple_value, lb,
                               (*it)->new_v);
#elif SIDDL
              rc_t r = t->Update((*it)->new_td, oid, nullptr, new_tuple_value,
                                 (*it)->new_v);
              if (r._val != RC_TRUE) {
                return rc_t{RC_ABORT_INTERNAL};
              }
#endif
            }
          }
        }
      }
      return rc_t{RC_TRUE};
    };
    thread->StartTask(parallel_scan);
  }

  OID end = scan_threads == 1 ? himark : num_per_scan_thread;
  for (OID oid = 0; oid <= end; oid++) {
    dbtuple *tuple = AWAIT oidmgr->oid_get_version(old_tuple_array, oid, xc);
    varstr tuple_value;
    fat_ptr *entry = config::enable_ddl_keys ? key_array->get(oid) : nullptr;
    varstr *key = entry ? (varstr *)((*entry).offset()) : nullptr;
    if (tuple && t->DoTupleRead(tuple, &tuple_value)._val == RC_TRUE) {
      for (std::vector<struct ddl_executor_paras *>::const_iterator it =
               ddl_executor_paras_list.begin();
           it != ddl_executor_paras_list.end(); ++it) {
        if ((*it)->old_td != old_td)
          continue;
        if ((*it)->type == VERIFICATION_ONLY ||
            (*it)->type == COPY_VERIFICATION) {
          if (!constraints[(*it)->constraint_idx](tuple_value, (*it)->new_v)) {
            DLOG(INFO) << "DDL failed";
            return rc_t{RC_ABORT_INTERNAL};
          }
        }
        if ((*it)->type == COPY_ONLY || (*it)->type == COPY_VERIFICATION) {
          arena->reset();
          uint64_t reformat_idx = (*it)->scan_reformat_idx == -1
                                      ? (*it)->reformat_idx
                                      : (*it)->scan_reformat_idx;
          varstr *new_tuple_value = reformats[reformat_idx](
              key, tuple_value, arena, (*it)->new_v, fid, oid);
          if (!new_tuple_value)
            continue;
#ifdef COPYDDL
#if defined(LAZYDDL) && !defined(OPTLAZYDDL)
          fat_ptr *out_entry = nullptr;
          OID o = t->DDLInsert((*it)->new_td, new_tuple_value, &out_entry,
                               (*it)->new_v);
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
            ConcurrentMasstreeIndex *secondary_index =
                (ConcurrentMasstreeIndex
                     *)((*it)->new_td->GetSecIndexes().front());
            if ((*it)->new_td->GetSecIndexes().size()) {
              varstr *new_secondary_index_key =
                  reformats[(*it)->secondary_index_key_create_idx](
                      key, tuple_value, arena, (*it)->new_v, fid, oid);
              if (!AWAIT secondary_index->InsertOID(t, *new_secondary_index_key,
                                                    o)) {
                continue;
              }
            }
          }
#elif OPTLAZYDDL
          t->DDLCDCInsert((*it)->new_td, oid, new_tuple_value, xc->end, nullptr,
                          (*it)->new_v);
#else
          t->DDLCDCInsert((*it)->new_td, oid, new_tuple_value,
                          !xc->end ? xc->begin : xc->end, nullptr,
                          (*it)->new_v);
#endif
#elif BLOCKDDL
          t->DDLScanUpdate((*it)->new_td, oid, new_tuple_value, nullptr,
                           (*it)->new_v);
#elif SIDDL
          rc_t r = t->Update((*it)->new_td, oid, nullptr, new_tuple_value,
                             (*it)->new_v);
          if (r._val != RC_TRUE) {
            return rc_t{RC_ABORT_INTERNAL};
          }
#endif
        }
      }
    }
  }

  if (!config::enable_late_scan_join) {
    join_scan_workers();
  }

#if defined(COPYDDL) && !defined(LAZYDDL)
  t->join_changed_data_capture_threads(cdc_workers);
  uint64_t current_csn = dlog::current_csn.load(std::memory_order_relaxed);
  if (ddl_failed) {
    DLOG(INFO) << "DDL failed";
    for (std::vector<struct ddl_executor_paras *>::const_iterator it =
             ddl_executor_paras_list.begin();
         it != ddl_executor_paras_list.end(); ++it) {
      (*it)->new_td->GetTupleArray()->destroy((*it)->new_td->GetTupleArray());
      return rc_t{RC_ABORT_INTERNAL};
    }
  }
  DLOG(INFO) << "First CDC ends, now go to grab a t4";
  if (config::enable_cdc_verification_test) {
    cdc_test = true;
    usleep(100);
  }
  ddl_td_set = false;
#endif

  return rc_t{RC_TRUE};
}

rc_t ddl_executor::changed_data_capture_impl(transaction *t, uint32_t thread_id,
                                             uint32_t ddl_thread_id,
                                             uint32_t begin_log,
                                             uint32_t end_log, str_arena *arena,
                                             bool *ddl_end) {
  RCU::rcu_enter();
  TXN::xid_context *xc = t->GetXIDContext();
  uint64_t begin_csn = xc->begin;
  uint64_t end_csn = xc->end;
  std::unordered_map<FID, TableDescriptor *> old_td_map = t->get_old_td_map();
  uint32_t count = 0;
  for (uint32_t i = begin_log; i <= end_log; i++) {
    dlog::tls_log *tlog = dlog::tlogs[i];
    uint64_t csn = volatile_read(pcommit::_tls_durable_csn[i]);
    if (tlog && csn && tlog != GetLog() && i != ddl_thread_id) {
      count++;
    }
  }
  uint64_t block_sz = sizeof(dlog::log_block),
           logrec_sz = sizeof(dlog::log_record), tuple_sc = sizeof(dbtuple);
  for (uint32_t i = begin_log; i <= end_log; i++) {
    dlog::tls_log *tlog = dlog::tlogs[i];
    uint64_t csn = volatile_read(pcommit::_tls_durable_csn[i]);
    if (tlog && csn && tlog != GetLog() && i != ddl_thread_id) {
      tlog->cdc_flush();
      std::vector<dlog::segment> *segments = tlog->get_segments();
      bool stop_scan = false;
      uint64_t offset_in_seg = volatile_read(_tls_durable_lsn[i]);
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
               (cdc_running || ddl_running_2) && !ddl_failed) {
          dlog::log_block *header =
              (dlog::log_block *)(data_buf + offset_increment);
          if ((end_csn && begin_csn <= header->csn && header->csn <= end_csn) ||
              (!end_csn && begin_csn <= header->csn)) {
            last_csn = header->csn;
            volatile_write(_cdc_last_csn[i], last_csn);
            uint64_t offset_in_block = 0;
            varstr *insert_key, *update_key, *insert_key_idx;
            while (offset_in_block < header->payload_size && !ddl_failed) {
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
                    if (t->DDLCDCInsert((*it)->new_td, o, new_value,
                                        logrec->csn, nullptr, (*it)->new_v)
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
                    if (t->DDLCDCUpdate((*it)->new_td, o, new_value,
                                        logrec->csn, nullptr, (*it)->new_v)
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
          volatile_write(_tls_durable_lsn[i], offset_in_seg + offset_increment);
        }
        RCU::rcu_free(data_buf);
        if (stop_scan ||
            (ddl_running_2 && insert_total == 0 && update_total == 0)) {
          count--;
        }
      }
    }
    if ((!cdc_running && !ddl_running_2) || ddl_failed) {
      break;
    }
  }
  RCU::rcu_exit();
  *ddl_end = (count == 0);
  return rc_t{RC_TRUE};
}

rc_t ddl_executor::build_map(transaction *t, str_arena *arena,
                             TableDescriptor *td) {
  rc_t r;
  TXN::xid_context *xc = t->GetXIDContext();

  uint64_t count = 0;
  FID fid = td->GetTupleFid();
  auto *alloc = oidmgr->get_allocator(fid);
  uint32_t himark = alloc->head.hiwater_mark;
  auto *tuple_array = td->GetTupleArray();
  auto *key_array = td->GetKeyArray();

  for (OID oid = 0; oid <= himark; oid++) {
    fat_ptr *entry = key_array->get(oid);
    dbtuple *tuple = AWAIT oidmgr->oid_get_version(tuple_array, oid, xc);
    varstr tuple_value;
    varstr *key = entry ? (varstr *)((*entry).offset()) : nullptr;
    if (tuple && t->DoTupleRead(tuple, &tuple_value)._val == RC_TRUE) {
      for (std::vector<struct ddl_executor_paras *>::const_iterator it =
               ddl_executor_paras_list.begin();
           it != ddl_executor_paras_list.end(); ++it) {
        if ((*it)->old_td == td) {
          reformats[(*it)->reformat_idx](key, tuple_value, arena, (*it)->new_v,
                                         fid, oid);
        }
      }
    }
  }

  return rc_t{RC_TRUE};
  ;
}

} // namespace ddl

} // namespace ermia
