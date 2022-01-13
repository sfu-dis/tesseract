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
  rc_t r;
  TXN::xid_context *xc = t->GetXIDContext();

  printf("DDL scan begins\n");
  uint64_t count = 0;
  TableDescriptor *old_td = t->get_old_td();
  FID fid = old_td->GetTupleFid();
  auto *alloc = oidmgr->get_allocator(fid);
  uint32_t himark = alloc->head.hiwater_mark;
  auto *old_tuple_array = old_td->GetTupleArray();
  auto *key_array = old_td->GetKeyArray();
  printf("himark: %d\n", himark);

  // uint32_t scan_threads = (thread::cpu_cores.size() / 2) * config::numa_nodes
  // -
  //                         config::cdc_threads - config::worker_threads;
  uint32_t scan_threads = config::scan_threads;
  uint32_t num_per_scan_thread = himark / (scan_threads + 2);
  printf("scan_threads: %d, num_per_scan_thread: %d\n", scan_threads + 1,
         num_per_scan_thread);

  std::vector<thread::Thread *> scan_workers;
  for (uint32_t i = 1; i <= scan_threads; i++) {
    uint32_t begin = (i + 1) * num_per_scan_thread;
    uint32_t end = (i + 2) * num_per_scan_thread;
    if (i == scan_threads)
      end = himark;
  retry:
    thread::Thread *thread =
        thread::GetThread(config::scan_physical_workers_only);
    ALWAYS_ASSERT(thread);
    /*if (!config::scan_physical_workers_only) {
      for (auto &sib : ddl_worker_logical_threads) {
        if (thread->sys_cpu == sib) {
          thread::PutThread(thread);
          goto retry;
        }
      }
    }*/
    scan_workers.push_back(thread);
    auto parallel_scan = [=](char *) {
      str_arena *arena = new str_arena(config::arena_size_mb);
      for (uint32_t oid = begin + 1; oid <= end; oid++) {
        fat_ptr *entry = key_array->get(oid);
        dbtuple *tuple =
            AWAIT oidmgr->oid_get_version(old_tuple_array, oid, xc);
        varstr tuple_value;
        varstr *key = (config::enable_ddl_keys && entry)
                          ? (varstr *)((*entry).offset())
                          : nullptr;
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
                printf("DDL failed\n");
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
              t->DDLCDCInsert((*it)->new_td, oid, new_tuple_value, xc->begin,
                              nullptr);
#elif BLOCKDDL
              t->DDLScanUpdate((*it)->new_td, oid, new_tuple_value);
#elif SIDDL
              rc_t r = t->Update((*it)->new_td, oid, nullptr, new_tuple_value);
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

#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
  printf("First CDC begins\n");
  cdc_workers = t->changed_data_capture();
#endif

  OID end = scan_threads == 0 ? himark : 2 * num_per_scan_thread;
  for (OID oid = 0; oid <= end; oid++) {
    fat_ptr *entry = key_array->get(oid);
    dbtuple *tuple = AWAIT oidmgr->oid_get_version(old_tuple_array, oid, xc);
    varstr tuple_value;
    varstr *key = (config::enable_ddl_keys && entry)
                      ? (varstr *)((*entry).offset())
                      : nullptr;
    if (tuple && t->DoTupleRead(tuple, &tuple_value)._val == RC_TRUE) {
      for (std::vector<struct ddl_executor_paras *>::const_iterator it =
               ddl_executor_paras_list.begin();
           it != ddl_executor_paras_list.end(); ++it) {
        if ((*it)->old_td != old_td)
          continue;
        if ((*it)->type == VERIFICATION_ONLY ||
            (*it)->type == COPY_VERIFICATION) {
          if (!constraints[(*it)->constraint_idx](tuple_value, (*it)->new_v)) {
            printf("DDL failed\n");
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
          t->DDLCDCInsert((*it)->new_td, oid, new_tuple_value, xc->begin,
                          nullptr);
#elif BLOCKDDL
          t->DDLScanUpdate((*it)->new_td, oid, new_tuple_value);
#elif SIDDL
          rc_t r = t->Update((*it)->new_td, oid, nullptr, new_tuple_value);
          if (r._val != RC_TRUE) {
            return rc_t{RC_ABORT_INTERNAL};
          }
#endif
        }
      }
    }
  }

  uint64_t current_csn = dlog::current_csn.load(std::memory_order_relaxed);
  printf("DDL main thread scan ends, current csn: %lu\n", current_csn);

  for (std::vector<thread::Thread *>::const_iterator it = scan_workers.begin();
       it != scan_workers.end(); ++it) {
    (*it)->Join();
    thread::PutThread(*it);
  }

  current_csn = dlog::current_csn.load(std::memory_order_relaxed);
  printf("DDL scan ends, current csn: %lu\n", current_csn);

#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
  printf("t->get_cdc_smallest_csn(): %lu\n", t->get_cdc_smallest_csn());
  // while (t->get_cdc_smallest_csn() <
  //   dlog::current_csn.load(std::memory_order_relaxed) - 100000 &&
  //   !ddl_failed) {
  // }
  t->join_changed_data_capture_threads(cdc_workers);
  current_csn = dlog::current_csn.load(std::memory_order_relaxed);
  printf("current csn: %lu, t->get_cdc_smallest_csn(): %lu\n", current_csn,
         t->get_cdc_smallest_csn());
  if (ddl_failed) {
    printf("DDL failed\n");
    for (std::vector<struct ddl_executor_paras *>::const_iterator it =
             ddl_executor_paras_list.begin();
         it != ddl_executor_paras_list.end(); ++it) {
      (*it)->new_td->GetTupleArray()->destroy((*it)->new_td->GetTupleArray());
      return rc_t{RC_ABORT_INTERNAL};
    }
  }
  printf("First CDC ends\n");
  printf("Now go to grab a t4\n");
  if (config::enable_cdc_verification_test) {
    cdc_test = true;
    usleep(10);
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
  FID fid = t->get_old_td()->GetTupleFid();
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
                // if (f != fid) {
                offset_in_block += logrec->rec_size;
                continue;
              }

              auto *key_array = old_td_map[f]->GetKeyArray();
              fat_ptr *entry = key_array->get(o);
              varstr *key = (config::enable_ddl_keys && entry)
                                ? (varstr *)((*entry).offset())
                                : nullptr;

              if (logrec->type == dlog::log_record::logrec_type::INSERT) {
                dbtuple *tuple = (dbtuple *)(logrec->data);
                varstr tuple_value(tuple->get_value_start(), tuple->size);

                for (std::vector<struct ddl_executor_paras *>::const_iterator
                         it = ddl_executor_paras_list.begin();
                     it != ddl_executor_paras_list.end(); ++it) {
                  if (!(*it)->handle_insert ||
                      (*it)->old_td->GetTupleFid() != f) {
                    goto keep_going;
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
                      goto keep_going;
                    }
                    if (t->DDLCDCInsert((*it)->new_td, o, new_value,
                                        logrec->csn)
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
                    goto keep_going;
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
                      goto keep_going;
                    }
                    if (t->DDLCDCUpdate((*it)->new_td, o, new_value,
                                        logrec->csn)
                            ._val != RC_TRUE) {
                      update_fail++;
                    }
                  }
                }
              }

            keep_going:
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

        /*if (update_fail > 0) {
          printf("CDC update fail, %d\n", update_fail);
        }
        if (insert_fail > 0) {
          printf("CDC insert fail, %d\n", insert_fail);
        }*/

        // if (ddl_running_2) printf("%d, %d\n", update_total, insert_total);
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

  printf("DDL build map begins\n");
  uint64_t count = 0;
  FID fid = td->GetTupleFid();
  auto *alloc = oidmgr->get_allocator(fid);
  uint32_t himark = alloc->head.hiwater_mark;
  auto *tuple_array = td->GetTupleArray();
  auto *key_array = td->GetKeyArray();
  printf("himark: %d\n", himark);

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

  printf("DDL build map ends\n");
  return rc_t{RC_TRUE};
  ;
}

} // namespace ddl

} // namespace ermia
