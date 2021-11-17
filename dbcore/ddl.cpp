#include "ddl.h"
#include "../benchmarks/oddlb-schemas.h"
#include "../benchmarks/tpcc.h"
#include "../schema.h"

namespace ermia {

namespace ddl {

std::vector<Reformat> reformats;
std::vector<Constraint> constraints;

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

  // printf("DDL scan begins\n");
  uint64_t count = 0;
  auto *alloc = oidmgr->get_allocator(old_td->GetTupleFid());
  uint32_t himark = alloc->head.hiwater_mark;
  auto *old_tuple_array = old_td->GetTupleArray();
  // printf("himark: %d\n", himark);
  auto *new_tuple_array = new_td->GetTupleArray();
  new_tuple_array->ensure_size(new_tuple_array->alloc_size(himark));

#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
  printf("First CDC begins\n");
  std::vector<ermia::thread::Thread *> cdc_workers = t->changed_data_capture();
#endif

  for (OID oid = 0; oid <= himark; oid++) {
    fat_ptr *entry = old_tuple_array->get(oid);
    if (*entry != NULL_PTR) {
      dbtuple *tuple = AWAIT oidmgr->oid_get_version(old_tuple_array, oid, xc);
      // Object *object = (Object *)entry->offset();
      // dbtuple *tuple = (dbtuple *)object->GetPayload();
      varstr tuple_value;
      if (tuple && t->DoTupleRead(tuple, &tuple_value)._val == RC_TRUE) {
        if (type == VERIFICATION_ONLY || type == COPY_VERIFICATION) {
          if (!constraints[constraint_idx](tuple_value, new_v)) {
            printf("DDL failed\n");
            return rc_t{RC_ABORT_INTERNAL};
          }
        }
        if (type == COPY_ONLY || type == COPY_VERIFICATION) {
          arena->reset();
          varstr *new_tuple_value =
              reformats[reformat_idx](tuple_value, arena, new_v);
#ifdef COPYDDL
          t->DDLCDCInsert(new_td, oid, new_tuple_value, xc->begin);
#elif BLOCKDDL
          t->DDLScanUpdate(new_td, oid, new_tuple_value);
#elif SIDDL
          r = t->Update(new_td, oid, nullptr, new_tuple_value);
          if (r._val != RC_TRUE) {
            return r;
          }
#endif
        }
      }
    }
  }

  uint64_t current_csn = dlog::current_csn.load(std::memory_order_relaxed);
  uint64_t rough_t3 = current_csn - 3000000;
  printf("DDL scan ends, current csn: %lu\n", current_csn);

#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
  while (t->get_cdc_smallest_csn() < rough_t3 && !cdc_failed) {
  }
  t->join_changed_data_capture_threads(cdc_workers);
  if (cdc_failed) {
    printf("DDL failed\n");
    new_td->GetTupleArray()->destroy(new_td->GetTupleArray());
    return rc_t{RC_ABORT_INTERNAL};
  }
  printf("First CDC ends\n");
  printf("Now go to grab a t4\n");
  if (config::enable_cdc_verification_test) {
    cdc_test = true;
    usleep(10);
  }
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
  bool ddl_end_tmp = true, ddl_end_flag = false;
  FID fid = old_td->GetTupleFid();
  ermia::ConcurrentMasstreeIndex *table_secondary_index = nullptr;
  if (new_td->GetSecIndexes().size()) {
    table_secondary_index =
        (ermia::ConcurrentMasstreeIndex *)(*(new_td->GetSecIndexes().begin()));
    ALWAYS_ASSERT(table_secondary_index);
  }
  uint32_t count = 0;
  for (uint32_t i = begin_log; i <= end_log; i++) {
    dlog::tls_log *tlog = dlog::tlogs[i];
    uint64_t csn = volatile_read(pcommit::_tls_durable_csn[i]);
    if (tlog && csn && tlog != GetLog() && i != ddl_thread_id) {
      count++;
    }
  }
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
        uint64_t block_sz = sizeof(dlog::log_block),
                 logrec_sz = sizeof(dlog::log_record),
                 tuple_sc = sizeof(dbtuple);

        while (offset_increment <
                   (data_sz = volatile_read(seg->size)) - offset_in_seg &&
               cdc_running) {
          char *header_buf = (char *)RCU::rcu_alloc(block_sz);
          size_t m = os_pread(seg->fd, (char *)header_buf, block_sz,
                              offset_in_seg + offset_increment);
          dlog::log_block *header = (dlog::log_block *)(header_buf);
          if ((end_csn && begin_csn <= header->csn && header->csn <= end_csn) ||
              (!end_csn && begin_csn <= header->csn)) {
            last_csn = header->csn;
            volatile_write(_cdc_last_csn[i], last_csn);
            uint32_t block_total_sz = header->total_size();
            char *data_buf = (char *)RCU::rcu_alloc(block_total_sz);
            size_t m = os_pread(seg->fd, (char *)data_buf, block_total_sz,
                                offset_in_seg + offset_increment);
            uint64_t offset_in_block = 0;
            varstr *insert_key, *update_key, *insert_key_idx;
            while (offset_in_block < header->payload_size && cdc_running) {
              dlog::log_record *logrec =
                  (dlog::log_record *)(data_buf + block_sz + offset_in_block);

              ALWAYS_ASSERT(header->csn == logrec->csn);

              FID f = logrec->fid;
              OID o = logrec->oid;

              if (f != fid) {
                offset_in_block += logrec->rec_size;
                continue;
              }

              arena->reset();

              if (logrec->type == dlog::log_record::logrec_type::INSERT) {
                dbtuple *tuple = (dbtuple *)(logrec->data);
                varstr tuple_value(tuple->get_value_start(), tuple->size);

                if (type == VERIFICATION_ONLY || type == COPY_VERIFICATION) {
                  if (!constraints[constraint_idx](tuple_value, new_v)) {
                    return rc_t{RC_ABORT_INTERNAL};
                  }
                }
                if (type == COPY_ONLY || type == COPY_VERIFICATION) {

                  varstr *new_value =
                      reformats[reformat_idx](tuple_value, arena, new_v);

                  insert_total++;
                  if (t->DDLCDCInsert(new_td, o, new_value, logrec->csn)._val !=
                      RC_TRUE) {
                    insert_fail++;
                  }
                  if (table_secondary_index) {
                    //   table_secondary_index->InsertOID(t, *insert_key_idx,
                    //   oid);
                  }
                }
              } else if (logrec->type ==
                         dlog::log_record::logrec_type::UPDATE) {
                dbtuple *tuple = (dbtuple *)(logrec->data);
                varstr tuple_value(tuple->get_value_start(), tuple->size);

                if (type == VERIFICATION_ONLY || type == COPY_VERIFICATION) {
                  if (!constraints[constraint_idx](tuple_value, new_v)) {
                    return rc_t{RC_ABORT_INTERNAL};
                  }
                }
                if (type == COPY_ONLY || type == COPY_VERIFICATION) {
                  varstr *new_value =
                      reformats[reformat_idx](tuple_value, arena, new_v);

                  update_total++;
                  if (t->DDLCDCUpdate(new_td, o, new_value, logrec->csn)._val !=
                      RC_TRUE) {
                    update_fail++;
                  }
                }
              }

              offset_in_block += logrec->rec_size;
            }
            RCU::rcu_free(data_buf);
          } else {
            if (end_csn && header->csn > end_csn) {
              RCU::rcu_free(header_buf);
              count--;
              stop_scan = true;
              break;
            }
          }
          offset_increment += header->total_size();
          volatile_write(_tls_durable_lsn[i], offset_in_seg + offset_increment);
          RCU::rcu_free(header_buf);
        }
        if (!stop_scan && (insert_total == 0 || update_total == 0)) {
          count--;
        }

        /*if (update_fail > 0) {
          printf("CDC update fail, %d\n", update_fail);
        }
        if (insert_fail > 0) {
          printf("CDC insert fail, %d\n", insert_fail);
        }*/

        // printf("%d, %d\n", update_total, insert_total);

        if (stop_scan)
          break;
      }
    }
    if (!cdc_running) {
      break;
    }
  }
  RCU::rcu_exit();
  *ddl_end = (count == 0);
  return rc_t{RC_TRUE};
}

} // namespace ddl

} // namespace ermia
