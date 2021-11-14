#include "ddl.h"
#include "../schema.h"

namespace ermia {

namespace ddl {

rc_t scan_copy(transaction *t, str_arena *arena, varstr &value) {
  rc_t r;
#ifdef COPYDDL
  struct Schema_record schema;
#else
  struct Schema_base schema;
#endif
  memcpy(&schema, (char *)value.data(), sizeof(schema));
  uint64_t schema_version = schema.v;

  TXN::xid_context *xc = t->GetXIDContext();
  TableDescriptor *old_td = t->GetOldTd();
  TableDescriptor *new_td = t->GetNewTd();

  // printf("DDL scan begins\n");
  uint64_t count = 0;
  auto *alloc = oidmgr->get_allocator(old_td->GetTupleFid());
  uint32_t himark = alloc->head.hiwater_mark;
  auto *old_tuple_array = old_td->GetTupleArray();
  // printf("himark: %d\n", himark);
  auto *new_tuple_array = new_td->GetTupleArray();
  new_tuple_array->ensure_size(new_tuple_array->alloc_size(himark));
  // oidmgr->recreate_allocator(new_td->GetTupleFid(), himark);

  uint32_t scan_threads =
      (thread::cpu_cores.size() / 2) * config::numa_nodes - config::threads;
  printf("scan_threads: %d\n", scan_threads);
  uint32_t num_per_scan_thread = himark / (scan_threads + 1);

#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
  printf("First CDC begins\n");
  std::vector<ermia::thread::Thread *> cdc_workers = t->changed_data_capture();
#endif

  /*std::vector<ermia::thread::Thread *> scan_workers;
  for (uint32_t i = 1; i <= scan_threads; i++) {
    uint32_t begin = i * num_per_scan_thread;
    uint32_t end = (i + 1) * num_per_scan_thread;
    if (i == scan_threads)
      end = himark;
    // printf("%d, %d\n", begin, end);
    thread::Thread *thread =
        thread::GetThread(config::cdc_physical_workers_only);
    ALWAYS_ASSERT(thread);
    scan_workers.push_back(thread);
    auto parallel_scan = [=](char *) {
      for (uint32_t oid = begin; oid <= end; oid++) {
        fat_ptr *entry = old_tuple_array->get(oid);
        if (*entry != NULL_PTR) {
          dbtuple *tuple =
              AWAIT oidmgr->oid_get_version(old_tuple_array, oid, xc);
          // Object *object = (Object *)entry->offset();
          // dbtuple *tuple = (dbtuple *)object->GetPayload();
          varstr tuple_value;
          if (tuple && t->DoTupleRead(tuple, &tuple_value)._val == RC_TRUE) {
            arena->reset();
            varstr *d_v = nullptr;
#ifdef MICROBENCH
            uint64_t a = 0;
            if (schema_version == 1) {
              struct Schema1 record;
              memcpy(&record, (char *)tuple_value.data(), sizeof(record));
              a = record.a;
            } else {
              struct Schema2 record;
              memcpy(&record, (char *)tuple_value.data(), sizeof(record));
              a = record.a;
            }
            char str2[sizeof(Schema2)];
            struct Schema2 record2;
            record2.v = schema_version;
            record2.a = a;
            record2.b = schema_version;
            record2.c = schema_version;
            memcpy(str2, &record2, sizeof(str2));
            d_v = arena->next(sizeof(str2));
            d_v->copy_from(str2, sizeof(str2));
#else
            order_line::value v_ol_temp;
            const order_line::value *v_ol = Decode(tuple_value, v_ol_temp);
            order_line_1::value v_ol_1;
            v_ol_1.ol_i_id = v_ol->ol_i_id;
            v_ol_1.ol_delivery_d = v_ol->ol_delivery_d;
            v_ol_1.ol_amount = v_ol->ol_amount;
            v_ol_1.ol_supply_w_id = v_ol->ol_supply_w_id;
            v_ol_1.ol_quantity = v_ol->ol_quantity;
            v_ol_1.v = schema_version;
            v_ol_1.ol_tax = 0.1;
            const size_t order_line_sz = ::Size(v_ol_1);
            d_v = arena->next(order_line_sz);
            d_v = &Encode(*d_v, v_ol_1);
#endif
            t->DDLCDCInsert(new_td, oid, d_v, t->GetXIDContext()->begin);
          }
        }
      }
    };
    thread->StartTask(parallel_scan);
  }*/

  for (OID oid = 0; oid <= himark; oid++) {
    fat_ptr *entry = old_tuple_array->get(oid);
    if (*entry != NULL_PTR) {
      dbtuple *tuple = AWAIT oidmgr->oid_get_version(old_tuple_array, oid, xc);
      // Object *object = (Object *)entry->offset();
      // dbtuple *tuple = (dbtuple *)object->GetPayload();
      varstr tuple_value;
      if (tuple && t->DoTupleRead(tuple, &tuple_value)._val == RC_TRUE) {
        arena->reset();
        varstr *d_v = nullptr;
        /*#ifdef MICROBENCH
                uint64_t a = 0;
                if (schema_version == 1) {
                  struct Schema1 record;
                  memcpy(&record, (char *)tuple_value.data(), sizeof(record));
                  a = record.a;
                } else {
                  struct Schema2 record;
                  memcpy(&record, (char *)tuple_value.data(), sizeof(record));
                  a = record.a;
                }

                char str2[sizeof(Schema2)];
                struct Schema2 record2;
                record2.v = schema_version;
                record2.a = a;
                record2.b = schema_version;
                record2.c = schema_version;
                memcpy(str2, &record2, sizeof(str2));
                d_v = arena->next(sizeof(str2));
                d_v->copy_from(str2, sizeof(str2));
        #else
                order_line::value v_ol_temp;
                const order_line::value *v_ol = Decode(tuple_value, v_ol_temp);

                order_line_1::value v_ol_1;
                v_ol_1.ol_i_id = v_ol->ol_i_id;
                v_ol_1.ol_delivery_d = v_ol->ol_delivery_d;
                v_ol_1.ol_amount = v_ol->ol_amount;
                v_ol_1.ol_supply_w_id = v_ol->ol_supply_w_id;
                v_ol_1.ol_quantity = v_ol->ol_quantity;
                v_ol_1.v = schema_version;
                v_ol_1.ol_tax = 0.1;

                const size_t order_line_sz = ::Size(v_ol_1);
                d_v = arena->next(order_line_sz);
                d_v = &Encode(*d_v, v_ol_1);
        #endif*/

        t->DDLCDCInsert(new_td, oid, d_v, t->GetXIDContext()->begin);
      }
    }
  }

  /*for (std::vector<ermia::thread::Thread *>::const_iterator it =
           scan_workers.begin();
       it != scan_workers.end(); ++it) {
    (*it)->Join();
    ermia::thread::PutThread(*it);
  }*/

  uint64_t current_csn = dlog::current_csn.load(std::memory_order_relaxed);
  uint64_t rough_t3 = current_csn - 1000000;
  printf("DDL scan ends, current csn: %lu, t3: %lu\n", current_csn, rough_t3);

#if defined(COPYDDL) && !defined(LAZYDDL) && !defined(DCOPYDDL)
  while (t->get_cdc_smallest_csn() < rough_t3) {
  }
  t->join_changed_data_capture_threads(cdc_workers);
  printf("First CDC ends\n");
  printf("Now go to grab a t4\n");
#endif

  return rc_t{RC_TRUE};
}

bool changed_data_capture_impl(transaction *t, uint32_t thread_id,
                               uint32_t ddl_thread_id, uint32_t begin_log,
                               uint32_t end_log, str_arena *arena,
                               util::fast_random &r) {
  RCU::rcu_enter();
  TableDescriptor *old_td = t->GetOldTd();
  TableDescriptor *new_td = t->GetNewTd();
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
      // printf("log %d cdc\n", i);
    process:
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
                varstr value(tuple->get_value_start(), tuple->size);

                /*order_line::value v_ol_temp;
                const order_line::value *v_ol = Decode(value, v_ol_temp);

                order_line_1::value v_ol_1;
                v_ol_1.ol_i_id = v_ol->ol_i_id;
                v_ol_1.ol_delivery_d = v_ol->ol_delivery_d;
                v_ol_1.ol_amount = v_ol->ol_amount;
                v_ol_1.ol_supply_w_id = v_ol->ol_supply_w_id;
                v_ol_1.ol_quantity = v_ol->ol_quantity;
                v_ol_1.v = v_ol->v + 1;
                v_ol_1.ol_tax = 0.1;

                const size_t order_line_sz = ::Size(v_ol_1);
                varstr *d_v = arena->next(order_line_sz);
                d_v = &Encode(*d_v, v_ol_1);
                */

                varstr *d_v = nullptr;

                insert_total++;
                if (t->DDLCDCInsert(new_td, o, d_v, logrec->csn)._val !=
                    RC_TRUE) {
                  insert_fail++;
                }
                if (table_secondary_index) {
                  //   table_secondary_index->InsertOID(t, *insert_key_idx,
                  //   oid);
                }
              } else if (logrec->type ==
                         dlog::log_record::logrec_type::UPDATE) {
                dbtuple *tuple = (dbtuple *)(logrec->data);
                varstr value(tuple->get_value_start(), tuple->size);

                varstr *d_v = nullptr;
                /*#ifdef MICROBENCH
                                struct Schema_base record_test;
                                memcpy(&record_test, (char *)value.data(),
                sizeof(record_test)); uint64_t version = record_test.v; uint64_t
                a = 0; if (version == 0) { struct Schema1 record;
                                  memcpy(&record, (char *)value.data(),
                sizeof(record)); a = record.a; } else { struct Schema2 record;
                                  memcpy(&record, (char *)value.data(),
                sizeof(record)); a = record.a;
                                }

                                version++;
                                ALWAYS_ASSERT(version != 0);

                                char str2[sizeof(Schema2)];
                                struct Schema2 record2;
                                record2.v = version;
                                record2.a = a;
                                record2.b = version;
                                record2.c = version;
                                memcpy(str2, &record2, sizeof(str2));
                                d_v = arena->next(sizeof(str2));
                                d_v->copy_from(str2, sizeof(str2));
                #else
                                order_line::value v_ol_temp;
                                const order_line::value *v_ol = Decode(value,
                v_ol_temp);

                                order_line_1::value v_ol_1;
                                v_ol_1.ol_i_id = v_ol->ol_i_id;
                                v_ol_1.ol_delivery_d = v_ol->ol_delivery_d;
                                v_ol_1.ol_amount = v_ol->ol_amount;
                                v_ol_1.ol_supply_w_id = v_ol->ol_supply_w_id;
                                v_ol_1.ol_quantity = v_ol->ol_quantity;
                                v_ol_1.v = v_ol->v + 1;
                                v_ol_1.ol_tax = 0.1;

                                const size_t order_line_sz = ::Size(v_ol_1);
                                d_v = arena->next(order_line_sz);
                                d_v = &Encode(*d_v, v_ol_1);
                #endif*/

                update_total++;
                if (t->DDLCDCUpdate(new_td, o, d_v, logrec->csn)._val !=
                    RC_TRUE) {
                  update_fail++;
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
  return count == 0;
}

} // namespace ddl

} // namespace ermia
