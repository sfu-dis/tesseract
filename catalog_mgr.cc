#include "catalog_mgr.h"

namespace ermia {

namespace catalog {

void create_schema_table(ermia::Engine *db, const char *name) {
  ermia::thread::Thread *thread = ermia::thread::GetThread(true);
  ALWAYS_ASSERT(thread);

  auto create_table = [=](char *) {
    db->CreateTable(name);
    db->CreateMasstreePrimaryIndex(name, std::string(name));
    ermia::schema_td = ermia::Catalog::GetTable(name);
  };

  thread->StartTask(create_table);
  thread->Join();
  ermia::thread::PutThread(thread);
}

void read_schema(transaction *t, ConcurrentMasstreeIndex *schema_table_index,
                 ConcurrentMasstreeIndex *target_table_index,
                 const varstr &table_name, varstr &out_schema_value, OID *out_schema_oid) {
  auto *target_td = target_table_index->GetMasstree().get_table_descriptor();
  bool present_in_table_set = t->get_table_set()->find(target_td->GetTupleFid());

#ifdef BLOCKDDL
  if (!present_in_table_set) {
    target_td->LockSchema(t->is_ddl());

    // Refresh begin timestamp after lock is granted
    t->GetXIDContext()->begin = dlog::current_csn.load(std::memory_order_relaxed);
  }
#endif

  bool schema_ready = true;
retry:
  rc_t rc;
  schema_table_index->GetRecord(t, rc, table_name, out_schema_value, out_schema_oid);
#ifdef BLOCKDDL
  // Under blocking DDL this will always succeed
  LOG_IF(FATAL, rc._val != RC_TRUE);
#else
  if (rc._val != RC_TRUE) {
    DLOG(INFO) << "Catalog: failed reading schema";
    goto retry;
  }
#endif

  schema_kv::value schema_value_temp;
  const schema_kv::value *schema = Decode(out_schema_value, schema_value_temp);
#ifdef COPYDDL
  if (unlikely(t->is_ddl() && schema->state != ddl::schema_state_type::COMPLETE)) {
    goto retry;
  }
  if (schema->state == ddl::schema_state_type::NOT_READY) {
#ifndef LAZYDDL
    if (schema->ddl_type != ddl::ddl_type::COPY_ONLY || config::enable_cdc_schema_lock) {
      goto retry;
    }
    TableDescriptor *td = Catalog::GetTable(schema->fid);
    if (!td || !td->IsReady()) {
      goto retry;
    } else {
      t->SetWaitForNewSchema(true);
      schema_ready = false;
    }
#else  // LAZYDDL
    goto retry;
#endif
  }
#endif  // COPYDDL

  if (!present_in_table_set) {
    TableDescriptor *old_td = schema->old_fid ? Catalog::GetTable(schema->old_fid) : nullptr;
    t->add_to_table_set(target_td, schema->fid, *out_schema_oid, schema->version, schema_ready, old_td);
  }
}

rc_t write_schema(transaction *t, ConcurrentMasstreeIndex *schema_table_index,
                  const varstr &table_name, varstr &schema_value,
                  OID *out_schema_oid, ddl::ddl_executor *ddl_exe,
                  bool is_insert) {
  // For DDL txn only
  ALWAYS_ASSERT(t->is_ddl());

  auto &schema_idx = schema_table_index->GetMasstree();
  auto *target_td = schema_idx.get_table_descriptor();

  OID oid = INVALID_OID;
  rc_t rc = is_insert ? schema_table_index->InsertRecord(t, table_name, schema_value, &oid)
                      : schema_table_index->UpdateRecord(t, table_name, schema_value);

#ifdef BLOCKDDL
  // Under blocking DDL this will always succeed
  LOG_IF(FATAL, rc._val != RC_TRUE);
#endif

  DLOG_IF(INFO, rc._val != RC_TRUE) << "Catalog: failed updating schema";

  if (out_schema_oid) {
    *out_schema_oid = oid;
  }
  return rc;
}

}  // namespace catalog

}  // namespace ermia
