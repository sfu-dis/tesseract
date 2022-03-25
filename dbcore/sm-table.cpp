#include "sm-table.h"

#include "../engine.h"

namespace ermia {

std::unordered_map<std::string, TableDescriptor *> Catalog::name_map;
std::unordered_map<FID, TableDescriptor *> Catalog::fid_map;
std::unordered_map<std::string, OrderedIndex *> Catalog::index_map;

TableDescriptor::TableDescriptor(std::string &name)
    : name(name),
      primary_index(nullptr),
      tuple_fid(0),
      tuple_array(nullptr),
      aux_fid_(0),
      aux_array_(nullptr) {
#ifdef BLOCKDDL
    int ret = pthread_rwlock_init(&schema_lock, nullptr);
    LOG_IF(FATAL, ret);
#endif
}

void TableDescriptor::Initialize() {
  tuple_fid = oidmgr->create_file(true);
  Catalog::fid_map[tuple_fid] = this;
  tuple_array = oidmgr->get_array(tuple_fid);

  // Dedicated array for keys
  aux_fid_ = oidmgr->create_file(true);
  aux_array_ = oidmgr->get_array(aux_fid_);
}

void TableDescriptor::SetPrimaryIndex(OrderedIndex *index,
                                      const std::string &name) {
  ALWAYS_ASSERT(index);
  ALWAYS_ASSERT(!primary_index);
  primary_index = index;
  Catalog::index_map[name] = index;
  index->SetArrays(true);
}

void TableDescriptor::AddSecondaryIndex(OrderedIndex *index,
                                        const std::string &name) {
  ALWAYS_ASSERT(index);
  sec_indexes.push_back(index);
  Catalog::index_map[name] = index;
  index->SetArrays(false);
}

void TableDescriptor::Recover(FID tuple_fid, FID aux_fid, OID himark) {
  ALWAYS_ASSERT(tuple_fid == 0);
  tuple_fid = tuple_fid;
  aux_fid_ = aux_fid;

  // Both primary and secondary indexes point to the same descriptor
  if (!Catalog::FidExists(tuple_fid)) {
    // Primary index
    oidmgr->recreate_file(tuple_fid);
    Catalog::fid_map[tuple_fid] = this;
  }
  oidmgr->recreate_file(aux_fid_);
  Catalog::fid_map[aux_fid_] = this;

  ALWAYS_ASSERT(oidmgr->file_exists(tuple_fid));
  tuple_array = oidmgr->get_array(tuple_fid);
  ALWAYS_ASSERT(oidmgr->file_exists(aux_fid));
  aux_array_ = oidmgr->get_array(aux_fid_);

  if (himark > 0) {
    tuple_array->ensure_size(tuple_array->alloc_size(himark));
    aux_array_->ensure_size(aux_array_->alloc_size(himark));
    oidmgr->recreate_allocator(tuple_fid, himark);
  }
}

#ifdef BLOCKDDL
// Acquire lock for a table (lock the schema)
void TableDescriptor::LockSchema(bool exclusive) {
  int ret = -1;
  if (exclusive) {
    ret = pthread_rwlock_wrlock(&schema_lock);
  } else {
    ret = pthread_rwlock_rdlock(&schema_lock);
  }
  LOG_IF(FATAL, ret);
}

void TableDescriptor::UnlockSchema() {
  int ret = pthread_rwlock_unlock(&schema_lock);
  LOG_IF(FATAL, ret);
}
#endif  // BLOCKDDL

}  // namespace ermia
