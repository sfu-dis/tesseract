#pragma once

#include <string>
#include <unordered_map>
#include "sm-common.h"
#include "sm-oid.h"

namespace ermia {

class OrderedIndex;

class TableDescriptor {
 private:
  std::string name;
  OrderedIndex *primary_index;
  std::vector<OrderedIndex *> sec_indexes;

  FID tuple_fid;
  oid_array* tuple_array;

  FID aux_fid_;
  oid_array* aux_array_;

 public:
  TableDescriptor(std::string& name);

  void Initialize();
  void SetPrimaryIndex(OrderedIndex *index, const std::string &name);
  void AddSecondaryIndex(OrderedIndex *index, const std::string &name);
  void Recover(FID tuple_fid, FID key_fid, OID himark = 0);
  inline std::string& GetName() { return name; }
  inline OrderedIndex* GetPrimaryIndex() { return primary_index; }
  inline FID GetTupleFid() { return tuple_fid; }
  inline FID GetKeyFid() {
    return aux_fid_;
  }
  inline oid_array* GetKeyArray() {
    return aux_array_;
  }
  inline oid_array* GetTupleArray() { return tuple_array; }
  inline std::vector<OrderedIndex *> GetSecIndexes() { return sec_indexes; }
  inline void SetTupleFid(FID fid) { tuple_fid = fid; }
  inline void SetOidArray(oid_array *array) { tuple_array = array; }
  inline void SetPrimaryIndex(OrderedIndex *index) {
    ALWAYS_ASSERT(index);
    primary_index = index;
  }
  inline void AddSecondaryIndex(OrderedIndex *index) {
    ALWAYS_ASSERT(index);
    sec_indexes.push_back(index);
  }
};

struct Catalog {
  // Map table name to descriptors, global, no CC
  static std::unordered_map<std::string, TableDescriptor*> name_map;

  // Map FID to descriptors, global, no CC
  static std::unordered_map<FID, TableDescriptor*> fid_map;

  // Map index name to OrderedIndex (primary or secondary), global, no CC
  static std::unordered_map<std::string, OrderedIndex*> index_map;

  static inline OrderedIndex *GetIndex(const std::string &name) {
    return index_map[name];
  }
  static inline bool NameExists(std::string name) {
    return name_map.find(name) != name_map.end();
  }
  static inline bool FidExists(FID fid) {
    return fid_map.find(fid) != fid_map.end();
  }
  static inline TableDescriptor* GetTable(std::string name) {
    return name_map[name];
  }
  static inline TableDescriptor* GetTable(FID fid) { return fid_map[fid]; }
  static inline OrderedIndex* GetPrimaryIndex(const std::string& name) {
    return name_map[name]->GetPrimaryIndex();
  }
  static inline OrderedIndex* GetPrimaryIndex(FID fid) {
    return fid_map[fid]->GetPrimaryIndex();
  }
  static inline TableDescriptor* NewTable(std::string name) {
    name_map[name] = new TableDescriptor(name);
    return name_map[name];
  }
  static inline uint32_t NumTables() { return name_map.size(); }
};

}  // namespace ermia
