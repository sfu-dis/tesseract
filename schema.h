#pragma once

#include "benchmarks/record/encoder.h"
#include "benchmarks/record/inline_str.h"
#include "engine.h"
#include "sm-table.h"

#define SCHEMA_KEY_FIELDS(x, y) x(uint32_t, schema_key)
#define SCHEMA_VALUE_FIELDS(x, y)                                              \
  x(uint32_t, version) y(uint32_t, csn) y(uint32_t, reformat_idx)              \
      y(uint32_t, constraint_idx) y(uint32_t, secondary_index_key_create_idx)  \
          y(uint32_t, ddl_type) y(uint64_t, index) y(uint32_t, fid)            \
              y(bool, show_index) y(uint32_t, old_fid) y(uint32_t, state)      \
                  y(uint32_t, old_version) y(uint64_t, old_index)              \
                      y(uint32_t, old_fids_total) y(uint32_t, reformats_total) \
                          y(inline_str_16<400>, old_fids)                      \
                              y(inline_str_16<400>, reformats)
DO_STRUCT(schema_kv, SCHEMA_KEY_FIELDS, SCHEMA_VALUE_FIELDS);

namespace ermia {

struct schema_base {
  uint32_t v;
  uint32_t csn;
  uint32_t reformat_idx;
  uint32_t constraint_idx;
  uint32_t secondary_index_key_create_idx;
  ddl::ddl_type ddl_type;
  OrderedIndex *index;
  TableDescriptor *td;
  bool show_index;  // simulate no index
};

struct schema_record : public schema_base {
  TableDescriptor *old_td;
  ddl::schema_state_type state;
  uint32_t old_v;
  OrderedIndex *old_index;
  uint32_t old_tds_total;
  uint32_t reformats_total;
  TableDescriptor *old_tds[16];
  uint32_t reformats[16];

  // Runtime schema record convert to persistent schema value
  inline void record_to_value(schema_kv::value &schema_value) {
    schema_value.version = v;
    schema_value.csn = csn;
    schema_value.reformat_idx = reformat_idx;
    schema_value.constraint_idx = constraint_idx;
    schema_value.secondary_index_key_create_idx =
        secondary_index_key_create_idx;
    schema_value.ddl_type = ddl_type;
    schema_value.index = (uint64_t)index;
    schema_value.fid = td->GetTupleFid();
    schema_value.show_index = show_index;
    schema_value.state = state;
    schema_value.old_fid = old_td ? old_td->GetTupleFid() : 0;
    schema_value.old_version = old_v;
    schema_value.old_index = (uint64_t)old_index;
    schema_value.old_fids_total = old_tds_total;
    schema_value.reformats_total = reformats_total;
    FID old_fids[16];
    for (int i = 0; i < old_tds_total; i++) {
      old_fids[i] = old_tds[i]->GetTupleFid();
    }
    schema_value.old_fids.assign((const char *)old_fids,
                                 sizeof(ermia::FID) * old_tds_total);
    schema_value.reformats.assign((const char *)reformats,
                                  sizeof(uint32_t) * reformats_total);
  }

  // Persistent schema value convert to runtime schema record
  inline void value_to_record(const schema_kv::value *schema_value) {
    v = schema_value->version;
    csn = schema_value->csn;
    reformat_idx = schema_value->reformat_idx;
    constraint_idx = schema_value->constraint_idx;
    secondary_index_key_create_idx =
        schema_value->secondary_index_key_create_idx;
    ddl_type = (ddl::ddl_type)schema_value->ddl_type;
    index = (OrderedIndex *)schema_value->index;
    td = ermia::Catalog::GetTable(schema_value->fid);
    show_index = schema_value->show_index;
    state = (ddl::schema_state_type)schema_value->state;
    old_td = schema_value->old_fid
                 ? ermia::Catalog::GetTable(schema_value->old_fid)
                 : nullptr;
    old_v = schema_value->old_version;
    old_index = (OrderedIndex *)schema_value->old_index;
    old_tds_total = schema_value->old_fids_total;
    reformats_total = schema_value->reformats_total;
    for (int i = 0; i < schema_value->old_fids_total; i++) {
      old_tds[i] = ermia::Catalog::GetTable(
          *(uint32_t *)(schema_value->old_fids.data() + i));
    }
    for (int i = 0; i < schema_value->reformats_total; i++) {
      reformats[i] = *((uint32_t *)schema_value->reformats.data() + i);
    }
  }
};

}  // namespace ermia
