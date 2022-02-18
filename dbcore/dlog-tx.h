#pragma once

#include "dlog-defs.h"
#include "sm-oid.h"

// Transaction-facing logging infrastructure that determines anything related to
// manupulating the bytes recorded by dlog, such as log record format

namespace ermia {

namespace dlog {

struct log_record {
  enum logrec_type {
    INVALID,
    INSERT,
    INSERT_KEY,
    UPDATE,
    UPDATE_KEY,
    OID_KEY,
  };

  logrec_type type;

  FID fid;
  OID oid;

  uint32_t rec_size;
  uint64_t csn;

  char data[0];
};

static uint32_t populate_log_record(log_record::logrec_type type,
                                    log_block *block, FID fid, OID oid,
                                    const char *after_image,
                                    const uint32_t size) {
  LOG_IF(FATAL, type != log_record::logrec_type::INSERT &&
                    type != log_record::logrec_type::UPDATE &&
                    type != log_record::logrec_type::INSERT_KEY &&
                    type != log_record::logrec_type::UPDATE_KEY &&
                    type != log_record::logrec_type::OID_KEY)
      << "Wrong log record type";
  LOG_IF(FATAL, block->payload_size + size > block->capacity)
      << "No enough space in log block";
  uint32_t off = block->payload_size;

  // Initialize the logrecord header
  log_record *logrec = (log_record *)(&block->payload[off]);

  // Copy contents
  logrec->type = type;
  logrec->fid = fid;
  logrec->oid = oid;
  logrec->csn = block->csn;
  memcpy(&logrec->data[0], after_image, size);

  // Account for the occupied space
  uint32_t rec_size = align_up(size + sizeof(log_record));
  logrec->rec_size = rec_size;
  block->payload_size += rec_size;

  return off;
}

inline static uint32_t log_insert(log_block *block, FID fid, OID oid,
                                  const char *image, const uint32_t size) {
  return populate_log_record(log_record::INSERT, block, fid, oid, image, size);
}

inline static uint32_t log_update(log_block *block, FID fid, OID oid,
                                  const char *image, const uint32_t size) {
  return populate_log_record(log_record::UPDATE, block, fid, oid, image, size);
}

inline static uint32_t log_insert_key(log_block *block, FID fid, OID oid,
                                      const char *image, const uint32_t size) {
  return populate_log_record(log_record::INSERT_KEY, block, fid, oid, image,
                             size);
}

inline static uint32_t log_update_key(log_block *block, FID fid, OID oid,
                                      const char *image, const uint32_t size) {
  return populate_log_record(log_record::UPDATE_KEY, block, fid, oid, image,
                             size);
}

}  // namespace dlog

}  // namespace ermia
