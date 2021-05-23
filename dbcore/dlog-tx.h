#pragma once

#include "dlog-defs.h"
#include "sm-oid.h"

// Transaction-facing logging infrastructure that determines anything related to
// manupulating the bytes recorded by dlog, such as log record format

namespace ermia {

namespace dlog {

struct log_record {
  enum logrec_type {
    INSERT,
    UPDATE,
    DELETE,
  };

  logrec_type type;

  FID fid;
  OID oid;

  char data[0];
};

static uint32_t log_update(log_block *block, FID fid, OID oid, const char *after_image, const uint32_t size) {
  LOG_IF(FATAL, block->payload_size + size > block->capacity) << "No enough space in log block";
  uint32_t off = block->payload_size;

  // Initialize the logrecord header
  log_record *logrec = (log_record *)(&block->payload[off]);

  // Copy contents
  logrec->type = log_record::UPDATE;
  logrec->fid = fid;
  logrec->oid = oid;
  memcpy(logrec->data + block->payload_size, after_image, size);

  // Account for the occupied space
  block->payload_size += align_up(size + sizeof(log_record));

  return off;
}

}

}  // namespace ermia
