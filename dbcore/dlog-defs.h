#pragma once

namespace ermia {

namespace dlog {

enum log_record_type : uint8_t {
  // Insert a new record, with the version either embedded directly or stored in
  // a separate "external" block. The external block is a log block that
  // precedes this one, thus ensuring that the payload becomes persistent
  // before the log record does.
  LOG_INSERT = LOG_FLAG_HAS_PAYLOAD | 0x1,
  LOG_INSERT_INDEX = LOG_FLAG_HAS_PAYLOAD | 0x2,

  // Update a record. Version may be embedded or external.
  LOG_UPDATE = LOG_FLAG_HAS_PAYLOAD | 0x3,

  // Delete a record. No payload.
  LOG_DELETE = 0x4,

  // "Enhanced" delete record that contains a pointer to the overwritten
  // version. Has "payload", essentially an update
  LOG_ENHANCED_DELETE = LOG_DELETE | LOG_FLAG_HAS_PAYLOAD,

  // Records the creation of an FID with a given table name
  LOG_FID = LOG_FLAG_HAS_PAYLOAD | 0x9,

  LOG_PRIMARY_INDEX = LOG_FLAG_HAS_PAYLOAD | 0x10,

  LOG_SECONDARY_INDEX = LOG_FLAG_HAS_PAYLOAD | 0x11,
};

// An individual log record describing one operation done to the database.
struct log_record {
  // What kind of log record is this, anyway?
  log_record_type type;

  // Size of the payload
  uint32_t payload_size;

  // Which table does this log record modify?
  FID fid;

  // Which object (tuple) does this log record modify?
  OID oid;

  // Actual payload, must be the last element
  char payload[0];
};

}  // namespace dlog

}  // namespace ermia
