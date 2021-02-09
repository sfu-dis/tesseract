#pragma once

#include "sm-common.h"
#include "sm-log-defs.h"

namespace ermia {

/* A factory class for creating log scans.

   These scans form the basis of recovery, and can be used before
   normal log functions are available.
 */
struct sm_log_scan_mgr {
  static size_t const NO_PAYLOAD = -1;

  enum record_type {
    LOG_INSERT,
    LOG_INSERT_INDEX,
    LOG_UPDATE,
    LOG_RELOCATE,
    LOG_DELETE,
    LOG_ENHANCED_DELETE,
    LOG_UPDATE_KEY,
    LOG_FID,
    LOG_PRIMARY_INDEX,
    LOG_SECONDARY_INDEX
  };

  /* A cursor for iterating over log records, whether those of a single
     transaction or all which follow some arbitrary starting point.
  */
  struct record_scan {
    /* Query whether the cursor currently rests on a valid record */
    bool valid();

    /* Advance to the next record */
    void next();

    /* Return the type of record */
    record_type type();

    /* Return the FID and OID the current record refers to */
    FID fid();
    OID oid();

    /* Return the size of the payload, or NO_PAYLOAD if the log record
       has no payload.

       NOTE: this function returns the size of the actual object, even
       for external/reloc records where the log record's "payload" is
       technically a pointer to the actual object.
    */
    size_t payload_size();

    /* Return a pointer to the payload, or NULL_PTR if the log record
       has no payload.

       NOTE: this function returns the pointer to the actual object,
       even for external/reloc records where the log record's
       "payload" is technically a pointer to the actual object.
    */
    fat_ptr payload_ptr();

    LSN payload_lsn();

    LSN block_lsn();

    /* Copy the current record's payload into [buf]. Throw
       illegal_argument if the record has no payload, or the payload
       is larger than [bufsz], or the record does not reside in the
       log.

       NOTE: this function is usually more efficient than
       sm_log::load_object, because the scanner probably loaded the
       payload into memory already as part of its normal operations.
    */
    void load_object(char *buf, size_t bufsz);

    virtual ~record_scan() {}

   protected:
    // forbid direct instantiation
    record_scan() {}
  };

  /* Similar to record_scan, but it does *not* fetch payloads.

     Fetching only headers can reduce the I/O bandwidth requirements of
     the scan by anywhere from 50% to well over 99%, depending on the
     sizes of log records involved). However, it generates a random
     access pattern that will perform poorly on spinning platters. It
     also means that all payloads must be fetched manually at a later
     time, and any log record not stored directly in the log will
     require a second I/O to fetch. These indirect pointers have type
     ASI_EXT rather than ASI_LOG or ASI_HEAP, and must be dereferenced
     by a call to sm_log::load_ptr. Indirect pointers do encode the
     proper object size, however, so buffer space can be allocated
     before requesting any I/O.
  */
  struct header_scan {
    /* Query whether the cursor currently rests on a valid record */
    bool valid();

    /* Advance to the next record */
    void next();

    /* Return the type of record */
    record_type type();

    /* Return the FID and OID the current record refers to */
    FID fid();
    OID oid();

    /* Return the size of the payload, or NO_PAYLOAD if the log record
       has no payload.

       NOTE: this function returns the size of the actual object, even
       for external/reloc records where the log record's "payload" is
       technically a pointer to the actual object.
    */
    size_t payload_size();

    /* Return a pointer to the payload, or NULL_PTR if the log record
       has no payload. If [follow_ext] is set and the pointer is
       ASI_EXT, dereference it (implying an I/O operation). Otherwise,
       return the raw pointer, regardless of its type.

       NOTE: a result of type ASI_EXT means it is necessary to
       dereference the pointer to find the true location of the
       record's payload.
    */
    fat_ptr payload_ptr(bool follow_ext = false);

    /* Attempt to copy the current record's payload into [buf]. Throw
       illegal_argument if the record has no payload, or the payload
       is larger than [bufsz].

       If the record resides in the log, load it; if the record
       payload is ASI_EXT, dereference the pointer (placing the result
       in [pdest]); if the result is ASI_LOG, fetch it as well and
       return true, otherwise (e.g. ASI_HEAP) return false.

       NOTE: this function is provided as a convenience, but is no
       more efficient than sm_log::load_object.
    */
    bool load_object(fat_ptr &pdest, char *buf, size_t bufsz);

    virtual ~header_scan() {}

   protected:
    // forbid direct instantiation
    header_scan() {}
  };

  /* Start scanning log headers from [start], stopping at
     end-of-log. Record payloads are not available, and must be
     loaded manually if desired.
   */
  header_scan *new_header_scan(LSN start, bool force_fetch_from_logbuf);

  /* Start scanning the log from [start], stopping only when
     end-of-log is encountered. Record payloads are available.
   */
  record_scan *new_log_scan(LSN start, bool fetch_payloads,
                            bool force_fetch_from_logbuf);

  /* Start scanning log entries for the transaction whose commit
     record resides at [start]. Stop when all records for the
     transaction have been visited. Record payloads are available.
   */
  record_scan *new_tx_scan(LSN start, bool force_fetch_from_logbuf);

  /* Load the object referenced by [ptr] from the log. The pointer
     must reference the log (ASI_LOG) and the given buffer must be large
     enough to hold the object.
   */
  void load_object(char *buf, size_t bufsz, fat_ptr ptr,
                   size_t align_bits = DEFAULT_ALIGNMENT_BITS);

  /* Retrieve the address of an externalized log record payload.

     The pointer must be external (ASI_EXT).
   */
  fat_ptr load_ext_pointer(fat_ptr ptr);

  virtual ~sm_log_scan_mgr() {}

 protected:
  // forbid direct instantiation
  sm_log_scan_mgr() {}
};
/* NOTE: This class is needed during recovery, so the implementation
   is in sm-log-recover.cpp, not sm-log.cpp.
 */
struct sm_log_scan_mgr_impl : sm_log_scan_mgr {
  sm_log_scan_mgr_impl(sm_log_recover_mgr *lm);

  sm_log_recover_mgr *lm;
};

/* NOTE: This class is needed during recovery, so the implementation
   is in sm-log-recover.cpp, not sm-log.cpp.
 */
struct sm_log_header_scan_impl : sm_log_scan_mgr::header_scan {
  sm_log_header_scan_impl(sm_log_recover_mgr *lm, LSN start,
                          bool force_fetch_from_logbuf);

  sm_log_recover_mgr::log_scanner scan;
  sm_log_recover_mgr *lm;
};

/* NOTE: This class is needed during recovery, so the implementation
   is in sm-log-recover.cpp, not sm-log.cpp.
 */
struct sm_log_record_scan_impl : sm_log_scan_mgr::record_scan {
  sm_log_record_scan_impl(sm_log_recover_mgr *lm, LSN start, bool just_one_tx,
                          bool fetch_payloads, bool force_fetch_from_logbuf);

  sm_log_recover_mgr::log_scanner scan;
  sm_log_recover_mgr *lm;

  LSN start_lsn;
  bool just_one;
};

DEF_IMPL(sm_log_scan_mgr);
DEF_IMPL2(sm_log_scan_mgr::record_scan, sm_log_record_scan_impl);
DEF_IMPL2(sm_log_scan_mgr::header_scan, sm_log_header_scan_impl);

}  // namespace ermia
