#pragma once
#include "sm-common.h"
#include "sm-log-defs.h"

namespace ermia {
class sm_log;
struct log_allocation;

/* This is the staging area for log record requests until they
   actually find a home in a proper log block. We do this because we
   want the transaction's CSN to be as late as possible, and so we
   can't just allocate a log block to put log records into.
 */
class sm_tx_log {
public:
  sm_tx_log(sm_log *l)
      : _log(l),
        _nreq(0),
        _payload_bytes(0),
        _prev_overflow(INVALID_LSN),
        _commit_block(NULL) {}

  /* Record an insertion. The payload of the version will be
     embedded in the log record on disk and the version's
     [disk_addr] will be set accordingly. The target OID will be
     re-allocated during recovery.

     The parameters [f] and [o] identify the record, whose contents
     are the payload of the version stored at [p]. In order to
     simplify the implementation, [psize] specifies the size of the
     record payload, which should *not* include the (volatile)
     version header information and which should be consistent with
     the encoded size embedded in [p].

     If [pdest] is non-NULL, the pointed-to location will be set to
     the record's location on disk. That assignment may not occur
     until a call to commit() or pre_commit(), so the pointer must
     remain valid at least that long. The pointer would normally
     reference version::disk_addr of the version this log record
     corresponds to, in which case lifetime requirements are met.

     WARNING: The caller cannot assume a record is durable just
     because it has been assigned a location.
  */
  void log_insert(FID f, OID o, fat_ptr p, int abits, fat_ptr *pdest);

  /* Record an insert to the index. p stores a pointer to the key value
   */
  void log_insert_index(FID f, OID o, fat_ptr p, int abits, fat_ptr *pdest);

  /* Record an update. Like an insertion, except that the OID is
     assumed to already have been allocated.
   */
  void log_update(FID f, OID o, fat_ptr p, int abits, fat_ptr *pdest);
  void log_update_key(FID f, OID o, fat_ptr p, int abits);

  /* Record a change in a record's on-disk location, to the address
     indicated. The OID remains the same and the data for the new
     location is already durable. Unlike an insertion or update, the
     new version's contents are not logged (being already durable).
  */
  void log_relocate(FID f, OID o, fat_ptr p, int abits);

  /* Record a deletion. During recovery, the OID slot is cleared and
     the OID deallocated.
  */
  void log_delete(FID f, OID o);
  void log_enhanced_delete(FID f, OID o, fat_ptr p, int abits);

  /* Record the creation of a table with tuple/key FIDs and name
   */
  void log_table(FID tuple_fid, FID key_fid, const std::string &name);

  /* Record the creation of an index
   */
  void log_index(FID table_fid, FID index_fid, const std::string &index_name, bool primary);

  /* Return this transaction's commit LSN, or INVALID_LSN if the
     transaction has not entered pre-commit yet.

     This function should be called by a transaction which already
     has a CLSN, and which wishes to determine whether it committed
     before or after this one. The implementation deals specifically
     with the race where the owner has acquired a CLSN but not yet
     published it, *and* where that CLSN is earlier than the one
     belonging to the caller.
   */
  LSN get_clsn();

  /* Acquire and return a commit LSN, but do not write the
     corresponding log records to disk yet. This function can safely
     be called multiple times to retrieve an existing commit LSN.

     NOTE: the commit LSN actually points past-end of the commit
     block, in keeping with cur_lsn and durable_flushed_lsn (which
     respectively identify the first LSN past-end of any currently
     in use, and the first LSN that is not durable).

     WARNING: log records cannot be added to the transaction after
     this call returns.
  */
  LSN pre_commit();

  /* Pre-commit succeeded. Log record(s) for this transaction can
     safely be made durable. Return the commit LSN. If [pdest] is
     non-NULL, fill it with the on-disk location of the commit
     block.

     It is not necessary to have called pre_commit first.

     NOTE: the transaction will not actually be durable until
     sm_log::durable_flushed_lsn catches up to the pre_commit LSN.

     WARNING: By calling this function, the caller gives up
     ownership of this object and should not access it again.
   */
  LSN commit(LSN *pdest);

  /* Transaction failed (perhaps even before pre-commit). Discard
     all log state and do not write anything to disk.

     WARNING: By calling this function, the caller gives up
     ownership of this object and should not access it again.
   */
  void discard();

private:
  sm_log *_log;
  size_t _nreq;  // includes a pending overflow record, if such is needed!!!
  size_t _payload_bytes;
  LSN _prev_overflow;
  log_allocation *_commit_block;

private:
  void add_request(log_request const &req);

  void add_payload_request(log_record_type type, FID f, OID o, fat_ptr p,
                           int abits, fat_ptr *pdest);
  void spill_overflow();
  void enter_precommit();

  log_allocation *_install_commit_block(log_allocation *a);
  void _populate_block(log_block *b);
  static size_t const MAX_BLOCK_RECORDS = 254;
  log_request log_requests[MAX_BLOCK_RECORDS];
};

}  // namespace ermia
