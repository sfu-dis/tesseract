#pragma once

/* A high-performance log manager for an append-only system.

   This is a redo-only log, as it records only committed changes
   (*). Actually, it's not even much of a redo log; really the system
   just has to bring the OID array back up to date from the most
   recent checkpoint.

   (*) Uncommitted changes can be logged, but are recorded in such a
   way that they will be ignord during reply unless their owning
   transaction commits.

 */
#include <unordered_map>
#include "sm-log-alloc.h"
#include "sm-log-recover.h"
#include "sm-tx-log.h"
#include "sm-log-scan.h"

#include "sm-thread.h"
#include "window-buffer.h"

namespace ermia {
struct segment_id;
struct sm_log_recover_impl;

struct sm_log {
  static bool need_recovery;

  static window_buffer *logbuf;

  void update_chkpt_mark(LSN cstart, LSN cend);
  LSN flush();
  void set_tls_lsn_offset(uint64_t offset);
  uint64_t get_tls_lsn_offset();

  /* Allocate and return a new sm_log object. If [dname] exists, it
     will be mounted and used. Otherwise, a new (empty) log
     directory will be created.
   */
  static sm_log *new_log(sm_log_recover_impl *recover_functor, void *rarg);

  static void allocate_log_buffer();

  /* Return a pointer to the log's scan manager.

     The caller should *not* delete it when finished.
   */
  sm_log_scan_mgr *get_scan_mgr();

  /* Allocate a new transaction log tracker. All logging occurs
     through this interface.

     WARNING: the caller is responsible to eventually call commit()
     or discard() on the returned object, or risk stalling the log.
   */
  sm_tx_log *new_tx_log(char *log_space);

  /* Return the current LSN. This is the LSN that the next
     successful call to allocate() will acquire.
   */
  LSN cur_lsn();

  /* Return the current durable LSN. This is the LSN before which
     all log records are known to have reached stable storage; any
     LSN at or beyond this point may not be durable yet. If
     cur_lsn() == durable_flushed_lsn(), all log records are durable.
   */
  LSN durable_flushed_lsn();

  /* Block the calling thread until durable_flushed_lsn() is not smaller
     than [dlsn]. This will not occur until all log_allocation
     objects with LSN smaller than [dlsn] have been released or
     discarded.
   */
  void wait_for_durable_flushed_lsn_offset(uint64_t offset);

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

  segment_id *get_offset_segment(uint64_t off);
  LSN get_chkpt_start();
  static window_buffer *get_logbuf();
  segment_id *assign_segment(uint64_t lsn_begin, uint64_t lsn_end);
  segment_id *get_segment(uint32_t segnum);
  void redo_log(LSN start_lsn, LSN end_lsn);
  void start_logbuf_redoers();
  void recover();
  void enqueue_committed_xct(uint32_t worker_id, uint64_t start_time);
  uint64_t durable_flushed_lsn_offset();
  int open_segment_for_read(segment_id *sid);
  void dequeue_committed_xcts(uint64_t upto, uint64_t end_time);

  ~sm_log() {}
  sm_log(sm_log_recover_impl *rf, void *rarg) : _lm(rf, rarg) {}

  /* Convert the given LSN into a fat_ptr that can be used to access
     the corresponding log record.
   */
  fat_ptr lsn2ptr(LSN lsn, bool is_ext);

  /* Convert a fat_ptr into the LSN it corresponds to.

     Throw illegal_argument if the pointer does not correspond to
     any LSN.
   */
  LSN ptr2lsn(fat_ptr ptr);

  sm_log_alloc_mgr _lm;
};

// Global log manager instance
extern sm_log *logmgr;

}  // namespace ermia
