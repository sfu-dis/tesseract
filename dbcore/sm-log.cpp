#include <cstring>
#include "sm-log-offset.h"
#include "sm-oid.h"
#include "sm-thread.h"

namespace ermia {

sm_log *logmgr = NULL;
bool sm_log::need_recovery = false;
window_buffer *sm_log::logbuf = nullptr;

void sm_log::allocate_log_buffer() {
  logbuf = new window_buffer(config::log_buffer_mb * config::MB);
}

segment_id *sm_log::get_offset_segment(uint64_t off) {
  return _lm._lm.get_offset_segment(off);
}

segment_id *sm_log::get_segment(uint32_t segnum) {
  return _lm._lm.get_segment(segnum);
}

LSN sm_log::get_chkpt_start() {
  return _lm._lm.get_chkpt_start();
}

void sm_log::set_tls_lsn_offset(uint64_t offset) {
  _lm.set_tls_lsn_offset(offset);
}

uint64_t sm_log::get_tls_lsn_offset() {
  return _lm.get_tls_lsn_offset();
}

window_buffer *sm_log::get_logbuf() { return sm_log::logbuf; }

void sm_log::redo_log(LSN start_lsn, LSN end_lsn) {
  _lm._lm.redo_log(start_lsn, end_lsn);
}

void sm_log::recover() { _lm._lm.recover(); }

segment_id *sm_log::assign_segment(uint64_t lsn_begin, uint64_t lsn_end) {
  auto rval = _lm._lm.assign_segment(lsn_begin, lsn_end);
  ALWAYS_ASSERT(rval.full_size);
  return rval.sid;
}

void sm_log::enqueue_committed_xct(uint32_t worker_id, uint64_t start_time) {
  _lm.enqueue_committed_xct(worker_id, start_time);
}

LSN sm_log::flush() { return _lm.flush(); }

void sm_log::update_chkpt_mark(LSN cstart, LSN cend) {
  _lm._lm.update_chkpt_mark(cstart, cend);
}

void sm_log::load_object(char *buf, size_t bufsz, fat_ptr ptr,
                         size_t align_bits) {
  _lm._lm.load_object(buf, bufsz, ptr, align_bits);
}

fat_ptr sm_log::load_ext_pointer(fat_ptr ptr) {
  return _lm._lm.load_ext_pointer(ptr);
}

int sm_log::open_segment_for_read(segment_id *sid) {
  return _lm._lm.open_for_read(sid);
}

sm_log *sm_log::new_log(sm_log_recover_impl *recover_functor, void *rarg) {
  need_recovery = false;
  if (config::null_log_device) {
    dirent_iterator iter(config::log_dir.c_str());
    for (char const *fname : iter) {
      if (strcmp(fname, ".") and strcmp(fname, ".."))
        os_unlinkat(iter.dup(), fname);
    }
  }
  ALWAYS_ASSERT(config::log_segment_mb);
  ALWAYS_ASSERT(config::log_buffer_mb);
  return new sm_log(recover_functor, rarg);
}

sm_log_scan_mgr *sm_log::get_scan_mgr() {
  return _lm._lm.scanner;
}

sm_tx_log *sm_log::new_tx_log(char *log_space) {
  return new (log_space) sm_tx_log(this);
}

fat_ptr sm_log::lsn2ptr(LSN lsn, bool is_ext) {
  return _lm._lm.lsn2ptr(lsn, is_ext);
}

LSN sm_log::ptr2lsn(fat_ptr ptr) { return _lm._lm.ptr2lsn(ptr); }

LSN sm_log::cur_lsn() {
  auto *log = &_lm;
  auto offset = log->cur_lsn_offset();
  auto *sid = log->_lm.get_offset_segment(offset);

  if (not sid) {
  /* must have raced a new segment opening */
  /*
  while (1) {
          sid = log->_lm._newest_segment();
          if (sid->start_offset >= offset)
                  break;
  }
  */

  retry:
    sid = log->_lm._newest_segment();
    ASSERT(sid);
    if (offset < sid->start_offset)
      offset = sid->start_offset;
    else if (sid->end_offset <= offset) {
      goto retry;
    }
  }
  return sid->make_lsn(offset);
}

void sm_log::dequeue_committed_xcts(uint64_t upto, uint64_t end_time) {
  LOG_IF(FATAL, !config::command_log) << "For command logging only";
  auto *log = &_lm;
  log->dequeue_committed_xcts(upto, end_time);
}

LSN sm_log::durable_flushed_lsn() {
  auto *log = &_lm;
  auto offset = log->dur_flushed_lsn_offset();
  auto *sid = log->_lm.get_offset_segment(offset);
  ASSERT(!sid || sid->start_offset <= offset);

  if (!sid) {
  retry:
    sid = log->_lm._newest_segment();
    ASSERT(sid);
    if (offset < sid->start_offset) {
      offset = sid->start_offset;
    } else if (sid->end_offset <= offset) {
      goto retry;
    }
  }
  ASSERT(sid);
  ASSERT(sid->start_offset <= offset);
  return sid->make_lsn(offset);
}

uint64_t sm_log::durable_flushed_lsn_offset() {
  return _lm.dur_flushed_lsn_offset();
}

void sm_log::wait_for_durable_flushed_lsn_offset(uint64_t offset) {
  _lm.wait_for_durable(offset);
}
}  // namespace ermia
