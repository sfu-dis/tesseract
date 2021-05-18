#include "../engine.h"
#include "rcu.h"
#include "sm-table.h"
#include "sm-log-recover-impl.h"
#include "sm-oid.h"
#include "sm-oid-alloc.h"

namespace ermia {

LSN parallel_oid_replay::operator()(void *arg, sm_log_scan_mgr *s,
                                    LSN from, LSN to) {
  MARK_REFERENCED(arg);
  //util::scoped_timer t("parallel_oid_replay");
  scanner = s;
  start_lsn = from;
  end_lsn = to;

  RCU::rcu_enter();
  // Look for new table creations after the chkpt
  // Use one redo thread per new table found
  // XXX(tzwang): no support for dynamically created tables for now
  // TODO(tzwang): figure out how this interacts with chkpt

  // One hiwater_mark/capacity_mark per FID
  FID max_fid = 0;
  if (redoers.size() == 0) {
    auto *scan =
        scanner->new_log_scan(start_lsn, config::eager_warm_up(), false);
    for (; scan->valid() and scan->payload_lsn() < end_lsn; scan->next()) {
      if (scan->type() != sm_log_scan_mgr::LOG_FID) continue;
      FID fid = scan->fid();
      max_fid = std::max(fid, max_fid);
      recover_fid(scan);
    }
    delete scan;
  }

  if (redoers.size() == 0) {
    for (uint32_t i = 0; i < nredoers; ++i) {
      redoers.emplace_back(this, i);
    }
  }

  // Fix internal files' marks
  oidmgr->recreate_allocator(sm_oid_mgr::OBJARRAY_FID, max_fid);
  oidmgr->recreate_allocator(sm_oid_mgr::ALLOCATOR_FID, max_fid);
  // oidmgr->recreate_allocator(sm_oid_mgr::METADATA_FID, max_fid);

  uint32_t done = 0;
  LSN replayed_lsn = INVALID_LSN;
process:
  for (auto &r : redoers) {
    // Scan the rest of the log
    if (not r.done and not r.IsImpersonated() and r.TryImpersonate()) {
      r.Start();
    }
  }

  // Loop over existing redoers to scavenge and reuse available threads
  while (done < redoers.size()) {
    for (auto &r : redoers) {
      if (r.IsImpersonated() and r.TryJoin()) {
        if (r.replayed_lsn > replayed_lsn) {
          replayed_lsn = r.replayed_lsn;
        }
        if (++done < redoers.size()) {
          goto process;
        } else {
          break;
        }
      }
    }
  }

  // Reset redoer states for reuse
  for (auto &r : redoers) {
    r.done = false;
  }

  // WARNING: DO NOT TAKE CHKPT UNTIL WE REPLAYED ALL INDEXES!
  // Otherwise we migth lose some FIDs/OIDs created before the chkpt.
  //
  // For easier measurement (like "how long does it take to bring the
  // system back to fully memory-resident after recovery), we spawn the
  // warm-up thread after rebuilding indexes as well.
  if (config::lazy_warm_up()) {
    oidmgr->start_warm_up();
  }

  return replayed_lsn;
}

void parallel_oid_replay::redo_runner::redo_partition() {
  RCU::rcu_enter();
  uint64_t icount = 0, ucount = 0, size = 0, iicount = 0, dcount = 0;
  ALWAYS_ASSERT(owner->start_lsn.segment() >= 1);
  auto *scan = owner->scanner->new_log_scan(owner->start_lsn,
                                            config::eager_warm_up(), false);
  static thread_local std::unordered_map<FID, OID> max_oid;
  replayed_lsn = INVALID_LSN;

  for (; scan->valid() and scan->payload_lsn().offset() + scan->payload_size() <= owner->end_lsn.offset(); scan->next()) {
    replayed_lsn = scan->block_lsn();

    auto oid = scan->oid();
    if (oid % owner->redoers.size() != oid_partition) continue;

    auto fid = scan->fid();
    max_oid[fid] = std::max(max_oid[fid], oid);

    switch (scan->type()) {
      case sm_log_scan_mgr::LOG_UPDATE_KEY:
        owner->recover_update_key(scan);
        break;
      case sm_log_scan_mgr::LOG_UPDATE:
      case sm_log_scan_mgr::LOG_RELOCATE:
        ucount++;
        owner->recover_update(scan, false, false);
        break;
      case sm_log_scan_mgr::LOG_DELETE:
      case sm_log_scan_mgr::LOG_ENHANCED_DELETE:
        // Ignore delete on primary server
        dcount++;
        break;
      case sm_log_scan_mgr::LOG_INSERT_INDEX:
        iicount++;
        owner->recover_index_insert(scan);
        break;
      case sm_log_scan_mgr::LOG_INSERT:
        icount++;
        owner->recover_insert(scan, false);
        break;
      case sm_log_scan_mgr::LOG_FID:
        // The main recover function should have already did this
        ASSERT(oidmgr->file_exists(scan->fid()));
        break;
      default:
        DIE("unreachable");
    }
    size += scan->payload_size();
  }
  ASSERT(icount <= iicount);  // No insert log record for 2nd index
  DLOG(INFO) << "[Recovery.log] OID partition " << oid_partition
             << " - inserts/updates/deletes/size: " << icount << "/" << ucount
             << "/" << dcount << "/" << size;

  for (auto &m : max_oid) {
    oidmgr->recreate_allocator(m.first, m.second);
  }

  delete scan;
  RCU::rcu_exit();
}

void parallel_oid_replay::redo_runner::MyWork(char *) {
  redo_partition();
  done = true;
  __sync_synchronize();
}
}  // namespace ermia
