#pragma once
#include <numa.h>
#include <x86intrin.h>

#include <iostream>
#include <string>

#include "sm-defs.h"

namespace ermia {

namespace config {

static const uint32_t MAX_THREADS = 96;
static const uint32_t MAX_COROS = 8;
static const uint64_t MB = 1024 * 1024;
static const uint64_t GB = MB * 1024;

// Common settings
extern bool tls_alloc;
extern bool threadpool;
extern bool verbose;
extern std::string benchmark;
extern uint32_t benchmark_scale_factor;
extern uint32_t threads;
extern uint32_t worker_threads;
extern int numa_nodes;
extern bool numa_spread;
extern std::string tmpfs_dir;
extern bool htt_is_on;
extern bool physical_workers_only;
extern uint32_t state;
extern bool enable_chkpt;
extern uint64_t chkpt_interval;
extern uint64_t log_buffer_kb;
extern uint64_t log_segment_mb;
extern std::string log_dir;
extern bool print_cpu_util;
extern uint32_t arena_size_mb;
extern bool enable_perf;
extern std::string perf_record_event;
extern uint32_t replay_threads;
extern bool always_load;
extern bool kStateRunning;
extern bool iouring_read_log;

extern uint64_t node_memory_gb;
extern bool phantom_prot;

// Primary-specific settings
extern bool parallel_loading;
extern bool retry_aborted_transactions;
extern int backoff_aborted_transactions;
extern int enable_gc;
extern bool null_log_device;
extern bool truncate_at_bench_start;
extern bool pcommit;
extern uint32_t pcommit_timeout_ms;
extern uint32_t pcommit_queue_length;  // how much to reserve
extern uint64_t pcommit_size_kb;
extern uint64_t pcommit_bytes;
extern bool pcommit_thread;

extern uint32_t benchmark_seconds;

// CoroBase-specific settings
extern bool index_probe_only;
extern bool coro_tx;
extern uint32_t coro_batch_size;
extern bool coro_batch_schedule;

extern bool scan_with_it;

// DDL & CDC settings
extern uint32_t cdc_threads;
extern bool cdc_physical_workers_only;
extern uint32_t scan_threads;
extern bool scan_physical_workers_only;
extern bool enable_cdc_schema_lock;
extern bool enable_cdc_verification_test;
extern uint32_t ddl_total;
extern uint32_t no_copy_verification_version_add;
extern bool enable_ddl_keys;
extern bool enable_lazy_background;
extern bool enable_late_scan_join;
extern bool enable_parallel_scan_cdc;
extern uint32_t client_load_per_core; // per 100ms
extern uint32_t latency_stat_interval_ms;
extern bool enable_lazy_on_conflict_do_nothing;

extern uint64_t print_interval_ms;

// Create an object for each version and install directly on the main
// indirection arrays only; for experimental purpose only to see the
// difference between pipelined/sync replay which use the pdest array.
extern bool full_replay;

enum SystemState { kStateLoading, kStateForwardProcessing, kStateShutdown };
inline bool IsLoading() { return volatile_read(state) == kStateLoading; }
inline bool IsForwardProcessing() {
  return volatile_read(state) == kStateForwardProcessing;
}
inline bool IsShutdown() { return volatile_read(state) == kStateShutdown; }

// Warm-up policy when recovering from a chkpt or the log.
// Set by --recovery-warm-up=[lazy/eager/whatever].
//
// lazy: spawn a thread to access every OID entry after recovery; log/chkpt
//       recovery will only oid_put objects that contain the records' log
//       location.
//       Tx's might encounter some storage-resident versions, if the tx tried
//       to
//       access them before the warm-up thread fetched those versions.
//
// eager: dig out versions from the log when scanning the chkpt and log; all
// OID
//        entries will point to some memory location after recovery finishes.
//        Txs will only see memory-residents, no need to dig them out during
//        execution.
//
// --recovery-warm-up ommitted or = anything else: don't do warm-up at all; it
//        is the tx's burden to dig out versions when accessing them.
enum WU_POLICY { WARM_UP_NONE, WARM_UP_LAZY, WARM_UP_EAGER };
extern int recovery_warm_up_policy;  // no/lazy/eager warm-up at recovery

/* CC-related options */
extern int enable_ssi_read_only_opt;
extern uint64_t ssn_read_opt_threshold;
static const uint64_t SSN_READ_OPT_DISABLED = 0xffffffffffffffff;

// XXX(tzwang): enabling safesnap for tpcc basically halves the performance.
// perf says 30%+ of cycles are on oid_get_version, which makes me suspect
// it's because enabling safesnap makes the reader has to go deeper in the
// version chains to find the desired version. So perhaps don't enable this
// for update-intensive workloads, like tpcc. TPC-E to test and verify.
extern int enable_safesnap;

extern bool log_key_for_update;

extern bool amac_version_chain;

extern double cycles_per_byte;

inline bool eager_warm_up() { return recovery_warm_up_policy == WARM_UP_EAGER; }

inline bool lazy_warm_up() { return recovery_warm_up_policy == WARM_UP_LAZY; }

void init();
void sanity_check();
inline bool ssn_read_opt_enabled() {
  return ssn_read_opt_threshold < SSN_READ_OPT_DISABLED;
}

}  // namespace config
}  // namespace ermia
