#include <unistd.h>
#include <numa.h>
#include "../macros.h"
#include "sm-config.h"
#include "sm-thread.h"
#include <iostream>

namespace ermia {
namespace config {

uint32_t arena_size_mb = 4;
bool threadpool = true;
bool tls_alloc = true;
bool verbose = true;
bool coro_tx = false;
uint32_t coro_batch_size = 1;
bool coro_batch_schedule = false;
bool scan_with_it = false;
std::string benchmark("");
uint32_t worker_threads = 0;
uint32_t benchmark_seconds = 30;
uint32_t benchmark_scale_factor = 1;
bool parallel_loading = false;
bool retry_aborted_transactions = false;
bool quick_bench_start = false;
int backoff_aborted_transactions = 0;
int numa_nodes = 0;
int enable_gc = 0;
std::string tmpfs_dir("/dev/shm");
int enable_safesnap = 0;
int enable_ssi_read_only_opt = 0;
uint64_t ssn_read_opt_threshold = SSN_READ_OPT_DISABLED;
uint64_t log_buffer_mb = 4;
uint64_t log_segment_mb = 16384;
std::string log_dir("");
bool null_log_device = false;
bool truncate_at_bench_start = false;
bool htt_is_on = true;
bool physical_workers_only = true;
bool print_cpu_util = false;
bool enable_perf = false;
std::string perf_record_event("");
uint64_t node_memory_gb = 12;
int recovery_warm_up_policy = WARM_UP_NONE;
bool pcommit = false;
uint32_t pcommit_queue_length = 25000;
uint32_t pcommit_timeout_ms = 1000;
uint64_t pcommit_size_kb = 4096;
uint64_t pcommit_bytes = 4096 * 1024;
bool pcommit_thread = false;
bool log_key_for_update = false;
bool enable_chkpt = 0;
uint64_t chkpt_interval = 50;
bool phantom_prot = 0;
double cycles_per_byte = 0;
uint32_t state = kStateLoading;
bool full_replay = false;
uint32_t replay_threads = 0;
uint32_t threads = 0;
bool index_probe_only = false;
bool amac_version_chain = false;
bool numa_spread = false;
uint32_t cdc_threads = 0;
bool cdc_physical_workers_only = true;
uint32_t scan_threads = 0;
bool scan_physical_workers_only = true;
bool enable_cdc_schema_lock = true;
uint32_t ddl_type = 1;
bool enable_cdc_verification_test = false;
bool enable_dml_slow_down = true;
uint32_t ddl_num_total = 1;
uint32_t ddl_start_time = 1;
uint32_t no_copy_verification_version_add = 1;
uint32_t ddl_example = 0;
bool enable_ddl_keys = false;
bool always_load = false;
bool kStateRunning = false;
bool iouring_read_log = false;

void init() {
  ALWAYS_ASSERT(threads);
  // Here [threads] refers to worker threads, so use the number of physical cores
  // to calculate # of numa nodes
  if (numa_spread) {
    numa_nodes = threads > numa_max_node() + 1 ? numa_max_node() + 1 : threads;
  } else {
    uint32_t max = thread::cpu_cores.size() / (numa_max_node() + 1);
    numa_nodes = (threads + max - 1) / max;
    ALWAYS_ASSERT(numa_nodes);
  }

  LOG(INFO) << "Workloads may run on " << numa_nodes << " nodes";
}

void sanity_check() {
  LOG_IF(FATAL, tls_alloc && !threadpool) << "Cannot use TLS allocator without threadpool";
  //ALWAYS_ASSERT(recover_functor);
  ALWAYS_ASSERT(numa_nodes || !threadpool);
  ALWAYS_ASSERT(!pcommit || pcommit_queue_length);
}

}  // namespace config
}  // namespace ermia
