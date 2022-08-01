#include <gflags/gflags.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>

#include "../dbcore/rcu.h"
#include "bench.h"

#if defined(SSI) && defined(SSN)
#error "SSI + SSN?"
#endif

DEFINE_bool(threadpool, true,
            "Whether to use ERMIA thread pool (no oversubscription)");
DEFINE_uint64(arena_size_mb, 4,
              "Size of transaction arena (private workspace) in MB");
DEFINE_bool(tls_alloc, true,
            "Whether to use the TLS allocator defined in sm-alloc.h");
DEFINE_bool(htt, true,
            "Whether the HW has hyper-threading enabled."
            "Ignored if auto-detection of physical cores succeeded.");
DEFINE_bool(
    physical_workers_only, true,
    "Whether to only use one thread per physical core as transaction workers.");
DEFINE_bool(amac_version_chain, false,
            "Whether to use AMAC for traversing version chain; applicable only "
            "for multi-get.");
DEFINE_bool(coro_tx, false,
            "Whether to turn each transaction into a coroutine");
DEFINE_uint64(coro_batch_size, 5, "Number of in-flight coroutines");
DEFINE_bool(coro_batch_schedule, false,
            "Whether to run the same type of transactions per batch");
DEFINE_bool(scan_with_iterator, false,
            "Whether to run scan with iterator version or callback version");
DEFINE_bool(verbose, true, "Verbose mode.");
DEFINE_string(benchmark, "tpcc", "Benchmark name: tpcc, tpce, or ycsb");
DEFINE_string(benchmark_options, "", "Benchmark-specific opetions.");
DEFINE_bool(index_probe_only, false,
            "Whether the read is only probing into index");
DEFINE_uint64(threads, 1, "Number of worker threads to run transactions.");
DEFINE_uint64(node_memory_gb, 12, "GBs of memory to allocate per node.");
DEFINE_bool(numa_spread, false,
            "Whether to pin threads in spread mode (compact if false)");
DEFINE_string(tmpfs_dir, "/dev/shm",
              "Path to a tmpfs location. Used by log buffer.");
DEFINE_string(log_data_dir, "/tmpfs/ermia-log", "Log directory.");
DEFINE_uint64(log_buffer_kb, 256, "Log buffer size in KB");
DEFINE_uint64(log_segment_mb, 16384, "Log segment size in MB.");
DEFINE_bool(phantom_prot, false, "Whether to enable phantom protection.");
DEFINE_bool(print_cpu_util, false, "Whether to print CPU utilization.");
DEFINE_uint64(print_interval_ms, 1000, "Result print interval (ms)");
DEFINE_bool(enable_perf, false,
            "Whether to run Linux perf along with benchmark.");
DEFINE_string(perf_record_event, "", "Perf record event");
#if defined(SSN) || defined(SSI)
DEFINE_bool(safesnap, false,
            "Whether to use the safe snapshot (for SSI and SSN only).");
#endif
#ifdef SSN
DEFINE_string(ssn_read_opt_threshold, "0xFFFFFFFFFFFFFFFF",
              "Threshold for SSN's read optimization."
              "0 - don't track reads at all;"
              "0xFFFFFFFFFFFFFFFF - track all reads.");
#endif
#ifdef SSI
DEFINE_bool(ssi_read_only_opt, false,
            "Whether to enable SSI's read-only optimization."
            "Note: this is **not** safe snapshot.");
#endif

// Options specific to the primary
DEFINE_uint64(seconds, 10, "Duration to run benchmark in seconds.");
DEFINE_bool(parallel_loading, true, "Load data in parallel.");
DEFINE_bool(retry_aborted_transactions, false,
            "Whether to retry aborted transactions.");
DEFINE_bool(backoff_aborted_transactions, false,
            "Whether backoff when retrying.");
DEFINE_uint64(scale_factor, 1, "Scale factor.");
DEFINE_string(
    recovery_warm_up, "none",
    "Method to load tuples during recovery:"
    "none - don't load anything; lazy - load tuples using a background thread; "
    "eager - load everything to memory during recovery.");
DEFINE_bool(enable_chkpt, false, "Whether to enable checkpointing.");
DEFINE_uint64(chkpt_interval, 10, "Checkpoint interval in seconds.");
DEFINE_bool(null_log_device, false, "Whether to skip writing log records.");
DEFINE_bool(truncate_at_bench_start, false,
            "Whether truncate the log/chkpt file written before starting "
            "benchmark (save tmpfs space).");
DEFINE_bool(log_key_for_update, false,
            "Whether to store the key in update log records.");
// Group (pipelined) commit related settings. The daemon will flush the log
// buffer
// when the following happens, whichever is earlier:
// 1. queue is full; 2. the log buffer is half full; 3. after [timeout] seconds.
DEFINE_bool(pcommit, true, "Whether to enable pipelined commit.");
DEFINE_uint64(pcommit_queue_length, 25000, "Pipelined commit queue length");
DEFINE_uint64(pcommit_timeout_ms, 1000,
              "Pipelined commit flush interval (in milliseconds).");
DEFINE_uint64(pcommit_size_kb, 4,
              "Pipelined commit flush size interval in KB.");
DEFINE_bool(pcommit_thread, false,
            "Whether to use a dedicated pipelined committer thread.");
DEFINE_bool(enable_gc, false, "Whether to enable garbage collection.");
DEFINE_bool(always_load, false, "Whether to load versions from logs.");
DEFINE_bool(kStateRunning, false,
            "Whether the benchmark is in the running phase.");
DEFINE_bool(iouring_read_log, false,
            "Whether to use iouring to load versions from logs.");

// DDL & CDC settings
DEFINE_uint64(cdc_threads, 3, "Number of CDC threads");
DEFINE_bool(cdc_physical_workers_only, true,
            "Whether to use physical workers for CDC");
DEFINE_uint64(scan_threads, 3, "Number of scan threads");
DEFINE_bool(scan_physical_workers_only, true,
            "Whether to use physical workers for scan");
DEFINE_bool(enable_cdc_schema_lock, true,
            "Whether to lock schema records when CDC");
DEFINE_bool(enable_cdc_verification_test, false,
            "Whether enable CDC test for verification related DDL");
DEFINE_uint64(ddl_total, 1, "Number of DDL txns");
DEFINE_uint64(no_copy_verification_version_add, 1,
              "To which version we want to add to version when doing no copy "
              "verification");
DEFINE_bool(enable_ddl_keys, false, "Whether need maintain key arrays");
DEFINE_bool(enable_lazy_background, false,
            "Whether enable background migration for lazy DDL");
DEFINE_bool(enable_late_scan_join, false,
            "Whether enable join scan workers after commit");
DEFINE_bool(enable_parallel_scan_cdc, true,
            "Whether enable doing scan and CDC together");
DEFINE_uint64(client_load_per_core, 4000, "Client load per core per 100ms");
DEFINE_uint64(latency_stat_interval_ms, 1000, "Latency statistics interval (ms)");
DEFINE_bool(enable_lazy_on_conflict_do_nothing, false,
            "Whether enable ON CONFLICT DO NOTHING for lazy DDL");
DEFINE_uint64(late_background_start_ms, 1000, "When start background migration");
DEFINE_bool(enable_large_ddl_begin_timestamp, false,
            "Whether enable large ddl begin timestamp");
DEFINE_bool(commit_latency_only, true, "Whether enable only commit latency");
DEFINE_bool(ddl_retry, false, "Whether retry DDL");

static std::vector<std::string> split_ws(const std::string &s) {
  std::vector<std::string> r;
  std::istringstream iss(s);
  copy(std::istream_iterator<std::string>(iss),
       std::istream_iterator<std::string>(),
       std::back_inserter<std::vector<std::string>>(r));
  return r;
}

int main(int argc, char **argv) {
#ifndef NDEBUG
  std::cerr << "WARNING: benchmark built in DEBUG mode!!!" << std::endl;
#endif
  std::cerr << "PID: " << getpid() << std::endl;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  ermia::config::threadpool = FLAGS_threadpool;
  ermia::config::tls_alloc = FLAGS_tls_alloc;
  ermia::config::benchmark = FLAGS_benchmark;
  ermia::config::state = ermia::config::kStateLoading;
  ermia::config::print_cpu_util = FLAGS_print_cpu_util;
  ermia::config::print_interval_ms = FLAGS_print_interval_ms;
  ermia::config::htt_is_on = FLAGS_htt;
  ermia::config::enable_perf = FLAGS_enable_perf;
  ermia::config::perf_record_event = FLAGS_perf_record_event;
  ermia::config::physical_workers_only = FLAGS_physical_workers_only;
  ermia::config::cdc_physical_workers_only = FLAGS_cdc_physical_workers_only;
  ermia::config::scan_physical_workers_only = FLAGS_scan_physical_workers_only;

  ermia::config::replay_threads = 0;
  ermia::config::worker_threads = FLAGS_threads;
  ermia::config::cdc_threads = FLAGS_cdc_threads;
  ermia::config::scan_threads = FLAGS_scan_threads;
  ermia::config::enable_cdc_schema_lock = FLAGS_enable_cdc_schema_lock;
  ermia::config::enable_cdc_verification_test =
      FLAGS_enable_cdc_verification_test;
  ermia::config::ddl_total = FLAGS_ddl_total;
  ermia::config::no_copy_verification_version_add =
      FLAGS_no_copy_verification_version_add;
  ermia::config::enable_ddl_keys = FLAGS_enable_ddl_keys;
  ermia::config::enable_lazy_background = FLAGS_enable_lazy_background;
  ermia::config::enable_late_scan_join = FLAGS_enable_late_scan_join;
  ermia::config::enable_parallel_scan_cdc = FLAGS_enable_parallel_scan_cdc;
  ermia::config::client_load_per_core = FLAGS_client_load_per_core;
  ermia::config::latency_stat_interval_ms = FLAGS_latency_stat_interval_ms;
  ermia::config::enable_lazy_on_conflict_do_nothing = FLAGS_enable_lazy_on_conflict_do_nothing;
  ermia::config::late_background_start_ms = FLAGS_late_background_start_ms;
  ermia::config::enable_large_ddl_begin_timestamp = FLAGS_enable_large_ddl_begin_timestamp;
  ermia::config::commit_latency_only = FLAGS_commit_latency_only;
  ermia::config::ddl_retry = FLAGS_ddl_retry;

  if (ermia::config::physical_workers_only) {
#ifdef DDL
    ermia::config::threads = ermia::config::worker_threads;
    if (ermia::config::cdc_physical_workers_only) {
      ermia::config::threads += ermia::config::cdc_threads;
    }
    if (ermia::config::scan_physical_workers_only) {
      ermia::config::threads += ermia::config::scan_threads - 1;
    }
#else
    ermia::config::threads = FLAGS_threads;
#endif
  } else {
    ermia::config::threads = (FLAGS_threads + 1) / 2;
  }
  ermia::config::index_probe_only = FLAGS_index_probe_only;
  ermia::config::verbose = FLAGS_verbose;
  ermia::config::node_memory_gb = FLAGS_node_memory_gb;
  ermia::config::numa_spread = FLAGS_numa_spread;
  ermia::config::tmpfs_dir = FLAGS_tmpfs_dir;
  ermia::config::log_dir = FLAGS_log_data_dir;
  ermia::config::log_buffer_kb = FLAGS_log_buffer_kb;
  ermia::config::log_segment_mb = FLAGS_log_segment_mb;
  ermia::config::phantom_prot = FLAGS_phantom_prot;
  // ermia::config::recover_functor = new
  // ermia::parallel_oid_replay(FLAGS_threads);

  ermia::config::amac_version_chain = FLAGS_amac_version_chain;

#if defined(SSI) || defined(SSN)
  ermia::config::enable_safesnap = FLAGS_safesnap;
#endif
#ifdef SSI
  ermia::config::enable_ssi_read_only_opt = FLAGS_ssi_read_only_opt;
#endif
#ifdef SSN
  ermia::config::ssn_read_opt_threshold =
      strtoul(FLAGS_ssn_read_opt_threshold.c_str(), nullptr, 16);
#endif

  ermia::config::arena_size_mb = FLAGS_arena_size_mb;

  ermia::config::coro_tx = FLAGS_coro_tx;
  ermia::config::coro_batch_size = FLAGS_coro_batch_size;
  ermia::config::coro_batch_schedule = FLAGS_coro_batch_schedule;

  ermia::config::scan_with_it = FLAGS_scan_with_iterator;

  ermia::config::benchmark_seconds = FLAGS_seconds;
  ermia::config::benchmark_scale_factor = FLAGS_scale_factor;
  ermia::config::retry_aborted_transactions = FLAGS_retry_aborted_transactions;
  ermia::config::backoff_aborted_transactions =
      FLAGS_backoff_aborted_transactions;
  ermia::config::null_log_device = FLAGS_null_log_device;
  ermia::config::truncate_at_bench_start = FLAGS_truncate_at_bench_start;

  ermia::config::pcommit = FLAGS_pcommit;
  ermia::config::pcommit_queue_length = FLAGS_pcommit_queue_length;
  ermia::config::pcommit_timeout_ms = FLAGS_pcommit_timeout_ms;
  ermia::config::pcommit_size_kb = FLAGS_pcommit_size_kb;
  ermia::config::pcommit_bytes = FLAGS_pcommit_size_kb * 1024;
  ermia::config::pcommit_thread = FLAGS_pcommit_thread;
  ermia::config::enable_chkpt = FLAGS_enable_chkpt;
  ermia::config::chkpt_interval = FLAGS_chkpt_interval;
  ermia::config::parallel_loading = FLAGS_parallel_loading;
  ermia::config::enable_gc = FLAGS_enable_gc;
  ermia::config::always_load = FLAGS_always_load;
  ermia::config::kStateRunning = FLAGS_kStateRunning;
  ermia::config::iouring_read_log = FLAGS_iouring_read_log;

  if (FLAGS_recovery_warm_up == "none") {
    ermia::config::recovery_warm_up_policy = ermia::config::WARM_UP_NONE;
  } else if (FLAGS_recovery_warm_up == "lazy") {
    ermia::config::recovery_warm_up_policy = ermia::config::WARM_UP_LAZY;
  } else if (FLAGS_recovery_warm_up == "eager") {
    ermia::config::recovery_warm_up_policy = ermia::config::WARM_UP_EAGER;
  } else {
    LOG(FATAL) << "Invalid recovery warm up policy: " << FLAGS_recovery_warm_up;
  }

  ermia::config::log_key_for_update = FLAGS_log_key_for_update;

  ermia::thread::Initialize();
  ermia::config::init();

  std::cerr << "CC: ";
#ifdef SSI
  std::cerr << "SSI";
  std::cerr << "  safe snapshot          : " << ermia::config::enable_safesnap
            << std::endl;
  std::cerr << "  read-only optimization : "
            << ermia::config::enable_ssi_read_only_opt << std::endl;
#elif defined(SSN)
#ifdef RC
  std::cerr << "RC+SSN";
  std::cerr << "  safe snapshot          : " << ermia::config::enable_safesnap
            << std::endl;
  std::cerr << "  read opt threshold     : 0x" << std::hex
            << ermia::config::ssn_read_opt_threshold << std::dec << std::endl;
#else
  std::cerr << "SI+SSN";
  std::cerr << "  safe snapshot          : " << ermia::config::enable_safesnap
            << std::endl;
  std::cerr << "  read opt threshold     : 0x" << std::hex
            << ermia::config::ssn_read_opt_threshold << std::dec << std::endl;
#endif
#elif defined(MVCC)
  std::cerr << "MVOCC";
#else
  std::cerr << "SI";
#endif
  std::cerr << std::endl;
  std::cerr << "  phantom-protection: " << ermia::config::phantom_prot
            << std::endl;

  std::cerr << "Settings and properties" << std::endl;
  std::cerr << "  always-load       : " << FLAGS_always_load << std::endl;
  std::cerr << "  amac-version-chain: " << FLAGS_amac_version_chain
            << std::endl;
  std::cerr << "  arena-size-mb     : " << FLAGS_arena_size_mb << std::endl;
  std::cerr << "  benchmark         : " << FLAGS_benchmark << std::endl;
  std::cerr << "  coro-tx           : " << FLAGS_coro_tx << std::endl;
  std::cerr << "  coro-batch-schedule: " << FLAGS_coro_batch_schedule
            << std::endl;
  std::cerr << "  coro-batch-size   : " << FLAGS_coro_batch_size << std::endl;
  std::cerr << "  scan-use-iterator : " << FLAGS_scan_with_iterator
            << std::endl;
  std::cerr << "  enable-perf       : " << ermia::config::enable_perf
            << std::endl;
  std::cerr << "  pipelined commit  : " << ermia::config::pcommit << std::endl;
  std::cerr << "  dedicated pcommit thread: " << ermia::config::pcommit_thread
            << std::endl;
  std::cerr << "  index-probe-only  : " << FLAGS_index_probe_only << std::endl;
  std::cerr << "  iouring-read-log  : " << FLAGS_iouring_read_log << std::endl;
  std::cerr << "  log_buffer_kb     : " << ermia::config::log_buffer_kb
            << std::endl;
  std::cerr << "  log-dir           : " << ermia::config::log_dir << std::endl;
  std::cerr << "  log-segment-mb    : " << ermia::config::log_segment_mb
            << std::endl;
  std::cerr << "  masstree_internal_node_size: "
            << ermia::ConcurrentMasstree::InternalNodeSize() << std::endl;
  std::cerr << "  masstree_leaf_node_size    : "
            << ermia::ConcurrentMasstree::LeafNodeSize() << std::endl;
  std::cerr << "  node-memory       : " << ermia::config::node_memory_gb << "GB"
            << std::endl;
  std::cerr << "  null-log-device   : " << ermia::config::null_log_device
            << std::endl;
  std::cerr << "  num-threads       : " << ermia::config::threads << std::endl;
  std::cerr << "  numa-nodes        : " << ermia::config::numa_nodes
            << std::endl;
  std::cerr << "  numa-mode         : "
            << (ermia::config::numa_spread ? "spread" : "compact") << std::endl;
  std::cerr << "  perf-record-event : " << ermia::config::perf_record_event
            << std::endl;
  std::cerr << "  physical-workers-only: "
            << ermia::config::physical_workers_only << std::endl;
  std::cerr << "  print-cpu-util    : " << ermia::config::print_cpu_util
            << std::endl;
  std::cerr << "  print-interval-ms : " << ermia::config::print_interval_ms << std::endl;
  std::cerr << "  threadpool        : " << ermia::config::threadpool
            << std::endl;
  std::cerr << "  tmpfs-dir         : " << ermia::config::tmpfs_dir
            << std::endl;
  std::cerr << "  tls-alloc         : " << FLAGS_tls_alloc << std::endl;
  std::cerr << "  total-threads     : " << ermia::config::threads << std::endl;
#ifdef USE_VARINT_ENCODING
  std::cerr << "  var-encode        : yes" << std::endl;
#else
  std::cerr << "  var-encode        : no" << std::endl;
#endif
  std::cerr << "  worker-threads    : " << ermia::config::worker_threads
            << std::endl;
#ifdef DDL
  std::cerr << "DDL settings" << std::endl;
  std::cerr << "  scan-threads				: "
            << ermia::config::scan_threads << std::endl;
  std::cerr << "  scan_physical_workers_only		: "
            << ermia::config::scan_physical_workers_only << std::endl;
  std::cerr << "  cdc-threads				: "
            << ermia::config::cdc_threads << std::endl;
  std::cerr << "  cdc_physical_workers_only		: "
            << ermia::config::cdc_physical_workers_only << std::endl;
  std::cerr << "  enable_cdc_schema_lock		: "
            << ermia::config::enable_cdc_schema_lock << std::endl;
  std::cerr << "  enable_cdc_verification_test		: "
            << ermia::config::enable_cdc_verification_test << std::endl;
  std::cerr << "  ddl_total				: "
            << ermia::config::ddl_total << std::endl;
  std::cerr << "  no_copy_verification_version_add	: "
            << ermia::config::no_copy_verification_version_add << std::endl;
  std::cerr << "  enable_ddl_keys			: "
            << ermia::config::enable_ddl_keys << std::endl;
  std::cerr << "  enable_lazy_background		: "
            << ermia::config::enable_lazy_background << std::endl;
  std::cerr << "  enable_late_scan_join			: "
            << ermia::config::enable_late_scan_join << std::endl;
  std::cerr << "  enable_parallel_scan_cdc              : "
            << ermia::config::enable_parallel_scan_cdc << std::endl;
  std::cerr << "  client_load_per_core                  : "
            << ermia::config::client_load_per_core << std::endl;
  std::cerr << "  latency_stat_interval_ms              : "
            << ermia::config::latency_stat_interval_ms << std::endl;
  std::cerr << "  enable_lazy_on_conflict_do_nothing    : "
            << ermia::config::enable_lazy_on_conflict_do_nothing << std::endl;
  std::cerr << "  late_background_start_ms              : "
            << ermia::config::late_background_start_ms << std::endl;
  std::cerr << "  enable_large_ddl_begin_timestamp      : "
            << ermia::config::enable_large_ddl_begin_timestamp << std::endl;
  std::cerr << "  commit_latency_only                   : "
            << ermia::config::commit_latency_only << std::endl;
  std::cerr << "  ddl_retry                             : "
            << ermia::config::ddl_retry << std::endl;
#endif

  system("rm -rf /dev/shm/$(whoami)/ermia-log/*");
  ermia::MM::prepare_node_memory();
  std::vector<std::string> bench_toks = split_ws(FLAGS_benchmark_options);
  argc = 1 + bench_toks.size();
  char *new_argv[argc];
  new_argv[0] = (char *)FLAGS_benchmark.c_str();
  for (size_t i = 1; i <= bench_toks.size(); i++)
    new_argv[i] = (char *)bench_toks[i - 1].c_str();

  // Must have everything in config ready by this point
  ermia::config::sanity_check();
  ermia::Engine *db = new ermia::Engine();
  void (*test_fn)(ermia::Engine *, int argc, char **argv) = NULL;
  if (FLAGS_benchmark == "ycsb") {
#ifdef ADV_COROUTINE
    ALWAYS_ASSERT(ermia::config::coro_tx);
    test_fn = ycsb_cs_advance_do_test;
#else
    if (ermia::config::coro_tx) {
      test_fn = ycsb_cs_do_test;
    } else {
      test_fn = ycsb_do_test;
    }
#endif
  } else if (FLAGS_benchmark == "tpcc") {
#ifndef ADV_COROUTINE
    test_fn = tpcc_do_test;
#else
    LOG(FATAL) << "Not supported in this build";
#endif
  } else if (FLAGS_benchmark == "tpce") {
    LOG(FATAL) << "Not supported in this build";
  } else if (FLAGS_benchmark == "oddl") {
    test_fn = oddlb_do_test;
  } else {
    LOG(FATAL) << "Invalid benchmark: " << FLAGS_benchmark;
  }

  // FIXME(tzwang): the current thread doesn't belong to the thread pool, and
  // it could be on any node. But not all nodes will be used by benchmark
  // (i.e., config::numa_nodes) and so not all nodes will have memory pool. So
  // here run on the first NUMA node to ensure we got a place to allocate memory
  numa_run_on_node(0);
  test_fn(db, argc, new_argv);
  delete db;
  return 0;
}
