#include "bench.h"

#include <stdlib.h>
#include <sys/sysinfo.h>
#include <sys/times.h>
#include <sys/wait.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "../dbcore/rcu.h"
#include "../dbcore/sm-config.h"
#include "../dbcore/sm-table.h"

volatile bool running = true;
std::vector<bench_worker *> bench_runner::workers;
volatile int ddl_done = 0;
volatile int ddl_worker_id = -1;
volatile bool ddl_start = false;
volatile unsigned ddl_start_times[8];
volatile unsigned ddl_examples[8];

thread_local ermia::epoch_num coroutine_batch_end_epoch = 0;

void bench_worker::do_workload_function(uint32_t i) {
  ASSERT(workload.size());
retry:
  util::timer t;
  const unsigned long old_seed = r.get_seed();
  const auto ret = workload[i].fn(this);
  if (finish_workload(ret, i, t)) {
    r.set_seed(old_seed);
    goto retry;
  }
}

void bench_worker::do_ddl_workload_function(uint32_t i) {
  ASSERT(ddl_workload.size());
retry:
  util::timer t;
  const unsigned long old_seed = r.get_seed();
  const auto ret = ddl_workload[i].ddl_fn(this, ddl_workload[i].ddl_example);
  if (finish_workload(ret, i, t)) {
    r.set_seed(old_seed);
    goto retry;
  }
}

uint32_t bench_worker::fetch_workload() {
  double d = r.next_uniform();
  for (size_t i = 0; i < workload.size(); i++) {
    if ((i + 1) == workload.size() || d < workload[i].frequency) {
      return i;
    }
    d -= workload[i].frequency;
  }

  // unreachable
  return 0;
}

bool bench_worker::finish_workload(rc_t ret, uint32_t workload_idx,
                                   util::timer t) {
  if (!ret.IsAbort()) {
    ++ntxn_commits;
    std::get<0>(txn_counts[workload_idx])++;
    if (!ermia::config::pcommit) {
      latency_numer_us += t.lap();
    }
    backoff_shifts >>= 1;
  } else {
    ++ntxn_aborts;
    std::get<1>(txn_counts[workload_idx])++;
    if (ret._val == RC_ABORT_USER) {
      std::get<3>(txn_counts[workload_idx])++;
    } else {
      std::get<2>(txn_counts[workload_idx])++;
    }
    switch (ret._val) {
      case RC_ABORT_SERIAL:
        inc_ntxn_serial_aborts();
        break;
      case RC_ABORT_SI_CONFLICT:
        inc_ntxn_si_aborts();
        break;
      case RC_ABORT_RW_CONFLICT:
        inc_ntxn_rw_aborts();
        break;
      case RC_ABORT_INTERNAL:
        inc_ntxn_int_aborts();
        break;
      case RC_ABORT_PHANTOM:
        inc_ntxn_phantom_aborts();
        break;
      case RC_ABORT_USER:
        inc_ntxn_user_aborts();
        break;
      default:
        ALWAYS_ASSERT(false);
    }
    if (ermia::config::retry_aborted_transactions && !ret.IsUserAbort() &&
        running) {
      if (ermia::config::backoff_aborted_transactions) {
        if (backoff_shifts < 63) backoff_shifts++;
        uint64_t spins = 1UL << backoff_shifts;
        spins *= 100;  // XXX: tuned pretty arbitrarily
        while (spins) {
          NOP_PAUSE;
          spins--;
        }
      }
      return true;
    }
  }
  return false;
}

void bench_worker::MyWork(char *) {
  if (is_worker) {
    tlog = ermia::GetLog();
    // Set _tls_durable_csn to be current global_durable_csn to
    // mark logs as active for cdc use
    tlog->reset_committer(false);
    workload = get_workload();
    ddl_workload = get_ddl_workload();
    txn_counts.resize(workload.size());
    barrier_a->count_down();
    barrier_b->wait_for();

    while (running) {
      if (worker_id == ddl_worker_id && ddl_done < ermia::config::ddl_total &&
          ddl_start) {
        util::timer ddl_timer;
        do_ddl_workload_function(ddl_done);
        double lap = ddl_timer.lap();
        DLOG(INFO) << "DDL duration: " << lap / 1000000.0 << "s" << std::endl;
        ddl_done++;
        ddl_start = false;
        ermia::ddl::ddl_start = false;
      } else {
        uint32_t workload_idx = fetch_workload();
        do_workload_function(workload_idx);
      }
    }
  }
}

void bench_runner::run() {
  // Get a thread to use benchmark-provided prepare(), which gathers
  // information about index pointers created by create_file_task.
  ermia::thread::Thread::Task runner_task =
      std::bind(&bench_runner::prepare, this, std::placeholders::_1);
  ermia::thread::Thread *runner_thread =
      ermia::thread::GetThread(true /* physical */);
  runner_thread->StartTask(runner_task);
  runner_thread->Join();
  ermia::thread::PutThread(runner_thread);

  // load data, unless we recover from logs or is a backup server (recover from
  // shipped logs)

  std::vector<bench_loader *> loaders = make_loaders();
  {
    util::scoped_timer t("dataloading", ermia::config::verbose);
    uint32_t done = 0;
    uint32_t n_running = 0;
  process:
    // force bench_loader to use physical thread and use all the physical
    // threads in the same socket to load (assuming 2HT per core).
    uint32_t n_loader_threads = std::thread::hardware_concurrency() /
                                (numa_max_node() + 1) / 2 *
                                ermia::config::numa_nodes;

    for (uint i = 0; i < loaders.size(); i++) {
      auto *loader = loaders[i];
      // Note: the thread pool creates threads for each hyperthread regardless
      // of how many worker threads will be running the benchmark. We don't
      // want to use threads on sockets that we won't be running benchmark on
      // for loading (that would cause some records' memory to become remote).
      // E.g., on a 40-core, 4 socket machine the thread pool will create 80
      // threads waiting to be dispatched. But if our workload only wants to
      // run 10 threads on the first socket, we won't want the loader to be run
      // on a thread from socket 2. So limit the number of concurrently running
      // loaders to the number of workers.
      if (loader && !loader->IsImpersonated() && n_running < n_loader_threads &&
          loader->TryImpersonate()) {
        loader->Start();
        ++n_running;
      }
    }

    // Loop over existing loaders to scavenge and reuse available threads
    while (done < loaders.size()) {
      for (uint i = 0; i < loaders.size(); i++) {
        auto *loader = loaders[i];
        if (loader and loader->IsImpersonated() and loader->TryJoin()) {
          delete loader;
          loaders[i] = nullptr;
          done++;
          --n_running;
          goto process;
        }
      }
    }
  }

  // FIXME:SSI safesnap to work with CSN.
  ermia::volatile_write(ermia::MM::safesnap_lsn, ermia::dlog::current_csn);
  ALWAYS_ASSERT(ermia::MM::safesnap_lsn);

  // Persist the database
  ermia::dlog::flush_all();
  if (ermia::config::pcommit) {
    ermia::dlog::dequeue_committed_xcts();
    // Sanity check to make sure all transactions are fully committed

    for (auto &tlog : ermia::dlog::tlogs) {
      LOG_IF(FATAL, tlog->get_commit_queue_size() > 0);
    }

    // Set all _tls_durable_csn to be zero first,
    // later running threads will set its corresponding _tls_durable_csn
    // to be current global_durable_csn to mark logs as active for cdc use
    ermia::dlog::tlogs[0]->reset_committer(true);
  }

  // if (ermia::config::enable_chkpt) {
  //  ermia::chkptmgr->do_chkpt();  // this is synchronous
  // }

  /*
    // Start checkpointer after database is ready
    if (ermia::config::enable_chkpt) {
      ermia::chkptmgr->start_chkpt_thread();
    }
    */
  ermia::volatile_write(ermia::config::state,
                        ermia::config::kStateForwardProcessing);

  if (ermia::config::worker_threads) {
    ermia::config::kStateRunning = true;
    start_measurement();
  }
}

void bench_runner::start_measurement() {
  workers = make_workers();
  ALWAYS_ASSERT(!workers.empty());
#ifdef DDL
  util::fast_random r(2343352);
  ddl_worker_id = r.next() % workers.size();
  DLOG(INFO) << "ddl_worker_id: " << ddl_worker_id;
#endif
  for (std::vector<bench_worker *>::const_iterator it = workers.begin();
       it != workers.end(); ++it) {
    while (!(*it)->IsImpersonated()) {
      (*it)->TryImpersonate();
    }
    (*it)->Start();
  }

  pid_t perf_pid;
  if (ermia::config::enable_perf) {
    std::cerr << "start perf..." << std::endl;

    std::stringstream parent_pid;
    parent_pid << getpid();

    pid_t pid = fork();
    // Launch profiler
    if (pid == 0) {
      if (ermia::config::perf_record_event != "") {
        exit(execl("/usr/bin/perf", "perf", "record", "-F", "99", "-e",
                   ermia::config::perf_record_event.c_str(), "-p",
                   parent_pid.str().c_str(), nullptr));
      } else {
        exit(execl(
            "/usr/bin/perf", "perf", "stat", "-B", "-e",
            "cache-references,cache-misses,cycles,instructions,branches,faults",
            "-p", parent_pid.str().c_str(), nullptr));
      }
    } else {
      perf_pid = pid;
    }
  }

  barrier_a.wait_for();  // wait for all threads to start up
  std::map<std::string, size_t> table_sizes_before;
  if (ermia::config::verbose) {
    for (std::map<std::string, ermia::OrderedIndex *>::iterator it =
             open_tables.begin();
         it != open_tables.end(); ++it) {
      const size_t s = it->second->Size();
      std::cerr << "table " << it->first << " size " << s << std::endl;
      table_sizes_before[it->first] = s;
    }
    std::cerr << "starting benchmark..." << std::endl;
  }

  // Print some results every second
  uint64_t slept = 0;
  uint64_t last_commits = 0, last_aborts = 0;

  // Print CPU utilization as well. Code adapted from:
  // https://stackoverflow.com/questions/63166/how-to-determine-cpu-and-memory-consumption-from-inside-a-process
  FILE *file;
  struct tms timeSample;
  char line[128];

  clock_t lastCPU = times(&timeSample);
  clock_t lastSysCPU = timeSample.tms_stime;
  clock_t lastUserCPU = timeSample.tms_utime;
  uint32_t nprocs = std::thread::hardware_concurrency();

  file = fopen("/proc/cpuinfo", "r");
  fclose(file);

  auto get_cpu_util = [&]() {
    ASSERT(ermia::config::print_cpu_util);
    struct tms timeSample;
    clock_t now;
    double percent;

    now = times(&timeSample);
    if (now <= lastCPU || timeSample.tms_stime < lastSysCPU ||
        timeSample.tms_utime < lastUserCPU) {
      percent = -1.0;
    } else {
      percent = (timeSample.tms_stime - lastSysCPU) +
                (timeSample.tms_utime - lastUserCPU);
      percent /= (now - lastCPU);
      percent /= nprocs;
      percent *= 100;
    }
    lastCPU = now;
    lastSysCPU = timeSample.tms_stime;
    lastUserCPU = timeSample.tms_utime;
    return percent;
  };

  if (ermia::config::print_cpu_util) {
    printf("Sec,Commits,Aborts,CPU\n");
  } else {
    printf("Sec,Commits,Aborts\n");
  }

  util::timer t, t_nosync;
  barrier_b.count_down();  // bombs away!

  double total_util = 0;
  double sec_util = 0;
  uint32_t sleep_time = ermia::config::print_interval_ms * 1000;
  auto gather_stats = [&]() {
    if (ddl_done < ermia::config::ddl_total &&
        slept == ddl_start_times[ddl_done] * 1000000) {
      ddl_start = true;
      ermia::ddl::ddl_start = true;
    }

    usleep(sleep_time);

    uint64_t sec_commits = 0, sec_aborts = 0;
    for (size_t i = 0; i < ermia::config::worker_threads; i++) {
      sec_commits += workers[i]->get_ntxn_commits();
      sec_aborts += workers[i]->get_ntxn_aborts();
    }
    sec_commits -= last_commits;
    sec_aborts -= last_aborts;
    last_commits += sec_commits;
    last_aborts += sec_aborts;

    if (ermia::config::print_cpu_util) {
      sec_util = get_cpu_util();
      total_util += sec_util;
      printf("%.1f,%lu,%lu,%.2f%%\n", double(slept + sleep_time) / 1000000,
             sec_commits, sec_aborts, sec_util);
    } else {
      printf("%.1f,%lu,%lu\n", double(slept + sleep_time) / 1000000,
             sec_commits, sec_aborts);
    }
    slept += sleep_time;
  };

  while (slept < ermia::config::benchmark_seconds * 1000000) {
    gather_stats();
  }
  running = false;

  ermia::volatile_write(ermia::config::state, ermia::config::kStateShutdown);
  for (size_t i = 0; i < ermia::config::worker_threads; i++) {
    workers[i]->Join();
  }

  const unsigned long elapsed_nosync = t_nosync.lap();

  if (ermia::config::enable_perf) {
    std::cerr << "stop perf..." << std::endl;
    kill(perf_pid, SIGINT);
    waitpid(perf_pid, nullptr, 0);
  }

  size_t n_commits = 0;
  size_t n_aborts = 0;
  size_t n_user_aborts = 0;
  size_t n_int_aborts = 0;
  size_t n_si_aborts = 0;
  size_t n_serial_aborts = 0;
  size_t n_rw_aborts = 0;
  size_t n_phantom_aborts = 0;
  size_t n_query_commits = 0;
  uint64_t latency_numer_us = 0;
  for (size_t i = 0; i < ermia::config::worker_threads; i++) {
#ifdef COPYDDL
    if (i == ddl_worker_id) {
      continue;
    }
#endif
    n_commits += workers[i]->get_ntxn_commits();
    n_aborts += workers[i]->get_ntxn_aborts();
    n_int_aborts += workers[i]->get_ntxn_int_aborts();
    n_user_aborts += workers[i]->get_ntxn_user_aborts();
    n_si_aborts += workers[i]->get_ntxn_si_aborts();
    n_serial_aborts += workers[i]->get_ntxn_serial_aborts();
    n_rw_aborts += workers[i]->get_ntxn_rw_aborts();
    n_phantom_aborts += workers[i]->get_ntxn_phantom_aborts();
    n_query_commits += workers[i]->get_ntxn_query_commits();
    if (ermia::config::pcommit) {
      latency_numer_us += workers[i]->get_log()->get_latency();
    } else {
      latency_numer_us += workers[i]->get_latency_numer_us();
    }
  }

#ifdef COPYDDL
  double workers_size = workers.size() - 1;
#else
  double workers_size = workers.size();
#endif

  const unsigned long elapsed = t.lap();
  const double elapsed_nosync_sec = double(elapsed_nosync) / 1000000.0;
  const double agg_nosync_throughput = double(n_commits) / elapsed_nosync_sec;
  const double avg_nosync_per_core_throughput =
      agg_nosync_throughput / workers_size;

  const double elapsed_sec = double(elapsed) / 1000000.0;
  const double agg_throughput = double(n_commits) / elapsed_sec;
  const double avg_per_core_throughput = agg_throughput / workers_size;

  const double agg_abort_rate = double(n_aborts) / elapsed_sec;
  const double avg_per_core_abort_rate = agg_abort_rate / workers_size;

  const double agg_system_abort_rate =
      double(n_aborts - n_user_aborts) / elapsed_sec;
  const double agg_user_abort_rate = double(n_user_aborts) / elapsed_sec;
  const double agg_int_abort_rate = double(n_int_aborts) / elapsed_sec;
  const double agg_si_abort_rate = double(n_si_aborts) / elapsed_sec;
  const double agg_serial_abort_rate = double(n_serial_aborts) / elapsed_sec;
  const double agg_phantom_abort_rate = double(n_phantom_aborts) / elapsed_sec;
  const double agg_rw_abort_rate = double(n_rw_aborts) / elapsed_sec;

  const double avg_latency_us = double(latency_numer_us) / double(n_commits);
  const double avg_latency_ms = avg_latency_us / 1000.0;

  uint64_t agg_latency_us = 0;
  uint64_t agg_redo_batches = 0;
  uint64_t agg_redo_size = 0;

  const double agg_replay_latency_ms = agg_latency_us / 1000.0;

  tx_stat_map agg_txn_counts = workers[0]->get_txn_counts();
  for (size_t i = 1; i < workers.size(); i++) {
#ifdef COPYDDL
    if (i == ddl_worker_id) {
      continue;
    }
#endif
    auto &c = workers[i]->get_txn_counts();
    for (auto &t : c) {
      std::get<0>(agg_txn_counts[t.first]) += std::get<0>(t.second);
      std::get<1>(agg_txn_counts[t.first]) += std::get<1>(t.second);
      std::get<2>(agg_txn_counts[t.first]) += std::get<2>(t.second);
      std::get<3>(agg_txn_counts[t.first]) += std::get<3>(t.second);
    }
  }

  if (ermia::config::verbose) {
    std::cerr << "--- table statistics ---" << std::endl;
    for (std::map<std::string, ermia::OrderedIndex *>::iterator it =
             open_tables.begin();
         it != open_tables.end(); ++it) {
      const size_t s = it->second->Size();
      const ssize_t delta = ssize_t(s) - ssize_t(table_sizes_before[it->first]);
      std::cerr << "table " << it->first << " size " << it->second->Size();
      if (delta < 0)
        std::cerr << " (" << delta << " records)" << std::endl;
      else
        std::cerr << " (+" << delta << " records)" << std::endl;
    }
    std::cerr << "--- benchmark statistics ---" << std::endl;
    std::cerr << "runtime: " << elapsed_sec << " sec" << std::endl;
    std::cerr << "cpu_util: " << total_util / elapsed_sec << "%" << std::endl;
    std::cerr << "agg_nosync_throughput: " << agg_nosync_throughput
              << " ops/sec" << std::endl;
    std::cerr << "avg_nosync_per_core_throughput: "
              << avg_nosync_per_core_throughput << " ops/sec/core" << std::endl;
    std::cerr << "agg_throughput: " << agg_throughput << " ops/sec"
              << std::endl;
    std::cerr << "avg_per_core_throughput: " << avg_per_core_throughput
              << " ops/sec/core" << std::endl;
    std::cerr << "avg_latency: " << avg_latency_ms << " ms" << std::endl;
    std::cerr << "agg_abort_rate: " << agg_abort_rate << " aborts/sec"
              << std::endl;
    std::cerr << "avg_per_core_abort_rate: " << avg_per_core_abort_rate
              << " aborts/sec/core" << std::endl;
#ifndef __clang__
    std::cerr << "txn breakdown: "
              << util::format_list(agg_txn_counts.begin(), agg_txn_counts.end())
              << std::endl;
#endif
  }

  // output for plotting script
  std::cout << "---------------------------------------\n";
  std::cout << agg_throughput
            << " commits/s, "
            //       << avg_latency_ms << " "
            << agg_abort_rate << " total_aborts/s, " << agg_system_abort_rate
            << " system_aborts/s, " << agg_user_abort_rate << " user_aborts/s, "
            << agg_int_abort_rate << " internal aborts/s, " << agg_si_abort_rate
            << " si_aborts/s, " << agg_serial_abort_rate << " serial_aborts/s, "
            << agg_rw_abort_rate << " rw_aborts/s, " << agg_phantom_abort_rate
            << " phantom aborts/s." << std::endl;
  std::cout << n_commits << " commits, " << n_query_commits
            << " query_commits, " << n_aborts << " total_aborts, "
            << n_aborts - n_user_aborts << " system_aborts, " << n_user_aborts
            << " user_aborts, " << n_int_aborts << " internal_aborts, "
            << n_si_aborts << " si_aborts, " << n_serial_aborts
            << " serial_aborts, " << n_rw_aborts << " rw_aborts, "
            << n_phantom_aborts << " phantom_aborts" << std::endl;

  std::cout << "---------------------------------------\n";
  for (auto &c : agg_txn_counts) {
    std::cout << c.first << "\t" << std::get<0>(c.second) / (double)elapsed_sec
              << " commits/s\t" << std::get<1>(c.second) / (double)elapsed_sec
              << " aborts/s\t" << std::get<2>(c.second) / (double)elapsed_sec
              << " system aborts/s\t"
              << std::get<3>(c.second) / (double)elapsed_sec
              << " user aborts/s\n";
  }
  std::cout.flush();
}

template <typename K, typename V>
struct map_maxer {
  typedef std::map<K, V> map_type;
  void operator()(map_type &agg, const map_type &m) const {
    for (typename map_type::const_iterator it = m.begin(); it != m.end(); ++it)
      agg[it->first] = std::max(agg[it->first], it->second);
  }
};

const tx_stat_map bench_worker::get_txn_counts() const {
  tx_stat_map m;
  const workload_desc_vec workload = get_workload();
  for (size_t i = 0; i < txn_counts.size(); i++)
    m[workload[i].name] = txn_counts[i];
  return m;
}

void bench_worker::PipelineScheduler() {
#ifdef BATCH_SAME_TRX
  LOG(FATAL)
      << "Pipeline scheduler doesn't work with batching same-type transactoins";
#endif
  LOG(INFO) << "Epoch management and latency recorder in Pipeline scheduler "
               "are not logically correct";

  CoroTxnHandle *handles = (CoroTxnHandle *)numa_alloc_onnode(
      sizeof(CoroTxnHandle) * ermia::config::coro_batch_size,
      numa_node_of_cpu(sched_getcpu()));
  memset(handles, 0, sizeof(CoroTxnHandle) * ermia::config::coro_batch_size);

  uint32_t *workload_idxs = (uint32_t *)numa_alloc_onnode(
      sizeof(uint32_t) * ermia::config::coro_batch_size,
      numa_node_of_cpu(sched_getcpu()));

  rc_t *rcs =
      (rc_t *)numa_alloc_onnode(sizeof(rc_t) * ermia::config::coro_batch_size,
                                numa_node_of_cpu(sched_getcpu()));

  barrier_a->count_down();
  barrier_b->wait_for();
  util::timer t;

  for (uint32_t i = 0; i < ermia::config::coro_batch_size; i++) {
    uint32_t workload_idx = fetch_workload();
    workload_idxs[i] = workload_idx;
    handles[i] = workload[workload_idx].coro_fn(this, i, 0).get_handle();
  }

  uint32_t i = 0;
  coroutine_batch_end_epoch = 0;
  ermia::epoch_num begin_epoch = ermia::MM::epoch_enter();
  while (running) {
    if (handles[i].done()) {
      rcs[i] = handles[i].promise().get_return_value();
#ifdef CORO_BATCH_COMMIT
      if (!rcs[i].IsAbort()) {
        rcs[i] = db->Commit(&transactions[i]);
      }
#endif
      finish_workload(rcs[i], workload_idxs[i], t);
      handles[i].destroy();

      uint32_t workload_idx = fetch_workload();
      workload_idxs[i] = workload_idx;
      handles[i] = workload[workload_idx].coro_fn(this, i, 0).get_handle();
    } else if (!handles[i].promise().callee_coro ||
               handles[i].promise().callee_coro.done()) {
      handles[i].resume();
    } else {
      handles[i].promise().callee_coro.resume();
    }

    i = (i + 1) & (ermia::config::coro_batch_size - 1);
  }

  ermia::MM::epoch_exit(coroutine_batch_end_epoch, begin_epoch);
}

void bench_worker::Scheduler() {
#ifdef BATCH_SAME_TRX
  LOG(FATAL)
      << "General scheduler doesn't work with batching same-type transactoins";
#endif
#ifdef CORO_BATCH_COMMIT
  LOG(FATAL) << "General scheduler doesn't work with batching commits";
#endif
  CoroTxnHandle *handles = (CoroTxnHandle *)numa_alloc_onnode(
      sizeof(CoroTxnHandle) * ermia::config::coro_batch_size,
      numa_node_of_cpu(sched_getcpu()));
  memset(handles, 0, sizeof(CoroTxnHandle) * ermia::config::coro_batch_size);

  uint32_t *workload_idxs = (uint32_t *)numa_alloc_onnode(
      sizeof(uint32_t) * ermia::config::coro_batch_size,
      numa_node_of_cpu(sched_getcpu()));

  rc_t *rcs =
      (rc_t *)numa_alloc_onnode(sizeof(rc_t) * ermia::config::coro_batch_size,
                                numa_node_of_cpu(sched_getcpu()));

  barrier_a->count_down();
  barrier_b->wait_for();

  while (running) {
    coroutine_batch_end_epoch = 0;
    ermia::epoch_num begin_epoch = ermia::MM::epoch_enter();
    uint32_t todo = ermia::config::coro_batch_size;
    util::timer t;

    for (uint32_t i = 0; i < ermia::config::coro_batch_size; i++) {
      uint32_t workload_idx = fetch_workload();
      workload_idxs[i] = workload_idx;
      handles[i] = workload[workload_idx].coro_fn(this, i, 0).get_handle();
    }

    while (todo) {
      for (uint32_t i = 0; i < ermia::config::coro_batch_size; i++) {
        if (!handles[i]) {
          continue;
        }
        if (handles[i].done()) {
          rcs[i] = handles[i].promise().get_return_value();
          finish_workload(rcs[i], workload_idxs[i], t);
          handles[i].destroy();
          handles[i] = nullptr;
          --todo;
        } else if (!handles[i].promise().callee_coro ||
                   handles[i].promise().callee_coro.done()) {
          handles[i].resume();
        } else {
          handles[i].promise().callee_coro.resume();
        }
      }
    }

    ermia::MM::epoch_exit(coroutine_batch_end_epoch, begin_epoch);
  }
}

void bench_worker::BatchScheduler() {
  CoroTxnHandle *handles = (CoroTxnHandle *)numa_alloc_onnode(
      sizeof(CoroTxnHandle) * ermia::config::coro_batch_size,
      numa_node_of_cpu(sched_getcpu()));
  memset(handles, 0, sizeof(CoroTxnHandle) * ermia::config::coro_batch_size);

  rc_t *rcs =
      (rc_t *)numa_alloc_onnode(sizeof(rc_t) * ermia::config::coro_batch_size,
                                numa_node_of_cpu(sched_getcpu()));

#ifndef BATCH_SAME_TRX
  LOG(FATAL) << "Batch scheduler batches same-type transactoins";
#endif

  barrier_a->count_down();
  barrier_b->wait_for();

  while (running) {
    coroutine_batch_end_epoch = 0;
    ermia::epoch_num begin_epoch = ermia::MM::epoch_enter();
    uint32_t todo = ermia::config::coro_batch_size;
    uint32_t workload_idx = -1;
    workload_idx = fetch_workload();
    util::timer t;

    for (uint32_t i = 0; i < ermia::config::coro_batch_size; i++) {
      handles[i] = workload[workload_idx].coro_fn(this, i, 0).get_handle();
    }

    while (todo) {
      for (uint32_t i = 0; i < ermia::config::coro_batch_size; i++) {
        if (!handles[i]) {
          continue;
        }
        if (handles[i].done()) {
          rcs[i] = handles[i].promise().get_return_value();
#ifndef CORO_BATCH_COMMIT
          finish_workload(rcs[i], workload_idx, t);
#endif
          handles[i].destroy();
          handles[i] = nullptr;
          --todo;
        } else if (!handles[i].promise().callee_coro ||
                   handles[i].promise().callee_coro.done()) {
          handles[i].resume();
        } else {
          handles[i].promise().callee_coro.resume();
        }
      }
    }

#ifdef CORO_BATCH_COMMIT
    for (uint32_t i = 0; i < ermia::config::coro_batch_size; i++) {
      if (!rcs[i].IsAbort()) {
        rcs[i] = db->Commit(&transactions[i]);
      }
      // No need to abort - TryCatchCond family of macros should have already
      finish_workload(rcs[i], workload_idx, t);
    }
#endif

    ermia::MM::epoch_exit(coroutine_batch_end_epoch, begin_epoch);
  }
}
