#pragma once

#include <set>
#include <vector>
#include <utility>
#include <string>

#include "../engine.h"
#include "../util.h"

#include "../dbcore/sm-coroutine.h"
#include "../dbcore/sm-thread.h"

extern void ycsb_do_test(ermia::Engine *db, int argc, char **argv);
extern void ycsb_cs_do_test(ermia::Engine *db, int argc, char **argv);
extern void ycsb_cs_advance_do_test(ermia::Engine *db, int argc, char **argv);
extern void tpcc_do_test(ermia::Engine *db, int argc, char **argv);
extern void tpcc_cs_do_test(ermia::Engine *db, int argc, char **argv);
extern void tpce_do_test(ermia::Engine *db, int argc, char **argv);

enum { RUNMODE_TIME = 0, RUNMODE_OPS = 1 };

// benchmark global variables
extern volatile bool running;

template <typename T>
static std::vector<T> unique_filter(const std::vector<T> &v) {
  std::set<T> seen;
  std::vector<T> ret;
  for (auto &e : v)
    if (!seen.count(e)) {
      ret.emplace_back(e);
      seen.insert(e);
    }
  return ret;
}

class bench_loader : public ermia::thread::Runner {
 public:
  bench_loader(unsigned long seed, ermia::Engine *db,
               const std::map<std::string, ermia::OrderedIndex *> &open_tables)
      : Runner(true)
      , r(seed), db(db), open_tables(open_tables) {
    // don't try_instantiate() here; do it when we start to load.
    // The way we reuse threads relies on this fact (see bench_runner::run()).
    txn_obj_buf = (ermia::transaction *)malloc(sizeof(ermia::transaction));
    arena = new ermia::str_arena(ermia::config::arena_size_mb);
  }

  virtual ~bench_loader() {}
  ALWAYS_INLINE ermia::varstr &str(uint64_t size) { return *arena->next(size); }

 private:
  virtual void MyWork(char *) { load(); }

 protected:
  inline ermia::transaction *txn_buf() { return txn_obj_buf; }
  virtual void load() = 0;

  util::fast_random r;
  ermia::Engine *const db;
  std::map<std::string, ermia::OrderedIndex *> open_tables;
  ermia::transaction *txn_obj_buf;
  ermia::str_arena *arena;
};

typedef std::tuple<uint64_t, uint64_t, uint64_t, uint64_t> tx_stat;
typedef std::map<std::string, tx_stat> tx_stat_map;

class bench_worker : public ermia::thread::Runner {
 public:
  bench_worker(unsigned int worker_id, bool is_worker, unsigned long seed,
               ermia::Engine *db, const std::map<std::string, ermia::OrderedIndex *> &open_tables,
               spin_barrier *barrier_a = nullptr, spin_barrier *barrier_b = nullptr)
      : Runner(ermia::config::physical_workers_only ? true : (worker_id >= ermia::config::worker_threads / 2)),
        worker_id(worker_id),
        is_worker(is_worker),
        r(seed),
        db(db),
        open_tables(open_tables),
        barrier_a(barrier_a),
        barrier_b(barrier_b),
        latency_numer_us(0),
        backoff_shifts(
            0),  // spin between [0, 2^backoff_shifts) times before retry
        // the ntxn_* numbers are per worker
        ntxn_commits(0),
        ntxn_aborts(0),
        ntxn_user_aborts(0),
        ntxn_int_aborts(0),
        ntxn_si_aborts(0),
        ntxn_serial_aborts(0),
        ntxn_rw_aborts(0),
        ntxn_phantom_aborts(0),
        ntxn_query_commits(0) {
    txn_obj_buf = (ermia::transaction *)malloc(sizeof(ermia::transaction));
    arena = new ermia::str_arena(ermia::config::arena_size_mb);
    if (ermia::config::numa_spread) {
      LOG(INFO) << "Worker " << worker_id << " going to node " << worker_id % ermia::config::numa_nodes;
      TryImpersonate(worker_id % ermia::config::numa_nodes);
    } else {
      TryImpersonate();
    }

    if (ermia::config::coro_tx) {
      transactions = (ermia::transaction*)numa_alloc_onnode(
        sizeof(ermia::transaction) * ermia::config::coro_batch_size,
        numa_node_of_cpu(sched_getcpu()));
      arenas = (ermia::str_arena*)numa_alloc_onnode(
        sizeof(ermia::str_arena) * ermia::config::coro_batch_size,
        numa_node_of_cpu(sched_getcpu()));
      for (auto i = 0; i < ermia::config::coro_batch_size; ++i) {
        new (arenas + i) ermia::str_arena(ermia::config::arena_size_mb);
      }
    }
  }
  ~bench_worker() {}

  /* For 'normal' workload (r/w on primary, r/o on backups) */
  typedef rc_t (*txn_fn_t)(bench_worker *);
  typedef std::experimental::coroutine_handle<ermia::coro::generator<rc_t>::promise_type> CoroTxnHandle;
  typedef ermia::coro::generator<rc_t> (*coro_txn_fn_t)(bench_worker *, uint32_t, ermia::epoch_num);
  typedef ermia::coro::task<rc_t> (*task_fn_t)(bench_worker *, uint32_t, ermia::epoch_num);
  struct workload_desc {
    workload_desc() {}
    workload_desc(const std::string &name, double frequency, txn_fn_t fn,
                  coro_txn_fn_t cf=nullptr, task_fn_t tf=nullptr)
        : name(name), frequency(frequency), fn(fn), coro_fn(cf) , task_fn(tf) {
      ALWAYS_ASSERT(frequency > 0.0);
      ALWAYS_ASSERT(frequency <= 1.0);
    }
    std::string name;
    double frequency;
    txn_fn_t fn;
    coro_txn_fn_t coro_fn;
    task_fn_t task_fn;
  };
  typedef std::vector<workload_desc> workload_desc_vec;
  virtual workload_desc_vec get_workload() const = 0;
  workload_desc_vec workload;

  inline size_t get_ntxn_commits() const { return ntxn_commits; }
  inline size_t get_ntxn_aborts() const { return ntxn_aborts; }
  inline size_t get_ntxn_user_aborts() const { return ntxn_user_aborts; }
  inline size_t get_ntxn_si_aborts() const { return ntxn_si_aborts; }
  inline size_t get_ntxn_serial_aborts() const { return ntxn_serial_aborts; }
  inline size_t get_ntxn_rw_aborts() const { return ntxn_rw_aborts; }
  inline size_t get_ntxn_int_aborts() const { return ntxn_int_aborts; }
  inline size_t get_ntxn_phantom_aborts() const { return ntxn_phantom_aborts; }
  inline size_t get_ntxn_query_commits() const { return ntxn_query_commits; }
  inline void inc_ntxn_user_aborts() { ++ntxn_user_aborts; }
  inline void inc_ntxn_si_aborts() { ++ntxn_si_aborts; }
  inline void inc_ntxn_serial_aborts() { ++ntxn_serial_aborts; }
  inline void inc_ntxn_rw_aborts() { ++ntxn_rw_aborts; }
  inline void inc_ntxn_int_aborts() { ++ntxn_int_aborts; }
  inline void inc_ntxn_phantom_aborts() { ++ntxn_phantom_aborts; }
  inline void inc_ntxn_query_commits() { ++ntxn_query_commits; }

  inline uint64_t get_latency_numer_us() const {
    return ermia::volatile_read(latency_numer_us);
  }

  inline double get_avg_latency_us() const {
    return double(latency_numer_us) / double(ntxn_commits);
  }

  const tx_stat_map get_txn_counts() const;

  void do_workload_function(uint32_t i);
  uint32_t fetch_workload();
  bool finish_workload(rc_t ret, uint32_t workload_idx, util::timer t);

 protected:
  virtual void MyWork(char *);
  inline ermia::transaction *txn_buf() { return txn_obj_buf; }

  unsigned int worker_id;
  bool is_worker;
  util::fast_random r;
  ermia::Engine *const db;
  std::map<std::string, ermia::OrderedIndex *> open_tables;
  spin_barrier *const barrier_a;
  spin_barrier *const barrier_b;

  // coroutine schedulers
  void Scheduler();
  void PipelineScheduler();
  void BatchScheduler();

 private:
  uint64_t latency_numer_us;
  unsigned backoff_shifts;

  // stats
  size_t ntxn_commits;
  size_t ntxn_aborts;
  size_t ntxn_user_aborts;
  size_t ntxn_int_aborts;
  size_t ntxn_si_aborts;
  size_t ntxn_serial_aborts;
  size_t ntxn_rw_aborts;
  size_t ntxn_phantom_aborts;
  size_t ntxn_query_commits;

 protected:
  std::vector<tx_stat> txn_counts;  // commits and aborts breakdown

  ermia::transaction *txn_obj_buf;
  ermia::str_arena *arena;

  // NOTE: inter-transaction interleaving
  ermia::transaction *transactions;
  ermia::str_arena *arenas;
};

class bench_runner {
 public:
  bench_runner(const bench_runner &) = delete;
  bench_runner(bench_runner &&) = delete;
  bench_runner &operator=(const bench_runner &) = delete;

  bench_runner(ermia::Engine *db)
      : db(db),
        barrier_a(ermia::config::worker_threads),
        barrier_b(ermia::config::worker_threads > 0 ? 1 : 0) {}
  virtual ~bench_runner() {}
  virtual void prepare(char *) = 0;
  void run();
  void start_measurement();

  static std::vector<bench_worker *> workers;

 protected:
  // only called once
  virtual std::vector<bench_loader *> make_loaders() = 0;

  // only called once
  virtual std::vector<bench_worker *> make_workers() = 0;

  ermia::Engine *const db;
  std::map<std::string, ermia::OrderedIndex *> open_tables;

  // barriers for actual benchmark execution
  spin_barrier barrier_a;
  spin_barrier barrier_b;
};

// XXX(stephentu): limit_callback is not optimal, should use
// static_limit_callback if possible
class limit_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  limit_callback(ssize_t limit = -1) : limit(limit), n(0) {
    ALWAYS_ASSERT(limit == -1 || limit > 0);
  }

  virtual bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) {
    ASSERT(limit == -1 || n < size_t(limit));
    values.emplace_back(ermia::varstr(keyp, keylen), value);
    return (limit == -1) || (++n < size_t(limit));
  }

  typedef std::pair<ermia::varstr, ermia::varstr> kv_pair;
  std::vector<kv_pair> values;

  const ssize_t limit;

 private:
  size_t n;
};

// Note: try_catch_cond_abort might call __abort_txn with rc=RC_FALSE
// so no need to assure rc must be RC_ABORT_*.
#define __abort_txn(r)                            \
{                                                 \
  db->Abort(txn);                                 \
  if (!r.IsAbort()) return {RC_ABORT_USER};       \
  return r;                                       \
}

#define __abort_txn_coro(r)                       \
{                                                 \
  db->Abort(txn);                                 \
  if (!r.IsAbort()) co_return {RC_ABORT_USER};    \
  co_return r;                                    \
}

// NOTE: only use these in transaction benchmark (e.g., TPCC) code, not in
// engine code

// reminescent the try...catch block:
// if return code is one of those RC_ABORT* then abort
#define TryCatch(rc)               \
{                                  \
  rc_t r = rc;                     \
  if (r.IsAbort()) __abort_txn(r); \
}

#define TryCatchCoro(rc)                \
{                                       \
  rc_t r = rc;                          \
  if (r.IsAbort()) __abort_txn_coro(r); \
}

// same as TryCatch but don't do abort, only return rc
// So far the only user is TPC-E's TxnHarness***.h.
#define TryReturn(rc)        \
{                            \
  rc_t r = rc;               \
  if (r.IsAbort()) return r; \
}

// if rc == RC_FALSE then do op
#define TryCatchCond(rc, op)       \
{                                  \
  rc_t r = rc;                     \
  if (r.IsAbort()) __abort_txn(r); \
  if (r._val == RC_FALSE) op;      \
}

// if rc == RC_FALSE then do op
#define TryCatchCondCoro(rc, op)        \
{                                       \
  rc_t r = rc;                          \
  if (r.IsAbort()) __abort_txn_coro(r); \
  if (r._val == RC_FALSE) op;           \
}

#define TryCatchCondAbort(rc)                            \
{                                                        \
  rc_t r = rc;                                           \
  if (r.IsAbort() or r._val == RC_FALSE) __abort_txn(r); \
}

// combines the try...catch block with ALWAYS_ASSERT and allows abort.
// The rc_is_abort case is there because sometimes we want to make
// sure say, a get, succeeds, but the read itsef could also cause
// abort (by SSN). Use try_verify_strict if you need rc=true.
#define TryVerifyRelaxed(oper)                     \
{                                                  \
  rc_t r = oper;                                   \
  LOG_IF(FATAL, r._val != RC_TRUE && !r.IsAbort()) \
    << "Wrong return value " << r._val;            \
  if (r.IsAbort()) __abort_txn(r);                 \
}

#define TryVerifyRelaxedCoro(oper)                 \
{                                                  \
  rc_t r = oper;                                   \
  LOG_IF(FATAL, r._val != RC_TRUE && !r.IsAbort()) \
    << "Wrong return value " << r._val;            \
  if (r.IsAbort()) __abort_txn_coro(r);            \
}

// No abort is allowed, usually for loading
inline void TryVerifyStrict(rc_t rc) {
  LOG_IF(FATAL, rc._val != RC_TRUE) << "Wrong return value " << rc._val;
}
