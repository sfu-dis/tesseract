#pragma once

#include <numa.h>
#include <sys/stat.h>

#include <condition_variable>
#include <fstream>
#include <functional>
#include <mutex>
#include <thread>

#include "../util.h"
#include "sm-defs.h"
#include "xid.h"

namespace ermia {
namespace thread {

struct CPUCore {
  uint32_t node;
  uint32_t physical_thread;
  std::vector<uint32_t> logical_threads;
  CPUCore(uint32_t n, uint32_t phys) : node(n), physical_thread(phys) {}
  void AddLogical(uint32_t t) { logical_threads.push_back(t); }
};

extern std::vector<CPUCore> cpu_cores;

bool DetectCPUCores();
void Initialize();

// == total number of threads had so far - never decreases
extern std::atomic<uint32_t> next_thread_id;

inline uint32_t MyId() {
  thread_local uint32_t thread_id CACHE_ALIGNED;
  thread_local bool thread_initialized CACHE_ALIGNED;

  if (!thread_initialized) {
    thread_id = next_thread_id.fetch_add(1);
    thread_initialized = true;
  }
  return thread_id;
}

struct Thread {
  const uint8_t kStateHasWork = 1U;
  const uint8_t kStateSleep = 2U;
  const uint8_t kStateNoWork = 3U;

  typedef std::function<void(char *task_input)> Task;
  pthread_t thd;
  pthread_attr_t thd_attr;
  uint16_t node;
  uint16_t core;
  uint32_t sys_cpu;  // OS-given CPU number
  bool shutdown;
  uint8_t state;
  Task task;
  char *task_input;
  bool sleep_when_idle;
  bool is_physical;

  std::condition_variable trigger;
  std::mutex trigger_lock;

  Thread();
  Thread(uint16_t n, uint16_t c, uint32_t sys_cpu, bool is_physical);
  ~Thread() {}

  void IdleTask();
  static void *StaticIdleTask(void *context) {
    ((Thread *)context)->IdleTask();
    return nullptr;
  }

  // No CC whatsoever, caller must know what it's doing
  inline void StartTask(Task t, char *input = nullptr) {
    task = t;
    task_input = input;
    auto s = __sync_val_compare_and_swap(&state, kStateNoWork, kStateHasWork);
    if (s == kStateSleep) {
      while (volatile_read(state) != kStateNoWork) {
        trigger.notify_all();
      }
      volatile_write(state, kStateHasWork);
    } else {
      ALWAYS_ASSERT(s == kStateNoWork);
    }
  }

  inline void Join() {
    while (volatile_read(state) == kStateHasWork) {
    }
  }
  inline bool TryJoin() { return volatile_read(state) != kStateHasWork; }
  inline void Destroy() {
    volatile_write(shutdown, true);
    trigger.notify_all();
  }
};

struct PerNodeThreadPool {
  static uint32_t max_threads_per_node;
  uint16_t node CACHE_ALIGNED;
  Thread *threads CACHE_ALIGNED;
  uint64_t bitmap CACHE_ALIGNED;  // max 64 threads per node, 1 - busy, 0 - free

  PerNodeThreadPool(uint16_t n);

  // Get a single new thread which can be physical or logical
  Thread *GetThread(bool physical);

  // Release a thread back to the pool
  inline void PutThread(Thread *t) {
    auto b = ~uint64_t{1UL << (t - threads)};
    __sync_fetch_and_and(&bitmap, b);
  }
};

extern PerNodeThreadPool *thread_pools;
extern uint32_t num_thread_pools;

inline Thread *GetThread(uint16_t from, bool physical) {
  if (config::threadpool) {
    return thread_pools[from].GetThread(physical);
  } else {
    return new Thread;
  }
}

inline Thread *GetThread(bool physical /* don't care where */) {
  if (config::threadpool) {
    for (uint16_t i = 0; i < num_thread_pools; i++) {
      auto *t = thread_pools[i].GetThread(physical);
      if (t) {
        return t;
      }
    }
  } else {
    return new Thread;
  }
  return nullptr;
}

inline void PutThread(Thread *t) {
  if (config::threadpool) {
    thread_pools[t->node].PutThread(t);
  } else {
    t->Destroy();
  }
}

// A wrapper that includes Thread for user code to use.
// Benchmark and log replay threads deal with this only,
// not with Thread.
struct Runner {
  Runner(bool physical = true) : me(nullptr), physical(physical) {}
  virtual ~Runner() {
    if (me) {
      Join();
    }
  }

  virtual void MyWork(char *) = 0;

  inline void Start() {
    ALWAYS_ASSERT(me);
    thread::Thread::Task t =
        std::bind(&Runner::MyWork, this, std::placeholders::_1);
    me->StartTask(t);
  }

  inline bool TryImpersonate(bool sleep_when_idle = true) {
    ALWAYS_ASSERT(not me);
    me = thread::GetThread(physical);
    if (me) {
      LOG_IF(FATAL, config::threadpool && me->is_physical != physical)
          << "Not the requested thread type";
      me->sleep_when_idle = sleep_when_idle;
      if (config::threadpool) {
        LOG(INFO) << "Impersonated on thread " << me->node << ", " << me->core;
      }
    }
    return me != nullptr;
  }

  inline bool TryImpersonate(uint32_t node, bool sleep_when_idle = true) {
    ALWAYS_ASSERT(not me);
    me = thread::GetThread(node, physical);
    if (me) {
      LOG_IF(FATAL, me->is_physical != physical)
          << "Not the requested thread type";
      me->sleep_when_idle = sleep_when_idle;
    }
    return me != nullptr;
  }

  inline void Join() {
    me->Join();
    PutThread(me);
    me = nullptr;
  }
  // Same as Join(), but don't return the thread
  inline void Wait() { me->Join(); }
  inline bool TryWait() { return me->TryJoin(); }
  inline bool IsImpersonated() { return me != nullptr; }
  inline bool TryJoin() {
    if (me->TryJoin()) {
      PutThread(me);
      me = nullptr;
      return true;
    }
    return false;
  }

  Thread *me;
  bool physical;
};
}  // namespace thread
}  // namespace ermia
