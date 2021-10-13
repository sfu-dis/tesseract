#pragma once

#include <array>
#include <map>
#include <numa.h>
#include <limits.h>

#include "../macros.h"
#include "sm-defs.h"

#ifdef __clang__
#include <experimental/coroutine>
using std::experimental::coroutine_handle;
using std::experimental::noop_coroutine;
using std::experimental::suspend_always;
using std::experimental::suspend_never;
#else
#include <coroutine>
using std::coroutine_handle;
using std::noop_coroutine;
using std::suspend_always;
using std::suspend_never;
#endif  // __has_include(<coroutine>)

namespace ermia {
namespace coro {

// Simple thread caching allocator.
class tcalloc {
    struct alignas(CACHELINE_SIZE) FrameNode {
        FrameNode *next;
        uint8_t entry_index;
        uint8_t padding[CACHELINE_SIZE - sizeof(intptr_t) - sizeof(uint8_t)];
    };

    static_assert(sizeof(FrameNode) == CACHELINE_SIZE, "");

   public:
    tcalloc() { 
        memset(entries, 0, sizeof(entries));

        constexpr size_t kArenaSize = 8 * 1024 * 1024;
        arena = static_cast<uint8_t *>(
            numa_alloc_onnode(kArenaSize, numa_node_of_cpu(sched_getcpu())));
        arena_top = arena;
    }
    ~tcalloc() {}

    static inline uint32_t lg_down(uint64_t x) {
        static_assert(sizeof(unsigned long long) * CHAR_BIT == 64, "");
        return 63U - __builtin_clzll(x);
    }

    static inline uint32_t lg_down(uint32_t x) {
        static_assert(sizeof(unsigned) * CHAR_BIT == 32, "");
        return 31U - __builtin_clz(x);
    }

    template <typename T>
    static inline uint32_t lg_up(T x) {
        return lg_down(x - 1) + 1;
    }

    void *alloc_from_arena(size_t byte_size, uint8_t alignment) {
        ASSERT(arena_top);
        const int8_t mask = alignment - 1;
        uint8_t *p = reinterpret_cast<uint8_t *>(
            reinterpret_cast<intptr_t>(arena_top + mask) & ~mask);
        arena_top = p + byte_size;
        return reinterpret_cast<void *>(p);
    }

    void *alloc(size_t byte_size) {
        const int ceil_log_2 = lg_up(byte_size);
        ASSERT(ceil_log_2 < kEndSizeExp);

        const int entry_index =
            ceil_log_2 > kBeginSizeExp ? ceil_log_2 - kBeginSizeExp : 0;

        FrameNode *frame_to_alloc = entries[entry_index];
        if (frame_to_alloc == nullptr) {
            const size_t frame_size = 1 << (entry_index + kBeginSizeExp);
            frame_to_alloc = reinterpret_cast<FrameNode *>(alloc_from_arena(
                sizeof(FrameNode) + frame_size, CACHELINE_SIZE));
            frame_to_alloc->entry_index = entry_index;
        } else {
            entries[entry_index] = frame_to_alloc->next;
        }

        return static_cast<void *>(frame_to_alloc + 1);
    }

    void free(void *p, size_t byte_size) {
        FrameNode *frame_to_free = reinterpret_cast<FrameNode *>(p) - 1;
        const int entry_index = frame_to_free->entry_index;
        frame_to_free->next = entries[entry_index];
        entries[entry_index] = frame_to_free;
    }

   private:
    static constexpr short kBeginSizeExp = 8;
    static constexpr short kEndSizeExp = 16;  // exclusive
    FrameNode *entries[kEndSizeExp - kBeginSizeExp];

    uint8_t *arena;
    uint8_t *arena_top;
};

extern thread_local tcalloc coroutine_allocator;

template <typename T = void> struct [[nodiscard]] generator {
  struct promise_type;
  using handle = coroutine_handle<promise_type>;

  struct promise_type {
    promise_type() {}
    ~promise_type() {
      reinterpret_cast<T*>(&ret_val_buf_)->~T();
    }
    auto get_return_object() { return generator{handle::from_promise(*this)}; }
    auto initial_suspend() { return suspend_never{}; }
    auto final_suspend() noexcept { return suspend_always{}; }
    void unhandled_exception() { std::terminate(); }
    void return_value(const T value) {
        new (&ret_val_buf_) T(std::move(value));
    }
    T && transfer_return_value() {
        return std::move(*reinterpret_cast<T*>(&ret_val_buf_));
    }
    T get_return_value() {
        return *reinterpret_cast<T*>(&ret_val_buf_);
    }
    void *operator new(size_t sz) { return coroutine_allocator.alloc(sz); }
    void operator delete(void *p, size_t sz) { coroutine_allocator.free(p, sz); }
    struct alignas(alignof(T)) T_Buf {
      uint8_t buf[sizeof(T)];
    };

    coroutine_handle<> callee_coro = nullptr;
    T_Buf ret_val_buf_;
  };

  auto get_handle() {
    auto result = coro;
    coro = nullptr;
    return result;
  }

  generator(generator const &) = delete;
  generator(handle h = nullptr) : coro(h) {}
  generator(generator &&rhs) : coro(rhs.coro) { rhs.coro = nullptr; }
  ~generator() {
    if (coro) {
      coro.destroy();
    }
  }

  generator &operator=(generator const &) = delete;
  generator &operator=(generator &&rhs) {
    if (this != &rhs) {
      coro = rhs.coro;
      rhs.coro = nullptr;
    }
    return *this;
  }

  struct awaiter {
    awaiter(handle h) : awaiter_coro(h) {}
    constexpr bool await_ready() const noexcept { return false; }
    template <typename awaiting_handle>
    constexpr void await_suspend(awaiting_handle awaiting_coro) noexcept {
      awaiting_coro.promise().callee_coro = awaiter_coro;
    }
    constexpr auto await_resume() noexcept {
      return awaiter_coro.promise().transfer_return_value();
    }
  private:
    handle awaiter_coro;
  };

  auto operator co_await () { return awaiter(coro); }

private:
  handle coro;
};

template<> struct generator<void>::promise_type {
  promise_type() {}
  ~promise_type() {}
  auto get_return_object() { return generator{handle::from_promise(*this)}; }
  auto initial_suspend() { return suspend_never{}; }
  auto final_suspend() { return suspend_always{}; }
  void unhandled_exception() { std::terminate(); }
  void return_void() {};
  void transfer_return_value() {};
  void *operator new(size_t sz) { return coroutine_allocator.alloc(sz); }
  void operator delete(void *p, size_t sz) { coroutine_allocator.free(p, sz); }

  coroutine_handle<> callee_coro = nullptr;
};
/*
 *  task<T> is implementation of coroutine promise. It supports chained
 *  co_await, which enables writing coroutine as easy as writing normal
 *  functions. However, the coroutine scheduling of task<T> may be a
 *  little different from other 'C++ coroutine library'.
 *
 *  When a chain of task<T> being executed, it somewhat goes like this:
 *
 *    function { coroutine_handle->resume(); }
 *      => coroutine1 { co_await task<Foo1>; }
 *        => coroutine2 { co_await task<Foo2>; }
 *          => coroutine3 { ... } => ...
 *
 *  Here `=>` represents a coroutine call through
 *  `coroutine_handle->resume()` or `co_await task<Foo>`
 *
 *  When `await_suspend` happens, for example in `coroutine2`, it hand over
 *  its control flow directly to the top level `function`. Therefore, any
 *  custom scheduling in `function` (could be seen as the main loop) can
 *  continue to work.
 */
namespace coro_task_private {

using generic_coroutine_handle = coroutine_handle<void>;

// final_awaiter is the awaiter returned on task<T> coroutine's `final_suspend`.
// It returns the control flow directly to the caller instead of the top-level
// function
struct final_awaiter {
  final_awaiter(generic_coroutine_handle coroutine)
      : caller_coroutine_(coroutine) {}
  ~final_awaiter() {}

  final_awaiter(const final_awaiter &) = delete;
  final_awaiter(final_awaiter &&other) : caller_coroutine_(nullptr) {
    std::swap(caller_coroutine_, other.caller_coroutine_);
  }

  constexpr bool await_ready() const noexcept { return false; }
  auto await_suspend(generic_coroutine_handle) const noexcept {
    return caller_coroutine_;
  }
  void await_resume() const noexcept {}

private:
  generic_coroutine_handle caller_coroutine_;
};


// task<T>::promise_type inherits promise_base. promise_base keeps track
// of the coroutine call stack.
struct promise_base {
  promise_base()
      : handle_(nullptr), parent_(nullptr), leaf_(nullptr), root_(nullptr) {}
  ~promise_base() {}

  promise_base(const promise_base &) = delete;
  promise_base(promise_base &&) = delete;

  auto initial_suspend() { return suspend_always{}; }
  auto final_suspend() noexcept {
    // For the first coroutine in the coroutine chain, it is started by
    // normal function through coroutine_handle.resume(). Therefore, it
    // does not be co_awaited on and has no awaiting_promise_.
    // In its final suspend, it returns control flow to its caller by
    // returning std::experimental::noop_coroutine.
    if (!parent_) {
        return coro_task_private::final_awaiter(
            noop_coroutine());
    }
    
    ASSERT(root_);
    root_->leaf_ = parent_;
    return coro_task_private::final_awaiter(
            parent_->get_coro_handle());
  }
  void unhandled_exception() { std::terminate(); }

  // TODO: Use arena allocator. Probably one arena for
  // each chain of coroutine task.
  // It is very important to use arena to reduce the
  // cache miss in access promise_base * which happens
  // a lot in task<T>.resume();
  void *operator new(size_t sz) { return coroutine_allocator.alloc(sz); }
  void operator delete(void *p, size_t sz) { coroutine_allocator.free(p, sz); }

  inline void set_parent(promise_base * caller_promise) {
      parent_ = caller_promise;

      ASSERT(parent_);
      root_ = parent_->root_;

      ASSERT(root_);
      root_->leaf_ = this;
  }

  inline promise_base *get_leaf() const {
      ASSERT(leaf_);
      return leaf_;
  }

  inline void set_as_root() {
      leaf_ = this;
      root_ = this;
  }

  inline generic_coroutine_handle get_coro_handle() const {
      return handle_;
  }

protected:
  generic_coroutine_handle handle_;
  promise_base * parent_;
  promise_base * leaf_;
  promise_base * root_;
};

} // namespace coro_task_private

template <typename T> class [[nodiscard]] task {
public:
  struct promise_type;
  struct awaiter;
  using coroutine_handle_t = coroutine_handle<promise_type>;


  task() : coroutine_(nullptr) {}
  task(coroutine_handle_t coro) : coroutine_(coro) {}
  ~task() {
    if (coroutine_) {
      destroy();
    }
  }

  task(task &&other) : coroutine_(nullptr) {
    std::swap(coroutine_, other.coroutine_);
  }
  task(const task &) = delete;

  task & operator=(task &&other) {
    if (coroutine_) {
      destroy();
    }

    coroutine_ = other.coroutine_;
    other.coroutine_ = nullptr;
    return *this;
  }
  task & operator=(const task &other) = delete;

  bool valid() const {
      return coroutine_ != nullptr;
  }

  bool done() const {
    ASSERT(coroutine_);
    return coroutine_.done();
  }

  void start() {
    ASSERT(coroutine_);
    ASSERT(!coroutine_.done());
    coroutine_.promise().set_as_root();
    coroutine_.resume();
  }

  void resume() {
    ASSERT(coroutine_);
    ASSERT(!coroutine_.done());
    ASSERT(coroutine_.promise().get_leaf());
    ASSERT(coroutine_.promise().get_leaf()->get_coro_handle());
    ASSERT(!coroutine_.promise().get_leaf()->get_coro_handle().done());

    coroutine_.promise().get_leaf()->get_coro_handle().resume();
  }

  void destroy() {
    ASSERT(done());
    coroutine_.destroy();
    coroutine_ = nullptr;
  }

  awaiter operator co_await() const {
    return awaiter(coroutine_);
  }

  // Call get_return_value() on task<> for more than one time is undefined
  template<typename U = T>
  typename std::enable_if<not std::is_same<U, void>::value, U>::type 
  get_return_value() const {
    return coroutine_.promise().transfer_return_value();
  }

private:
  coroutine_handle_t coroutine_;
};

template <> struct task<void>::promise_type : coro_task_private::promise_base {
  using coroutine_handle_t =
      coroutine_handle<typename task<void>::promise_type>;

  promise_type() {}
  ~promise_type() {}

  auto get_return_object() {
    auto coroutine_handle = coroutine_handle_t::from_promise(*this);
    handle_ = coroutine_handle;
    return task{coroutine_handle};
  }

  void return_void() {};
};

template <typename T>
struct task<T>::promise_type : coro_task_private::promise_base {
  using coroutine_handle_t =
      coroutine_handle<typename task<T>::promise_type>;

  friend struct task<T>::awaiter;

  promise_type() {}
  ~promise_type() {
    reinterpret_cast<T*>(&ret_val_buf_)->~T();
  }

  auto get_return_object() {
    auto coroutine_handle = coroutine_handle_t::from_promise(*this);
    handle_ = coroutine_handle;
    return task{coroutine_handle};
  }

  // XXX: explore if there is anyway to get ride of
  // the copy constructing.
  void return_value(T value) {
    new (&ret_val_buf_) T(std::move(value));
  }
  T && transfer_return_value() {
      return std::move(*reinterpret_cast<T*>(&ret_val_buf_));
  }

private:
  struct alignas(alignof(T)) T_Buf {
    uint8_t buf[sizeof(T)];
  };
  T_Buf ret_val_buf_;
};

/*
 * In the first traversal of nested coroutines, a chain of coroutine_handles 
 * will be created by co_await(more exactly, await_suspend) layer-by-layer.
 * Then on the surface it feels like that the deepest coroutine returns the 
 * control to the top-level corutine directly, but in fact the compiler
 * generates return_to_the_caller() to return the control flow layer-by-layer.
 *
 * In the following traversals, since the top-level coroutine is the direct
 * caller or resumer of the deepest coroutine, the deepest coroutine will
 * actually return the control flow to the top-level coroutine directly.
 *
 * The semantics of co_await <expr> can be translated (roughly) as follows:
 * {
 *   if (not awaiter.await_ready()) {
 *     //if await_suspend returns void
 *     ...
 *     //if await_suspend returns bool
 *     ...
 *     //if await_suspend returns another coroutine_handle
 *     try {
 *       another_coro_handle = awaiter.await_suspend(coroutine_handle);
 *     } catch (...) {
 *       goto resume_point;
 *     }
 *     another_coro_handle.resume();
 *     return_to_the_caller();
 *   }
 *  return awaiter.await_resume();
 * }
 *
 */
template <typename T> struct task<T>::awaiter {
  using coroutine_handle_t =
      coroutine_handle<typename task<T>::promise_type>;

  awaiter(coroutine_handle_t task_coroutine)
      : suspended_task_coroutine_(task_coroutine) {}
  ~awaiter() {}

  awaiter(const awaiter &) = delete;
  awaiter(awaiter && other) : suspended_task_coroutine_(nullptr){
      std::swap(suspended_task_coroutine_, other.suspended_task_coroutine_);
  }

  // suspended_task_coroutine points to coroutine being co_awaited on
  //
  // awaiting_coroutine points to the coroutine running co_await
  // (i.e. it waiting for suspended_task_coroutine to complete first)
  template <typename awaiting_promise_t>
  auto await_suspend(coroutine_handle<awaiting_promise_t>
                         awaiting_coroutine) noexcept {
    suspended_task_coroutine_.promise().set_parent(&(awaiting_coroutine.promise()));
    return suspended_task_coroutine_;
  }
  constexpr bool await_ready() const noexcept {
    return suspended_task_coroutine_.done();
  }

  template <typename U>
  using non_void_T =
      typename std::enable_if<not std::is_same<U, void>::value, U>::type;
  template <typename U>
  using void_T =
      typename std::enable_if<std::is_same<U, void>::value, void>::type;

  template <typename U = T> non_void_T<U> await_resume() noexcept {
    ASSERT(suspended_task_coroutine_.done());
    return suspended_task_coroutine_.promise().transfer_return_value();
  }

  template <typename U = T> void_T<U> await_resume() noexcept {
    ASSERT(suspended_task_coroutine_.done());
  }

private:
  coroutine_handle_t suspended_task_coroutine_;
};


} // namespace coro
} // namespace ermia

#ifdef ADV_COROUTINE
  #define PROMISE(t) ermia::coro::task<t>
  #define RETURN co_return
  #define AWAIT co_await
  #define SUSPEND co_await suspend_always{}

template<typename T>
inline T sync_wait_coro(ermia::coro::task<T> &&coro_task) {
    coro_task.start();
    while(!coro_task.done()) {
        coro_task.resume();
    }

    return coro_task.get_return_value();
}

template<>
inline void sync_wait_coro(ermia::coro::task<void> &&coro_task) {
    coro_task.start();
    while(!coro_task.done()) {
        coro_task.resume();
    }
}

inline void sync_wait_void_coro(ermia::coro::task<void> &&coro_task) {
    coro_task.start();
    while(!coro_task.done()) {
        coro_task.resume();
    }
}

#else
  #define PROMISE(t) t
  #define RETURN return
  #define AWAIT
  #define SUSPEND
  #define sync_wait_void_coro (void)

template <typename T>
inline auto sync_wait_coro(T &&t) {
    return std::forward<T>(t);
}

#endif // ADV_COROUTINE
