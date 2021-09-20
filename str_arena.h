#pragma once

#include "dbcore/sm-config.h"
#include "dbcore/sm-common.h"
#include "varstr.h"
#include <atomic>
#include <memory>

namespace ermia {
class str_arena {
public:
  static const size_t MinStrReserveLength = 2 * CACHELINE_SIZE;
  str_arena(uint32_t size_mb) : n(0) {
    // Make sure arena is only initialized after config is initialized so we have
    // a valid size
    ALWAYS_ASSERT(size_mb == config::arena_size_mb);

    // adler32 (log checksum) needs it aligned
    ALWAYS_ASSERT(
        not posix_memalign((void **)&str, DEFAULT_ALIGNMENT, size_mb * config::MB));
    memset(str, '\0', config::arena_size_mb * config::MB);
    reset();
  }

  // non-copyable/non-movable for the time being
  str_arena(str_arena &&) = delete;
  str_arena(const str_arena &) = delete;
  str_arena &operator=(const str_arena &) = delete;

  inline void reset() {
    ASSERT(n < config::arena_size_mb * config::MB);
    n = 0;
  }

  varstr *next(uint64_t size) {
    uint64_t off = n;
    n += align_up(size + sizeof(varstr));
    if (n >= config::arena_size_mb * config::MB) {
      // printf("id: %u, arena beyond\n", thread::MyId());
      return nullptr;
    }
    ASSERT(n < config::arena_size_mb * config::MB);
    varstr *ret = new (str + off) varstr(str + off + sizeof(varstr), size);
    return ret;
  }

  // Assume the caller is the benchmark using str(Size(v))
  inline void return_space(uint64_t size) {
    n -= (align_up(size + sizeof(varstr)));
  }

  varstr *atomic_next(uint64_t size) {
    uint64_t off = __atomic_fetch_add(&n, align_up(size + sizeof(varstr)), __ATOMIC_ACQ_REL);
    ASSERT(n < config::arena_size_mb * config::MB);
    varstr *ret = new (str + off) varstr(str + off + sizeof(varstr), size);
    return ret;
  }

  inline varstr *operator()(uint64_t size) { return next(size); }

  bool manages(const varstr *px) const {
    return (const char *)px >= str and
           (uint64_t) px->data() + px->size() <= (uint64_t)str + n;
  }

  inline char *get_str() { return str; }

private:
  char *str;
  size_t n;
};

class scoped_str_arena {
public:
  scoped_str_arena(str_arena *arena) : arena(arena) {}

  scoped_str_arena(str_arena &arena) : arena(&arena) {}

  scoped_str_arena(scoped_str_arena &&) = default;

  // non-copyable
  scoped_str_arena(const scoped_str_arena &) = delete;
  scoped_str_arena &operator=(const scoped_str_arena &) = delete;

  ~scoped_str_arena() {
    if (arena)
      arena->reset();
  }

  ALWAYS_INLINE str_arena *get() { return arena; }

private:
  str_arena *arena;
};
} // namespace ermia
