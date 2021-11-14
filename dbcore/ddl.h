#pragma once

#include <stdio.h>
#include <string.h>

#include "../str_arena.h"
#include "rcu.h"
#include "sm-config.h"
#include "sm-rc.h"
#include "sm-thread.h"

namespace ermia {

namespace ddl {

rc_t scan_copy(transaction *t, str_arena *arena, varstr &value);

bool changed_data_capture_impl(transaction *t, uint32_t thread_id,
                               uint32_t ddl_thread_id, uint32_t begin_log,
                               uint32_t end_log, str_arena *arena,
                               util::fast_random &r);

} // namespace ddl

} // namespace ermia
