/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2014 President and Fellows of Harvard College
 * Copyright (c) 2012-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, subject to the conditions
 * listed in the Masstree LICENSE file. These conditions include: you must
 * preserve this copyright notice, and you cannot mention the copyright
 * holders in advertising related to the Software without their permission.
 * The Software is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This
 * notice is a summary of the Masstree LICENSE file; the license in that file
 * is legally binding.
 */
#ifndef MASSTREE_HH
#define MASSTREE_HH
#include "../dbcore/sm-coroutine.h"
#include "../dbcore/sm-oid.h"
#include "../dbcore/xid.h"
#include "../macros.h"
#include "ksearch.hh"
#include "str.hh"

namespace Masstree {
using lcdf::Str;
using lcdf::String;

template <typename T>
class value_print;

template <int LW = 15, int IW = LW>
struct nodeparams {
  static constexpr int leaf_width = LW;
  static constexpr int internode_width = IW;
  static constexpr bool concurrent = true;
  static constexpr bool prefetch = true;
  static constexpr int bound_method = bound_method_binary;
  static constexpr int debug_level = 0;
  static constexpr bool printable_keys = true;
  typedef uint64_t ikey_type;
};

template <int LW, int IW>
constexpr int nodeparams<LW, IW>::leaf_width;
template <int LW, int IW>
constexpr int nodeparams<LW, IW>::internode_width;
template <int LW, int IW>
constexpr int nodeparams<LW, IW>::debug_level;

template <typename P>
class node_base;
template <typename P>
class leaf;
template <typename P>
class internode;
template <typename P>
class leafvalue;
template <typename P>
class key;
template <typename P>
class basic_table;
template <typename P>
class unlocked_tcursor;
template <typename P>
class tcursor;

template <typename P>
struct scan_info;

template <typename P>
class basic_table {
 public:
  typedef P param_type;
  typedef node_base<P> node_type;
  typedef leaf<P> leaf_type;
  typedef typename P::value_type value_type;
  typedef typename P::threadinfo_type threadinfo;
  typedef unlocked_tcursor<P> unlocked_cursor_type;
  typedef tcursor<P> cursor_type;

  inline basic_table();

  void initialize(threadinfo &ti);
  void destroy(threadinfo &ti);

  inline node_type *root() const;
  inline node_type *fix_root();

  PROMISE(bool) get(Str key, value_type &value, threadinfo &ti) const;

  template <typename F>
  PROMISE(int)
  scan(Str firstkey, bool matchfirst, F &scanner, ermia::TXN::xid_context *xc,
       threadinfo &ti) const;
  template <typename F>
  PROMISE(int)
  rscan(Str firstkey, bool matchfirst, F &scanner, ermia::TXN::xid_context *xc,
        threadinfo &ti) const;

  template <typename F>
  inline int modify(Str key, F &f, threadinfo &ti);
  template <typename F>
  inline int modify_insert(Str key, F &f, threadinfo &ti);

  inline void print(FILE *f = 0, int indent = 0) const;
  inline void set_tuple_array(ermia::oid_array *oa) { tuple_array_ = oa; }
  inline void set_pdest_array(ermia::oid_array *oa) { pdest_array_ = oa; }
  inline node_type *get_root() const { return root_; }

  node_type *root_;
  ermia::oid_array *tuple_array_;
  ermia::oid_array *pdest_array_;

 public:
  template <typename H, typename F>
  PROMISE(int)
  scan(H helper, Str firstkey, bool matchfirst, F &scanner,
       ermia::TXN::xid_context *xc, threadinfo &ti) const;

  template <bool IsNext, typename H, typename F>
  PROMISE(bool)
  scan_init_or_next_value(H helper, F &scanner, ermia::TXN::xid_context *xc,
                          threadinfo &ti, scan_info<P> *si) const;

  friend class unlocked_tcursor<P>;
  friend class tcursor<P>;
};

}  // namespace Masstree
#endif
