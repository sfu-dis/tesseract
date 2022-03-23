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
#ifndef MASSTREE_SCAN_HH
#define MASSTREE_SCAN_HH
#include "masstree_struct.hh"
#include "masstree_tcursor.hh"

namespace Masstree {

template <typename P>
class scanstackelt {
 public:
  typedef leaf<P> leaf_type;
  typedef typename leaf_type::leafvalue_type leafvalue_type;
  typedef typename leaf_type::bound_type bound_type;
  typedef typename P::ikey_type ikey_type;
  typedef key<ikey_type> key_type;
  typedef typename leaf_type::permuter_type permuter_type;
  typedef typename P::threadinfo_type threadinfo;
  typedef typename node_base<P>::nodeversion_type nodeversion_type;

  leaf<P> *node() const { return n_; }
  typename nodeversion_type::value_type full_version_value() const {
    return (v_.version_value() << permuter_type::size_bits) + perm_.size();
  }
  int size() const { return perm_.size(); }
  permuter_type permutation() const { return perm_; }
  int operator()(const key_type &k, const scanstackelt<P> &n, int p) {
    return n.n_->compare_key(k, p);
  }

  scanstackelt() {}

  node_base<P> *root_;
  leaf<P> *n_;
  nodeversion_type v_;
  permuter_type perm_;
  int ki_;

  enum { scan_emit, scan_find_next, scan_down, scan_up, scan_retry };

  template <typename H>
  PROMISE(int)
  find_initial(H &helper, key_type &ka, bool emit_equal, leafvalue_type &entry,
               threadinfo &ti);
  template <typename H>
  PROMISE(int)
  find_retry(H &helper, key_type &ka, threadinfo &ti);
  template <typename H>
  int find_next(H &helper, key_type &ka, leafvalue_type &entry);

  int kp() const {
    if (unsigned(ki_) < unsigned(perm_.size()))
      return perm_[ki_];
    else
      return -1;
  }

  template <typename PX>
  friend class basic_table;
};

template <typename P>
struct scan_info {
  typedef scanstackelt<P> mystack_type;
  typedef typename P::ikey_type ikey_type;
  typedef node_base<P> node_type;
  typedef leaf<P> leaf_type;
  typedef typename node_type::key_type key_type;
  typedef typename node_type::leaf_type::leafvalue_type leafvalue_type;
  union {
    ikey_type
        x[(MASSTREE_MAXKEYLEN + sizeof(ikey_type) - 1) / sizeof(ikey_type)];
    char s[MASSTREE_MAXKEYLEN];
  } keybuf;
  key_type ka;
  int state;

  mystack_type
      stack[(MASSTREE_MAXKEYLEN + sizeof(ikey_type) - 1) / sizeof(ikey_type)];
  int stackpos = 0;
  leafvalue_type entry;

  scan_info() {}
  scan_info(const basic_table<P> *bt, Str firstkey) {
    masstree_precondition(firstkey.len <= (int)sizeof(keybuf));
    memcpy(keybuf.s, firstkey.s, firstkey.len);
    ka = key_type(keybuf.s, firstkey.len);
    stack[0].root_ = bt->get_root();
    entry = leafvalue_type::make_empty();
  }
};

struct forward_scan_helper {
  bool initial_ksuf_match(int ksuf_compare, bool emit_equal) const {
    return ksuf_compare > 0 || (ksuf_compare == 0 && emit_equal);
  }
  template <typename K>
  bool is_duplicate(const K &k, typename K::ikey_type ikey, int keylenx) const {
    return k.compare(ikey, keylenx) >= 0;
  }
  template <typename K, typename N>
  int lower(const K &k, const N *n) const {
    return N::bound_type::lower_by(k, *n, *n).i;
  }
  template <typename K, typename N>
  int lower_with_position(const K &k, const N *n, int &kp) const {
    key_indexed_position kx = N::bound_type::lower_by(k, *n, *n);
    kp = kx.p;
    return kx.i;
  }
  void found() const {}
  int next(int ki) const { return ki + 1; }
  template <typename N, typename K>
  N *advance(const N *n, const K &) const {
    return n->safe_next();
  }
  template <typename N, typename K>
  typename N::nodeversion_type stable(const N *n, const K &) const {
    return n->stable();
  }
  template <typename K>
  void shift_clear(K &ka) const {
    ka.shift_clear();
  }
};

struct reverse_scan_helper {
  // We run ki backwards, referring to perm.size() each time through,
  // because inserting elements into a node need not bump its version.
  // Therefore, if we decremented ki, starting from a node's original
  // size(), we might miss some concurrently inserted keys!
  // Also, a node's size might change DURING a lower_bound operation.
  // The "backwards" ki must be calculated using the size taken by the
  // lower_bound, NOT some later size() (which might be bigger or smaller).
  // The helper type reverse_scan_node allows this.
  reverse_scan_helper() : upper_bound_(false) {}
  bool initial_ksuf_match(int ksuf_compare, bool emit_equal) const {
    return ksuf_compare < 0 || (ksuf_compare == 0 && emit_equal);
  }
  template <typename K>
  bool is_duplicate(const K &k, typename K::ikey_type ikey, int keylenx) const {
    return k.compare(ikey, keylenx) <= 0 && !upper_bound_;
  }
  template <typename K, typename N>
  int lower(const K &k, const N *n) const {
    if (upper_bound_) return n->size() - 1;
    key_indexed_position kx = N::bound_type::lower_by(k, *n, *n);
    return kx.i - (kx.p < 0);
  }
  template <typename K, typename N>
  int lower_with_position(const K &k, const N *n, int &kp) const {
    key_indexed_position kx = N::bound_type::lower_by(k, *n, *n);
    kp = kx.p;
    return kx.i - (kx.p < 0);
  }
  int next(int ki) const { return ki - 1; }
  void found() const { upper_bound_ = false; }
  template <typename N, typename K>
  N *advance(const N *n, K &k) const {
    k.assign_store_ikey(n->ikey_bound());
    k.assign_store_length(0);
    return n->prev_;
  }
  template <typename N, typename K>
  typename N::nodeversion_type stable(N *&n, const K &k) const {
    while (1) {
      typename N::nodeversion_type v = n->stable();
      N *next = n->safe_next();
      int cmp;
      if (!next || (cmp = ::compare(k.ikey(), next->ikey_bound())) < 0 ||
          (cmp == 0 && k.length() == 0))
        return v;
      n = next;
    }
  }
  template <typename K>
  void shift_clear(K &ka) const {
    ka.shift_clear_reverse();
    upper_bound_ = true;
  }

 private:
  mutable bool upper_bound_;
};

template <typename P>
template <typename H>
PROMISE(int)
scanstackelt<P>::find_initial(H &helper, key_type &ka, bool emit_equal,
                              leafvalue_type &entry, threadinfo &ti) {
  int kp, keylenx = 0;
  char suffixbuf[MASSTREE_MAXKEYLEN];
  Str suffix;

retry_root:
  n_ = AWAIT root_->reach_leaf(ka, v_, ti);

retry_node:
  if (v_.deleted()) goto retry_root;
  n_->prefetch();
  perm_ = n_->permutation();

  ki_ = helper.lower_with_position(ka, this, kp);
  if (kp >= 0) {
    keylenx = n_->keylenx_[kp];
    fence();
    entry = n_->lv_[kp];
    entry.prefetch(keylenx);
    if (n_->keylenx_has_ksuf(keylenx)) {
      suffix = n_->ksuf(kp);
      memcpy(suffixbuf, suffix.s, suffix.len);
      suffix.s = suffixbuf;
    }
  }
  if (n_->has_changed(v_)) {
    n_ = n_->advance_to_key(ka, v_, ti);
    goto retry_node;
  }

  if (kp >= 0) {
    if (n_->keylenx_is_layer(keylenx)) {
      this[1].root_ = entry.layer();
      RETURN scan_down;
    } else if (n_->keylenx_has_ksuf(keylenx)) {
      int ksuf_compare = suffix.compare(ka.suffix());
      if (helper.initial_ksuf_match(ksuf_compare, emit_equal)) {
        int keylen = ka.assign_store_suffix(suffix);
        ka.assign_store_length(keylen);
        RETURN scan_emit;
      }
    } else if (emit_equal)
      RETURN scan_emit;
    // otherwise, this entry must be skipped
    ki_ = helper.next(ki_);
  }

  RETURN scan_find_next;
}

template <typename P>
template <typename H>
PROMISE(int)
scanstackelt<P>::find_retry(H &helper, key_type &ka, threadinfo &ti) {
retry:
  n_ = AWAIT root_->reach_leaf(ka, v_, ti);
  if (v_.deleted()) goto retry;

  n_->prefetch();
  perm_ = n_->permutation();
  ki_ = helper.lower(ka, this);
  RETURN scan_find_next;
}

template <typename P>
template <typename H>
int scanstackelt<P>::find_next(H &helper, key_type &ka, leafvalue_type &entry) {
  int kp;

  if (v_.deleted()) return scan_retry;

retry_entry:
  kp = this->kp();
  if (kp >= 0) {
    ikey_type ikey = n_->ikey0_[kp];
    int keylenx = n_->keylenx_[kp];
    int keylen = keylenx;
    fence();
    entry = n_->lv_[kp];
    entry.prefetch(keylenx);
    if (n_->keylenx_has_ksuf(keylenx))
      keylen = ka.assign_store_suffix(n_->ksuf(kp));

    if (n_->has_changed(v_))
      goto changed;
    else if (helper.is_duplicate(ka, ikey, keylenx)) {
      ki_ = helper.next(ki_);
      goto retry_entry;
    }

    // We know we can emit the data collected above.
    ka.assign_store_ikey(ikey);
    helper.found();
    if (n_->keylenx_is_layer(keylenx)) {
      this[1].root_ = entry.layer();
      return scan_down;
    } else {
      ka.assign_store_length(keylen);
      return scan_emit;
    }
  }

  if (!n_->has_changed(v_)) {
    n_ = helper.advance(n_, ka);
    if (!n_) return scan_up;
    n_->prefetch();
  }

changed:
  v_ = helper.stable(n_, ka);
  perm_ = n_->permutation();
  ki_ = helper.lower(ka, this);
  return scan_find_next;
}

template <typename P>
template <bool IsNext, typename H, typename F>
PROMISE(bool)
basic_table<P>::scan_init_or_next_value(H helper, F &scanner,
                                        ermia::TXN::xid_context *xc,
                                        threadinfo &ti,
                                        scan_info<P> *si) const {
  if (IsNext) {
    si->stack[si->stackpos].ki_ = helper.next(si->stack[si->stackpos].ki_);
    si->state = si->stack[si->stackpos].find_next(helper, si->ka, si->entry);
  }

  while (1) {
    switch (si->state) {
      case scan_info<P>::mystack_type::scan_emit: {
        if (!scanner.visit_value_no_callback(si->ka)) {
          RETURN false;
        }
        RETURN true;
      } break;

      case scan_info<P>::mystack_type::scan_find_next:
      find_next:
        si->state =
            si->stack[si->stackpos].find_next(helper, si->ka, si->entry);
        if (si->state != scan_info<P>::mystack_type::scan_up)
          scanner.visit_leaf(si->stack[si->stackpos], si->ka, ti);
        break;

      case scan_info<P>::mystack_type::scan_up:
        do {
          if (--si->stackpos < 0) RETURN false;
          si->ka.unshift();
          si->stack[si->stackpos].ki_ =
              helper.next(si->stack[si->stackpos].ki_);
        } while (unlikely(si->ka.empty()));
        goto find_next;

      case scan_info<P>::mystack_type::scan_down:
        helper.shift_clear(si->ka);
        ++si->stackpos;
        goto retry;

      case scan_info<P>::mystack_type::scan_retry:
      retry:
        si->state =
            AWAIT si->stack[si->stackpos].find_retry(helper, si->ka, ti);
        break;
    }
  }

  RETURN true;
}

template <typename P>
template <typename H, typename F>
PROMISE(int)
basic_table<P>::scan(H helper, Str firstkey, bool emit_firstkey, F &scanner,
                     ermia::TXN::xid_context *xc, threadinfo &ti) const {
  typedef typename P::ikey_type ikey_type;
  typedef typename node_type::key_type key_type;
  typedef typename node_type::leaf_type::leafvalue_type leafvalue_type;
  union {
    ikey_type
        x[(MASSTREE_MAXKEYLEN + sizeof(ikey_type) - 1) / sizeof(ikey_type)];
    char s[MASSTREE_MAXKEYLEN];
  } keybuf;
  masstree_precondition(firstkey.len <= (int)sizeof(keybuf));
  memcpy(keybuf.s, firstkey.s, firstkey.len);
  key_type ka(keybuf.s, firstkey.len);

  typedef scanstackelt<param_type> mystack_type;
  mystack_type
      stack[(MASSTREE_MAXKEYLEN + sizeof(ikey_type) - 1) / sizeof(ikey_type)];
  int stackpos = 0;
  stack[0].root_ = root_;
  leafvalue_type entry = leafvalue_type::make_empty();

  int scancount = 0;
  int state;

  while (1) {
    state = AWAIT stack[stackpos].find_initial(helper, ka, emit_firstkey, entry,
                                               ti);
    scanner.visit_leaf(stack[stackpos], ka, ti);
    if (state != mystack_type::scan_down) break;
    ka.shift();
    ++stackpos;
  }

  while (1) {
    switch (state) {
      case mystack_type::scan_emit: {  // surpress cross init warning about v
        ++scancount;
        ermia::dbtuple *v = NULL;
        ermia::OID o = entry.value();
        v = AWAIT ermia::oidmgr->oid_get_version(tuple_array_, o, xc);
        if (v) {
          if (!scanner.visit_value(ka, v, o)) goto done;
        }
#ifdef OPTLAZYDDL
        else {
          if (!scanner.visit_value(ka, v, o)) goto done;
        }
#endif
        stack[stackpos].ki_ = helper.next(stack[stackpos].ki_);
        state = stack[stackpos].find_next(helper, ka, entry);
      } break;

      case mystack_type::scan_find_next:
      find_next:
        state = stack[stackpos].find_next(helper, ka, entry);
        if (state != mystack_type::scan_up)
          scanner.visit_leaf(stack[stackpos], ka, ti);
        break;

      case mystack_type::scan_up:
        do {
          if (--stackpos < 0) goto done;
          ka.unshift();
          stack[stackpos].ki_ = helper.next(stack[stackpos].ki_);
        } while (unlikely(ka.empty()));
        goto find_next;

      case mystack_type::scan_down:
        helper.shift_clear(ka);
        ++stackpos;
        goto retry;

      case mystack_type::scan_retry:
      retry:
        state = AWAIT stack[stackpos].find_retry(helper, ka, ti);
        break;
    }
  }

done:
  RETURN scancount;
}

template <typename P>
template <typename F>
PROMISE(int)
basic_table<P>::scan(Str firstkey, bool emit_firstkey, F &scanner,
                     ermia::TXN::xid_context *xc, threadinfo &ti) const {
  return scan(forward_scan_helper(), firstkey, emit_firstkey, scanner, xc, ti);
}

template <typename P>
template <typename F>
PROMISE(int)
basic_table<P>::rscan(Str firstkey, bool emit_firstkey, F &scanner,
                      ermia::TXN::xid_context *xc, threadinfo &ti) const {
  return scan(reverse_scan_helper(), firstkey, emit_firstkey, scanner, xc, ti);
}

}  // namespace Masstree
#endif
