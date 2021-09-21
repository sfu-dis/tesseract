#pragma once

#include "macros.h"
#include "varstr.h"

#include "dbcore/serial.h"
#include "dbcore/sm-object.h"
#include "dbcore/xid.h"

#ifdef SSN
// Indicate somebody has read this tuple and thought it was an old one
#define PERSISTENT_READER_MARK 0x1
#endif

namespace ermia {

/**
 * A dbtuple is the type of value which we stick
 * into underlying (non-transactional) data structures- it
 * also contains the memory of the value
 */
struct dbtuple {
 public:
#if defined(SSN) || defined(SSI)
  TXN::readers_list::bitmap_t readers_bitmap;  // bitmap of in-flight readers
  fat_ptr sstamp;  // successor (overwriter) stamp (\pi in ssn), set to writer
                   // XID during
  // normal write to indicate its existence; become writer cstamp at commit
  uint64_t xstamp;  // access (reader) stamp (\eta), updated when reader commits
  uint64_t preader;  // did I have some reader thinking I'm old?
#endif

#ifdef MVOCC
  fat_ptr sstamp;
#endif

#ifdef SSI
  uint64_t s2;  // smallest successor stamp of all reads performed by the tx
                // that clobbered this version
                // Consider a transaction T which clobbers this version, upon
                // commit, T writes its cstamp in sstamp, and the smallest
                // sstamp among all its reads in s2 of this version. This
                // basically means T has clobbered this version, and meantime,
                // some other transaction C clobbered T's read.
                // So [X] r:w T r:w C. If anyone reads this version again,
                // it will become the X in the dangerous structure above
                // and must abort.
#endif
  uint32_t size;   // actual size of record
  uint8_t value_start[0];  // must be last field

  dbtuple(uint32_t size)
      :
#if defined(SSN) || defined(SSI)
        sstamp(NULL_PTR),
        xstamp(0),
        preader(0),
#endif
#ifdef SSI
        s2(0),
#endif
        size(CheckBounds(size)) {
  }

  ~dbtuple() {}

#if defined(SSN)
  /* return the tuple's age based on a safe_lsn provided by the calling tx.
   * safe_lsn usually = the calling tx's begin offset.
   *
   * and we need to check the type of clsn because a "committed" tx might
   * change the clsn to ASI_LOG type after changing state.
   */
  ALWAYS_INLINE uint64_t age(TXN::xid_context *visitor) {
    uint64_t end = 0;
    XID owner = volatile_read(visitor->owner);

  retry:
    fat_ptr cstamp = GetObject()->GetClsn();

    if (cstamp.asi_type() == fat_ptr::ASI_XID) {
      XID xid = XID::from_ptr(cstamp);
      if (xid == owner)  // my own update
        return 0;
      TXN::xid_context *xc = TXN::xid_get_context(xid);
      end = volatile_read(xc->end);
      if (not xc or xc->owner != xid) goto retry;
    } else {
      ASSERT(cstamp.asi_type() == fat_ptr::ASI_LOG);
      end = cstamp.offset();
    }

    // the caller must be alive...
    return volatile_read(visitor->begin) - end;
  }
  bool is_old(TXN::xid_context *visitor);  // FOR READERS ONLY!
  ALWAYS_INLINE bool set_persistent_reader() {
    uint64_t pr = 0;
    do {
      pr = volatile_read(preader);
      if (pr >> 7)  // some updater already locked it
        return false;
    } while (
        volatile_read(preader) != PERSISTENT_READER_MARK and
        not __sync_bool_compare_and_swap(&preader, pr, PERSISTENT_READER_MARK));
    return true;
  }
#endif
#if defined(SSN)
  ALWAYS_INLINE bool has_persistent_reader() {
    return volatile_read(preader) & PERSISTENT_READER_MARK;
  }
  // XXX: for the writer who's updating this tuple only
  ALWAYS_INLINE void lockout_read_mostly_tx() {
    if (config::ssn_read_opt_enabled()) {
      if (not(volatile_read(preader) >> 7))
        __sync_fetch_and_xor(&preader, uint64_t{1} << 7);
      ASSERT(volatile_read(preader) >> 7);
    }
  }

  // XXX: for the writer who's updating this tuple only
  ALWAYS_INLINE void welcome_read_mostly_tx() {
    if (config::ssn_read_opt_enabled()) {
      if (volatile_read(preader) >> 7)
        __sync_fetch_and_xor(&preader, uint64_t{1} << 7);
      ASSERT(not(volatile_read(preader) >> 7));
    }
  }
#endif

  ALWAYS_INLINE uint8_t *get_value_start() { return &value_start[0]; }

  ALWAYS_INLINE const uint8_t *get_value_start() const {
    return &value_start[0];
  }

  inline Object *GetObject() {
    Object *obj = (Object *)((char *)this - sizeof(Object));
    ASSERT(obj->GetPayload() == (char *)this);
    return obj;
  }

  inline dbtuple *NextVolatile() {
    // So far this is only used by the primary
    Object *myobj = GetObject();
    ASSERT(myobj->GetPayload() == (char *)this);
    Object *next_obj = (Object*)myobj->GetNextVolatile().offset();
    return next_obj ? next_obj->GetPinnedTuple() : nullptr;
  }

 private:
  static ALWAYS_INLINE uint32_t CheckBounds(uint32_t s) {
    ASSERT(s <= std::numeric_limits<uint32_t>::max());
    return s;
  }

 public:
  inline rc_t DoRead(varstr *out_v) const {
    out_v->p = get_value_start();
    out_v->l = size;
    return size > 0 ? rc_t{RC_TRUE} : rc_t{RC_FALSE};
  }
};

}  // namespace ermia
