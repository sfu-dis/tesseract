#include "sm-alloc.h"
#include "sm-object.h"
#include "../tuple.h"
#include "../engine.h"

namespace ermia {

// Dig out the payload from the durable log
// ptr should point to some position in the log and its size_code should refer
// to only data size (i.e., the size of the payload of dbtuple rounded up).
// Returns a fat_ptr to the object created
PROMISE(void) Object::Pin() {
  // config::always_load is a scenario where we always dig out the payload from the durable log,
  // even if the version is already in-memory.
  if (config::always_load) {
try_load:
    uint32_t val = __sync_val_compare_and_swap(&status_, kStatusMemory, kStatusLoading);
    if (val == kStatusLoading) {
      // Serialize the concurrent readers such that they don't mess up with the same version.
      while (volatile_read(status_) != kStatusMemory) {}
      goto try_load;
    }
  } else {
    uint32_t status = volatile_read(status_);
    if (status != kStatusStorage) {
      if (status == kStatusLoading) {
        while (volatile_read(status_) != kStatusMemory) {
        }
      }
      ALWAYS_ASSERT(volatile_read(status_) == kStatusMemory ||
                    volatile_read(status_) == kStatusDeleted);
      RETURN;
    }

    // Try to 'lock' the status
    // TODO(tzwang): have the thread do something else while waiting?
    uint32_t val =
        __sync_val_compare_and_swap(&status_, kStatusStorage, kStatusLoading);
    if (val == kStatusMemory) {
      RETURN;
    } else if (val == kStatusLoading) {
      while (volatile_read(status_) != kStatusMemory) {}
      RETURN;
    } else {
      ASSERT(val == kStatusStorage);
      ASSERT(volatile_read(status_) == kStatusLoading);
    }
  }

  uint32_t final_status = kStatusMemory;

  // Now we can load it from the durable log
  ALWAYS_ASSERT(pdest_._ptr);
  uint16_t where = pdest_.asi_type();
  ALWAYS_ASSERT(where == fat_ptr::ASI_LOG || where == fat_ptr::ASI_CHK);

  // Already pre-allocated space when creating the object
  dbtuple *tuple = (dbtuple *)GetPayload();
  new (tuple) dbtuple(0);  // set the correct size later

  size_t data_sz = decode_size_aligned(pdest_.size_code());
  if (where == fat_ptr::ASI_LOG) {
    LSN lsn = LSN::from_ptr(pdest_);
    auto *log = GetLog(lsn.logid());

    ASSERT(pdest_.log_segment() == lsn.segment());
    ASSERT(lsn.segment() >= 0 && lsn.segment() <= NUM_LOG_SEGMENTS);
    auto *segment = log->get_segment(lsn.segment());
    ASSERT(segment);

    dlog::log_record *logrec = (dlog::log_record *)malloc(data_sz);
    uint64_t offset_in_seg = lsn.loffset() - segment->start_offset;
    // TODO(khuang): add io_uring path here, and suspend right after issuing read 
    // size_t m = pread(segment->fd, (char *)logrec, data_sz, offset_in_seg);
    log->issue_read(segment->fd, (char *)logrec, data_sz, offset_in_seg);
    while(log->peek_read()) {
      SUSPEND;
    }
    // ALWAYS_ASSERT(m == data_sz);

    // Copy the entire dbtuple including dbtuple header and data
    memcpy(tuple, &logrec->data[0], sizeof(dbtuple) + ((dbtuple *)logrec->data)->size);

    // Could be a delete
    ASSERT(tuple->size < data_sz);
    if (tuple->size == 0) {
      final_status = kStatusDeleted;
      ASSERT(next_pdest_.offset());
    }

    // Set CSN
    fat_ptr csn_ptr = GenerateCsnPtr(logrec->csn);
    SetCSN(csn_ptr);
    ASSERT(GetCSN().asi_type() == fat_ptr::ASI_CSN);
  } else {
    ALWAYS_ASSERT(0);
    /*
    // Load tuple data form the chkpt file
    ASSERT(sm_chkpt_mgr::base_chkpt_fd);
    ALWAYS_ASSERT(pdest_.offset());
    ASSERT(volatile_read(status_) == kStatusLoading);
    // Skip the status_ and alloc_epoch_ fields
    static const uint32_t skip = sizeof(status_) + sizeof(alloc_epoch_);
    uint32_t read_size = data_sz - skip;
    auto n = os_pread(sm_chkpt_mgr::base_chkpt_fd, (char *)this + skip,
                      read_size, pdest_.offset() + skip);
    ALWAYS_ASSERT(n == read_size);
    ASSERT(tuple->size <= read_size - sizeof(dbtuple));
    next_pdest_ = NULL_PTR;
    */
  }
  // FIXME(khuang): asi_type() is not ASI_LOG.
#if 0
  //ASSERT(csn_.asi_type() == fat_ptr::ASI_LOG);
  //ALWAYS_ASSERT(pdest_.offset());
  //ALWAYS_ASSERT(csn_.offset());
#endif
  ASSERT(volatile_read(status_) == kStatusLoading);
  // In the always_load scenario, the final status would not be set
  // until the payload is copied to the local buffer in the benchmark drivers.
  if (!config::always_load) {
    SetStatus(final_status);
  }
}

fat_ptr Object::Create(const varstr *tuple_value, epoch_num epoch) {
  // Calculate tuple size
  const uint32_t data_sz = tuple_value ? tuple_value->size() : 0;
  size_t alloc_sz = sizeof(dbtuple) + sizeof(Object) + data_sz;

  // Allocate a version
  Object *obj = new (MM::allocate(alloc_sz)) Object();
  // In case we got it from the tls reuse pool
  ASSERT(obj->GetAllocateEpoch() <= epoch - 4);
  obj->SetAllocateEpoch(epoch);

  // Tuple setup
  dbtuple *tuple = (dbtuple *)obj->GetPayload();
  new (tuple) dbtuple(data_sz);
  if (tuple_value) {
    memcpy(tuple->get_value_start(), tuple_value->p, data_sz);
  }

  size_t size_code = encode_size_aligned(alloc_sz);
  ASSERT(size_code != INVALID_SIZE_CODE);
  return fat_ptr::make(obj, size_code, 0 /* 0: in-memory */);
}

// Make sure the object has a valid csn/pdest
fat_ptr Object::GenerateCsnPtr(uint64_t csn) {
  fat_ptr csn_ptr = CSN::make(csn).to_ptr();
  uint16_t s = csn_ptr.asi_type();
  ASSERT(s == fat_ptr::ASI_CSN);
  return csn_ptr;
}

}  // namespace ermia
