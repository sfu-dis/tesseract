#include "sm-object.h"
#include "../engine.h"
#include "../tuple.h"
#include "sm-alloc.h"

namespace ermia {

// Dig out the payload from the durable log
// ptr should point to some position in the log and its size_code should refer
// to only data size (i.e., the size of the payload of dbtuple rounded up).
// Returns a fat_ptr to the object created
void Object::Pin() {
  uint32_t status = volatile_read(status_);
  if (status != kStatusStorage) {
    if (status == kStatusLoading) {
      while (volatile_read(status_) != kStatusMemory) {
      }
    }
    ALWAYS_ASSERT(volatile_read(status_) == kStatusMemory ||
                  volatile_read(status_) == kStatusDeleted);
    return;
  }

  // Try to 'lock' the status
  // TODO(tzwang): have the thread do something else while waiting?
  uint32_t val =
      __sync_val_compare_and_swap(&status_, kStatusStorage, kStatusLoading);
  if (val == kStatusMemory) {
    return;
  } else if (val == kStatusLoading) {
    while (volatile_read(status_) != kStatusMemory) {}
    return;
  } else {
    ASSERT(val == kStatusStorage);
    ASSERT(volatile_read(status_) == kStatusLoading);
  }

  uint32_t final_status = kStatusMemory;

  // Now we can load it from the durable log
  ALWAYS_ASSERT(pdest_.offset());
  uint16_t where = pdest_.asi_type();
  ALWAYS_ASSERT(where == fat_ptr::ASI_LOG || where == fat_ptr::ASI_CHK);

  // Already pre-allocated space when creating the object
  dbtuple *tuple = (dbtuple *)GetPayload();
  new (tuple) dbtuple(0);  // set the correct size later

  size_t data_sz = decode_size_aligned(pdest_.size_code());
  if (where == fat_ptr::ASI_LOG) {
    auto segnum = pdest_.log_segment();
    ASSERT(segnum >= 0 && segnum <= NUM_LOG_SEGMENTS);

    auto *log = GetLog();
    auto *segment = log->get_segment(segnum);
    ASSERT(segment);

    dlog::log_block *lb = (dlog::log_block *)malloc(data_sz);
    uint64_t tlsn = LSN::from_ptr(pdest_).loffset();
    uint64_t offset_in_seg = tlsn - segment->start_offset;
    size_t m = os_pread(segment->fd, (char *)lb, data_sz, offset_in_seg);

    // Strip out the header
    tuple->size = lb->payload_size;
    memcpy(tuple->get_value_start(), lb->get_payload(), lb->payload_size);

    // Could be a delete
    ASSERT(tuple->size < data_sz);
    if (tuple->size == 0) {
      final_status = kStatusDeleted;
      ASSERT(next_pdest_.offset());
    }

    // Set CSN
    fat_ptr csn_ptr = GenerateCsnPtr(lb->csn);
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
  ASSERT(csn_.asi_type() == fat_ptr::ASI_LOG);
  ALWAYS_ASSERT(pdest_.offset());
  ALWAYS_ASSERT(csn_.offset());
  ASSERT(volatile_read(status_) == kStatusLoading);
  SetStatus(final_status);
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
