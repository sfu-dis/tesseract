#include "dlog.h"
#include "dlog-tx.h"
#include "sm-oid.h"
#include "sm-alloc.h"
#include "sm-object.h"

namespace ermia {

namespace dlog {

void tls_log::cdc(transaction *t, uint64_t begin_csn, uint64_t end_csn, std::vector<char *> bufs) {
  printf("log %u cdc, seg size: %lu\n", id, segments.size());
  bool stop_scan = false;
  for (std::vector<segment>::reverse_iterator seg = segments.rbegin(); seg != segments.rend(); seg++) { 
    uint64_t offset_in_seg = 0;
    uint64_t data_sz = seg->size;
    printf("seg size: %lu\n", data_sz);
    char *data_buf = (char *)malloc(data_sz);
    bufs.emplace_back(data_buf);
    size_t m = os_pread(seg->fd, (char *)data_buf, data_sz, offset_in_seg);
    uint64_t block_sz = sizeof(log_block), logrec_sz = sizeof(log_record), tuple_sc = sizeof(dbtuple);

    while (offset_in_seg < data_sz) {
      log_block *header = (log_block *)(data_buf + offset_in_seg);
      if (begin_csn < header->csn) {
        uint64_t offset_in_block = 0;
        while (offset_in_block < header->payload_size) {
          log_record *logrec = (log_record *)(data_buf + offset_in_seg + block_sz + offset_in_block);
          ALWAYS_ASSERT(logrec->oid);
	  ALWAYS_ASSERT(logrec->fid);
	  // printf("logrec->fid: %u\n", logrec->fid);
	  ALWAYS_ASSERT(logrec->rec_size);
	  ALWAYS_ASSERT(logrec->data);
	  dbtuple *tuple = (dbtuple *)(logrec->data);
	  // printf("tuple->size: %u\n", tuple->size);
	  
	  varstr value(tuple->get_value_start(), tuple->size);  

	  /*
	  Object* obj = new (MM::allocate(tuple->size))
	  Object(logrec->data, NULL_PTR, 0, config::eager_warm_up());
          obj->SetClsn(logrec->data);
          ASSERT(obj->GetClsn().asi_type() == fat_ptr::ASI_LOG);
	  */

	  offset_in_block += logrec->rec_size;
        }
      } else {
        stop_scan = true;
      }
      offset_in_seg += header->payload_size + block_sz;
      // printf("payload_size: %u\n", header->payload_size);
    }

    // printf("offset_in_seg: %lu\n", offset_in_seg);
    
    if (stop_scan) break;
  }
}

}  // namespace dlog

}  // namespace ermia

