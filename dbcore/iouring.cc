#include "iouring.h"

thread_local IOUringClass IOUring;

std::set<sessionid_t> GetIOCompleteSessions () {
    std::set<sessionid_t> ioComplete;

    int ret = -1;
    unsigned long seid = 0;
    while (ret = IOUring.get_write_completion (seid), ret >= 0) {
        ioComplete.insert (seid);
    };

    return ioComplete;
}