#pragma once
#include <fcntl.h>
#include <liburing.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>

#include <set>

class IOUringClass {
private:
    struct io_uring ring;

public:
    bool inited = false;
    void init (int entries) {
        io_uring_queue_init (entries, &ring, 0);
        inited = true;
    }
    void exit () { io_uring_queue_exit (&ring); }

    int submit_write_request (int fd, char* data, int len, void* private_data) {
        struct io_uring_sqe* sqe = io_uring_get_sqe (&ring);
        io_uring_prep_write (sqe, fd, data, len, 0);
        io_uring_sqe_set_data (sqe, private_data);
        int ret = io_uring_submit (&ring);
        return ret;
    }

    int get_write_completion (unsigned long &seid) {
        struct io_uring_cqe* cqe;
        int ret = io_uring_peek_cqe (&ring, &cqe);
        if (ret < 0) {
            return -1;
        }
        if (cqe->res < 0) {
            return -11;
        }
        // TODO: cqe->res should be the same as request
        seid = (long)io_uring_cqe_get_data (cqe);

        io_uring_cqe_seen (&ring, cqe);
        return 0;
    }
};

using sessionid_t = uint16_t;

extern thread_local IOUringClass IOUring;

extern std::set<sessionid_t> GetIOCompleteSessions ();