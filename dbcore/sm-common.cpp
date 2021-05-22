#include "sm-common.h"
#include <string.h>
#include "rcu.h"

#include <glog/logging.h>

#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <unistd.h>
#include <sys/fcntl.h>
#include <vector>

namespace ermia {

void die(char const *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  std::abort();
}

char *os_asprintf(char const *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  DEFER(va_end(ap));

  char *msg;
  int err = vasprintf(&msg, fmt, ap);
  THROW_IF(err < 0, os_error, errno, "Unable to format string template: %s",
           fmt);
  return msg;
}

int os_open(char const *path, int flags) {
  int fd = open(path, flags);
  LOG_IF(FATAL, fd < 0) << "Unable to open file " << path << "(" << fd << ")";
  return fd;
}

int os_openat(int dfd, char const *fname, int flags) {
  int fd = openat(dfd, fname, flags, S_IRUSR | S_IWUSR);
  LOG_IF(FATAL, fd < 0) << "Unable to open file " << fname << "(" << fd << "(";
  return fd;
}

void os_write(int fd, void const *buf, size_t bufsz) {
  size_t err = write(fd, buf, bufsz);
  LOG_IF(FATAL, err != bufsz) << "Error writing " << bufsz << " bytes to file";
}

size_t os_pwrite(int fd, char const *buf, size_t bufsz, off_t offset) {
  size_t n = 0;
  while (n < bufsz) {
    ssize_t m = pwrite(fd, buf + n, bufsz - n, offset + n);
    if (not m) break;
    LOG_IF(FATAL, m < 0) << "Error writing " << bufsz << " bytes at offset "
                         << offset << "(" << errno << ")";
    THROW_IF(m < 0, os_error, errno,
             "Error writing %zd bytes to file at offset %zd", bufsz, offset);
    n += m;
  }
  return n;
}

size_t os_pread(int fd, char *buf, size_t bufsz, off_t offset) {
  size_t n = 0;
  while (n < bufsz) {
    ssize_t m = pread(fd, buf + n, bufsz - n, offset + n);
    if (not m) break;
    LOG_IF(FATAL, m < 0)
      << "Error reading " << bufsz << " bytes from file at offset " << offset;
    n += m;
  }
  return n;
}

void os_truncate(char const *path, size_t size) {
  int err = truncate(path, size);
  THROW_IF(err, os_error, errno, "Error truncating file %s to %zd bytes", path,
           size);
}

void os_truncateat(int dfd, char const *path, size_t size) {
  int fd = os_openat(dfd, path, O_WRONLY | O_CREAT);
  DEFER(os_close(fd));

  int err = ftruncate(fd, size);
  THROW_IF(err, os_error, errno, "Error truncating file %s to %zd bytes", path,
           size);
}

void os_renameat(int fromfd, char const *from, int tofd, char const *to) {
  int err = renameat(fromfd, from, tofd, to);
  THROW_IF(err, os_error, errno, "Error renaming file %s to %s", from, to);
}

void os_unlinkat(int dfd, char const *fname, int flags) {
  int err = unlinkat(dfd, fname, flags);
  THROW_IF(err, os_error, errno, "Error unlinking file %s", fname);
}

#warning os_fsync is not guaranteed to actually do anything (thanks, POSIX)
/* ^^^

   Turns out that POSIX makes no guarantees about what fsync does. In
   the worst case, a no-op implementation is explicitly allowed by the
   standard, and Mac OS X is a real life example.

   The eventual solution will be to add a whole bunch of extra code
   here, covering all possible implementation deficiencies of
   platforms we support. For now, we just ignore the problem.
 */
void os_fsync(int fd) {
  int err = fsync(fd);
  THROW_IF(err, os_error, errno, "Error synching fd %d to disk", fd);
}

void os_close(int fd) {
  int err = close(fd);
  THROW_IF(err, os_error, errno, "Error closing fd %d", fd);
}

int os_dup(int fd) {
  int rval = dup(fd);
  THROW_IF(rval < 0, os_error, errno, "Unable to duplicate fd %d", fd);
  return rval;
}

dirent_iterator::dirent_iterator(char const *dname)
    : _d(opendir(dname)), used(false) {
  THROW_IF(not _d, os_error, errno, "Unable to open/create directory: %s",
           dname);
}

dirent_iterator::~dirent_iterator() {
  int err = closedir(_d);
  WARN_IF(err, "Closing dirent iterator gave errno %d", errno);
}

void dirent_iterator::iterator::operator++() {
  errno = 0;
  _dent = readdir(_d);
  if (not _dent) {
    THROW_IF(errno, os_error, errno, "Error during directory scan");
    _d = NULL;
  }
}

dirent_iterator::iterator dirent_iterator::begin() {
  if (used) rewinddir(_d);

  used = true;
  iterator rval{_d, NULL};
  ++rval;  // prime it
  return rval;
}

dirent_iterator::iterator dirent_iterator::end() {
  return iterator{NULL, NULL};
}

int dirent_iterator::dup() { return os_dup(dirfd(_d)); }
}  // namespace ermia
