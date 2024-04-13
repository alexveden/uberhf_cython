from libc.stdio cimport FILE, fopen, fwrite, fclose
from libc.stdlib cimport malloc
from posix.fcntl cimport open, O_RDONLY, O_CREAT, O_EXCL, O_RDWR
from posix.unistd cimport close, read, off_t, ftruncate
from posix.types cimport mode_t
from libc.string cimport memset
import os

cdef extern from "sys/stat.h":
    cdef struct stat:
        off_t st_size
    int fstat(int fildes, stat *buf)
    enum:
        S_IRWXU



cdef extern from "sys/mman.h":
    void *mmap(void *addr, size_t len, int prot, int flags, int fd, off_t offset)
    int shm_open(const char *name, int oflag, mode_t mode)
    int shm_unlink(const char *name)

    enum:
        PROT_READ
        PROT_WRITE
        MAP_FILE
        MAP_SHARED

def main(size):
    cdef int _flags = O_CREAT | O_EXCL | O_RDWR
    cdef char * fn = '/test_jupyter1'
    cdef int _fd = shm_open(fn, _flags, S_IRWXU)
    if _fd == -1:
        shm_unlink(fn)
        _fd = shm_open(fn, _flags, S_IRWXU)
        assert _fd != -1, f'File already exists: {fn}'

    cdef stat statbuf
    fstat(_fd, &statbuf)

    ftruncate(_fd, size)
    print(size)

    cdef void * buf = mmap(NULL, statbuf.st_size, PROT_WRITE | PROT_READ, MAP_FILE | MAP_SHARED, _fd, 0)
    cdef int buf_count = statbuf.st_size

    memset(buf, b'b', buf_count)

    # # Write size header
    # fwrite(&K, sizeof(int), 1, f)
    # # Create K IntList instances of random size and content and write them
    # for k in range(K):
    #     x = IntList(random.randrange(N/2, N))
    #     for i in range(len(x)):
    #         x[i] = random.randrange(-N, N)
    #     x.write_handle(f)

    #  buf = &(<int*>buf)[1] # skip size header

    cdef char * data = <char *> buf

    #b = data[0]
    data[0] = 1

    for i in range(buf_count):
        b = data[i]
        # if i < buf_count-1:
        #     data[i] = b'b'
        # else:
        #     data[i] = b'z'

    #b = &(<int*>buf)[0] # skip size header
    #print(data)

    if _fd != -1:
        close(_fd)
        shm_unlink(fn)
        print('closed')

    return _fd