from posix.fcntl cimport open, O_RDONLY, O_CREAT, O_EXCL, O_RDWR
from posix.unistd cimport close, read, off_t, ftruncate
from posix.mman cimport mmap, munmap, shm_open, shm_unlink, PROT_READ, PROT_WRITE, MAP_SHARED, MAP_FAILED
from posix.stat cimport struct_stat, fstat, S_IRWXU
from libc.limits cimport UINT_MAX
from posix.types cimport off_t, mode_t
from libc.errno cimport errno, ENOENT
from libc.string cimport strerror, memset
from libc.stdlib cimport malloc, free
from uberhf.includes.asserts cimport cyassert, cybreakpoint
from uberhf.includes.uhfprotocols cimport TRANSPORT_SENDER_SIZE, V2_TICKER_MAX_LEN, ProtocolStatus, TRANSPORT_HDR_MGC
from uberhf.includes.hashmap cimport HashMap
from uberhf.includes.utils cimport strlcpy, is_str_valid
from libc.math cimport NAN, HUGE_VAL
from libc.limits cimport LONG_MAX
import fcntl
import os
from multiprocessing import Lock

lock = Lock()

cdef class SharedQuotesCache:
    def __cinit__(self, unsigned int uhffeed_life_id, int source_capacity, int quotes_capacity, shared_filename=b'/uhfeed_shared_cache'):
        self.uhffeed_life_id = uhffeed_life_id
        self.is_server = uhffeed_life_id != 0
        self.mmap_data = NULL
        self.shmem_fd = -1
        self.shared_filename = shared_filename

        cdef int sh_fn_access = 0
        if self.is_server:
            if not lock.acquire(block=False):
                # Locking for multiple instances of the server mode
                raise RuntimeError(f'Trying to launch multiple SharedQuotesCache class instances')

            # Locking for multiple processes
            self.lock_acquired = 1
            self.lock_fd = os.open(f"/tmp/instance_uhfeed_core.lock", os.O_WRONLY | os.O_CREAT)
            try:
                fcntl.lockf(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError:
                raise RuntimeError(f'Trying to launch multiple SharedQuotesCache server processes')

            assert source_capacity > 0
            assert quotes_capacity > 0

            sh_fn_access = O_CREAT | O_RDWR
        else:
            assert source_capacity == 0, 'Client must always set source_capacity to 0'
            assert quotes_capacity == 0, 'Client must always set quote_capacity to 0'
            sh_fn_access = O_RDONLY
            self.lock_acquired = 0 # Client don't hold locks
            self.lock_fd = -1

        self.shmem_fd = shm_open(self.shared_filename, sh_fn_access, S_IRWXU)

        if self.shmem_fd == -1:
            raise FileNotFoundError(f'shm_open: possibly no server running. {strerror(errno)}')

        cdef struct_stat statbuf
        fstat(self.shmem_fd, &statbuf)

        cdef size_t shmem_size = 0
        is_new_file = False

        if statbuf.st_size == 0:
            cyassert(self.is_server == 1) # Only for servers!
            # New file lets make a valid cache
            shmem_size = SharedQuotesCache.calc_shmem_size(source_capacity, quotes_capacity)
            ftruncate(self.shmem_fd, shmem_size)
            is_new_file = True
        else:
            shmem_size = statbuf.st_size

        if self.is_server:
            self.mmap_data = mmap(NULL, shmem_size, PROT_WRITE | PROT_READ, MAP_SHARED, self.shmem_fd, 0)
            self.mmap_size = shmem_size

            if self.mmap_data == MAP_FAILED:
                # Close shmem descriptor
                close(self.shmem_fd)
                self.shmem_fd = -1
                self.mmap_data = NULL
                assert self.mmap_data != MAP_FAILED, f'mmap: error {strerror(errno)}'
                self.mmap_size = 0
                raise RuntimeError(f'mmap failed')
            if is_new_file:
                # Zero memory if the file is new to avoid junk data
                memset(self.mmap_data, 0, self.mmap_size)
        else:
            self.mmap_data = mmap(NULL, shmem_size, PROT_READ, MAP_SHARED, self.shmem_fd, 0)
            self.mmap_size = shmem_size

        # man mmap: After the mmap() call has returned, the file descriptor, fd, can be closed immediately without invalidating the mapping.
        #close(_fd)

        #
        # Initializing the headers
        #
        self.header = <QCHeader*>self.mmap_data

        cdef QCSourceHeader* src_h
        cdef QCRecord * q

        if self.is_server:
            self.header.uhffeed_life_id = uhffeed_life_id
            self.header.magic_number = TRANSPORT_HDR_MGC
            if is_new_file:
                self.header.quote_count = 0
                self.header.quote_capacity = quotes_capacity
                self.header.source_count = 0
                self.header.source_capacity = source_capacity

            self.header.quote_errors = 0
            self.header.source_errors = 0
        else:
            # Nothing to change for a client
            pass

        self.sources = <QCSourceHeader *> (self.mmap_data + sizeof(QCHeader))
        self.records = <QCRecord *> (self.mmap_data + sizeof(QCHeader) + self.header.source_capacity * sizeof(QCSourceHeader))

        self.source_map = HashMap(sizeof(Name2Idx), self.header.source_capacity)
        self.ticker_map = HashMap(sizeof(Name2Idx), self.header.quote_capacity)

        if self.is_server and not is_new_file:
            assert self.header.quote_capacity >= quotes_capacity, f'Existing quote capacity less than requested'
            assert self.header.source_capacity >= source_capacity, f'Existing source capacity less than requested'

        self._reload_sources_or_srvreset()
        self._reload_quotes()

    cdef void _reload_sources_or_srvreset(self) nogil:
        cdef Name2Idx nidx
        cdef QCRecord * q
        cdef int n_valid_sources = 0
        cdef int i, j
        for i in range(self.header.source_count):
            src_h = &self.sources[i]
            if src_h.magic_number != TRANSPORT_HDR_MGC:
                continue

            strlcpy(nidx.name, src_h.data_source_id, TRANSPORT_SENDER_SIZE)
            nidx.idx = i
            n_valid_sources += 1
            self.source_map.set(&nidx)

            if self.is_server:
                # Reset source stats
                src_h.quotes_status = ProtocolStatus.UHF_INACTIVE
                src_h.data_source_life_id = 0
                src_h.source_errors = 0
                src_h.quote_errors = 0

                for j in range(self.header.quote_count):
                    q = &self.records[j]
                    if q.magic_number != TRANSPORT_HDR_MGC:
                        continue
                    # Also resetting all quotes of this source
                    if q.data_source_hidx == i:
                        cyassert(strcmp(q.data_source_id, src_h.data_source_id) == 0)
                        SharedQuotesCache.reset_quote(&q.quote)

        cyassert(self.header.source_count == n_valid_sources)
        cyassert(<size_t>self.header.source_count == self.source_map.count())

    cdef void _reload_quotes(self) nogil:
        cdef Name2Idx nidx
        cdef int n_valid_quotes = 0

        for i in range(self.header.quote_count):
            q = &self.records[i]
            if q.magic_number != TRANSPORT_HDR_MGC:
                continue
            strlcpy(nidx.name, q.v2_ticker, V2_TICKER_MAX_LEN)
            nidx.idx = i
            n_valid_quotes += 1
            self.ticker_map.set(&nidx)

        cyassert(self.header.quote_count == n_valid_quotes)
        cyassert(<size_t>self.header.quote_count == self.ticker_map.count())

    @staticmethod
    cdef size_t calc_shmem_size(int source_capacity, int quotes_capacity):
        cyassert(source_capacity > 0)
        cyassert(quotes_capacity > 0)

        return sizeof(QCHeader) + source_capacity*sizeof(QCSourceHeader) + quotes_capacity*sizeof(QCRecord)

    @staticmethod
    cdef void reset_quote(Quote *q) nogil:
        q.bid = NAN
        q.ask = NAN
        q.bid_size = NAN
        q.ask_size = NAN
        q.last = NAN
        q.last_upd_utc = 0


    cdef int source_initialize(self, char * data_src_id, unsigned int data_source_life_id) nogil:
        """
        Starts source initialization
        
        :param data_src_id: 
        :param data_source_life_id: 
        :return: 
        """
        cyassert(self.is_server)
        cyassert(self.source_map.count() == <size_t>self.header.source_count)

        if not is_str_valid(data_src_id, TRANSPORT_SENDER_SIZE):
            self.header.source_errors += 1
            return -1
        if data_source_life_id == 0:
            self.header.source_errors += 1
            return -2

        cdef int src_i = -1
        cdef Name2Idx * src_idx = <Name2Idx*>self.source_map.get(data_src_id)
        cdef QCSourceHeader * src_h = NULL
        if src_idx == NULL:
            # new source
            if self.header.source_count == self.header.source_capacity:
                # Source capacity overflow
                self.header.source_errors += 1
                return -3

            src_idx = <Name2Idx*>malloc(sizeof(Name2Idx))

            strlcpy(src_idx.name, data_src_id, TRANSPORT_SENDER_SIZE)
            src_idx.idx = self.header.source_count
            src_i = self.header.source_count
            self.header.source_count += 1
            self.source_map.set(src_idx)
            src_h = &self.sources[src_i]

            free(src_idx)
        else:
            src_i = src_idx.idx
            cyassert(src_i < self.header.source_capacity)
            src_h = &self.sources[src_i]

            cyassert(src_h.magic_number == TRANSPORT_HDR_MGC)

            if src_h.data_source_life_id != 0 and <int>(src_h.data_source_life_id/10**8) != <int>(data_source_life_id / 10 ** 8):
                # Sources under the same name but different module ids
                src_h.quotes_status = ProtocolStatus.UHF_ERROR
                return -100


        if strcmp(src_h.data_source_id, data_src_id) != 0 or src_h.magic_number != TRANSPORT_HDR_MGC:
            # Malformed of new source
            strlcpy(src_h.data_source_id, data_src_id, TRANSPORT_SENDER_SIZE)
            src_h.magic_number = TRANSPORT_HDR_MGC

        src_h.data_source_life_id = data_source_life_id
        src_h.quotes_status = ProtocolStatus.UHF_INITIALIZING
        src_h.quotes_processed = 0
        src_h.iinfo_processed = 0
        src_h.instruments_registered = 0
        src_h.last_quote_ns = 0
        src_h.quote_errors = 0
        src_h.source_errors = 0

        cyassert(<size_t>self.header.source_count == self.source_map.count())
        return src_i

    cdef int source_register_instrument(self, char * data_src_id, char * v2_ticker, uint64_t instrument_id, InstrumentInfo * iinfo) nogil:
        """
        Data source registers new instrument
        
        :param data_src_id: aka sender id 
        :param v2_ticker: full qualified v2 ticker
        :param instrument_id: unique for datasource instrument id
        :param iinfo: instrument info structure
        
        :return: negative if error, >= 0 as quote index of the new instrument 
        """
        cyassert(self.is_server)
        cyassert(self.ticker_map.count() == <size_t>self.header.quote_count)

        if not is_str_valid(data_src_id, TRANSPORT_SENDER_SIZE):
            self.header.source_errors += 1
            return -1

        if not is_str_valid(v2_ticker, V2_TICKER_MAX_LEN):
            self.header.source_errors += 1
            return -2
        if instrument_id == 0:
            self.header.source_errors += 1
            return -3

        cdef Name2Idx * src_idx = <Name2Idx*>self.source_map.get(data_src_id)
        if src_idx == NULL:
            self.header.source_errors += 1
            return -4

        cdef QCSourceHeader * src_h = &self.sources[src_idx.idx]
        cdef QCRecord * q = NULL
        cdef int q_idx = -1

        if strcmp(src_h.data_source_id, data_src_id) != 0 or src_h.quotes_status !=  ProtocolStatus.UHF_INITIALIZING:
            self.header.source_errors += 1
            src_h.source_errors += 1
            return -5

        cdef Name2Idx * instrument_idx = <Name2Idx *> self.ticker_map.get(v2_ticker)
        if instrument_idx == NULL:
            # Add new instrument
            if self.header.quote_count == self.header.quote_capacity:
                self.header.source_errors += 1
                return -6

            instrument_idx = <Name2Idx*>malloc(sizeof(Name2Idx))
            strlcpy(instrument_idx.name, v2_ticker, V2_TICKER_MAX_LEN)
            instrument_idx.idx = self.header.quote_count
            self.ticker_map.set(instrument_idx)
            free(instrument_idx)

            q_idx = self.header.quote_count
            q = &self.records[q_idx]
            self.header.quote_count += 1
            strlcpy(q.v2_ticker, v2_ticker, V2_TICKER_MAX_LEN)
            strlcpy(q.data_source_id, data_src_id, TRANSPORT_SENDER_SIZE)
            q.instrument_id = instrument_id
            q.magic_number = TRANSPORT_HDR_MGC
            q.subscriptions_bits = 0
        else:
            q_idx = instrument_idx.idx
            q = &self.records[q_idx]
            cyassert(strcmp(q.v2_ticker, v2_ticker) == 0)
            cyassert(q.magic_number == TRANSPORT_HDR_MGC)

            if q.instrument_id != instrument_id:
                src_h.source_errors += 1
                self.header.source_errors += 1
                return -7
            if strcmp(q.data_source_id, src_h.data_source_id) != 0:
                src_h.source_errors += 1
                self.header.source_errors += 1
                return -8

        q.data_source_hidx = src_idx.idx
        # Dereference and copy!
        q.iinfo = iinfo[0]
        SharedQuotesCache.reset_quote(&q.quote)
        src_h.instruments_registered += 1

        cyassert(<size_t>self.header.quote_count == self.ticker_map.count())
        return q_idx


    cdef int source_activate(self, char * data_src_id) nogil:
        """
        Activates data source 
        
        :param data_src_id: previously registered source 
        
        :return: negative on error, source index in self.sources 
        """
        cyassert(self.is_server)

        if not is_str_valid(data_src_id, TRANSPORT_SENDER_SIZE):
            self.header.source_errors += 1
            return -1

        cdef Name2Idx * src_idx = <Name2Idx *> self.source_map.get(data_src_id)
        if src_idx == NULL:
            self.header.source_errors += 1
            return -2

        cdef QCSourceHeader * src_h = &self.sources[src_idx.idx]

        if src_h.instruments_registered == 0:
            src_h.quotes_status = ProtocolStatus.UHF_INACTIVE
            src_h.source_errors += 1
            self.header.source_errors += 1
            return -3
        else:
            if src_h.quotes_status == ProtocolStatus.UHF_INITIALIZING:
                src_h.quotes_status = ProtocolStatus.UHF_ACTIVE
                return src_idx.idx
            else:
                return -4

    cdef int source_disconnect(self, char * data_src_id) nogil:
        """
        Marks source as inactive
        
        :param data_src_id: previously registered source
        :return: negative on error, source index in self.sources
        """
        cyassert(self.is_server)

        if not is_str_valid(data_src_id, TRANSPORT_SENDER_SIZE):
            self.header.source_errors += 1
            return -1

        cdef Name2Idx * src_idx = <Name2Idx *> self.source_map.get(data_src_id)
        if src_idx == NULL:
            self.header.source_errors += 1
            return -2

        cdef QCSourceHeader * src_h = &self.sources[src_idx.idx]

        src_h.quotes_status = ProtocolStatus.UHF_INACTIVE
        src_h.data_source_life_id = 0

        return src_idx.idx

    cdef int source_on_quote(self, ProtocolDSQuoteMessage * msg) nogil:
        """
        Processing quote messages
        
        :param msg: 
        :return: negative on error, or quote cache index of updated quote
        """
        cyassert(self.is_server)

        if msg.instrument_index < 0 or msg.instrument_index >= self.header.quote_count:
            self.header.quote_errors += 1
            return -1

        cdef QCRecord * q = &self.records[msg.instrument_index]
        cdef QCSourceHeader * src_h = &self.sources[q.data_source_hidx]
        if src_h.data_source_life_id != msg.header.client_life_id:
            self.header.quote_errors += 1
            src_h.quote_errors += 1
            return -2
        if self.header.uhffeed_life_id != msg.header.server_life_id:
            self.header.quote_errors += 1
            src_h.quote_errors += 1
            return -3

        if q.instrument_id != msg.instrument_id:
            self.header.quote_errors += 1
            src_h.quote_errors += 1
            return -4

        if msg.is_snapshot:
            # Full quote snapshot make a full copy
            q.quote = msg.quote
        else:
            if msg.quote.ask != HUGE_VAL:
                q.quote.ask = msg.quote.ask
            if msg.quote.bid != HUGE_VAL:
                q.quote.bid = msg.quote.bid
            if msg.quote.last != HUGE_VAL:
                q.quote.last = msg.quote.last
            if msg.quote.ask_size != HUGE_VAL:
                q.quote.ask_size = msg.quote.ask_size
            if msg.quote.bid_size != HUGE_VAL:
                q.quote.bid_size = msg.quote.bid_size

            q.quote.last_upd_utc = msg.quote.last_upd_utc

        src_h.last_quote_ns = q.quote.last_upd_utc
        src_h.quotes_processed += 1

        return msg.instrument_index

    cdef int feed_on_subscribe(self, char * v2_ticker, uint64_t client_life_id, bint is_subscribe) nogil:
        cyassert(self.is_server == 1)

        if not is_str_valid(v2_ticker, V2_TICKER_MAX_LEN):
            return -1

        if client_life_id < 10UL**8UL or client_life_id > UINT_MAX:
            return -2

        cdef uint64_t module_id = <int>(client_life_id / 10**8)

        #
        if module_id <= 0 or module_id > 40:
            return -3

        cdef Name2Idx * tckr_idx = <Name2Idx *> self.ticker_map.get(v2_ticker)
        if tckr_idx == NULL:
            return -4

        cdef QCRecord * qr = &self.records[tckr_idx.idx]

        if is_subscribe:
            qr.subscriptions_bits |= 1UL << module_id
        else:
            qr.subscriptions_bits &= ~(1UL << module_id)
        #cybreakpoint(1)

        return tckr_idx.idx


    cdef QCRecord * get(self, char * v2_ticker) nogil:
        cyassert(self.is_server == 0) # Only for clients!

        if not is_str_valid(v2_ticker, V2_TICKER_MAX_LEN):
            return NULL
        
        if <size_t>self.header.quote_count != self.ticker_map.count():
            self._reload_quotes()

        cdef Name2Idx * tckr_idx = <Name2Idx*>self.ticker_map.get(v2_ticker)
        if tckr_idx == NULL:
            return NULL
        cyassert(tckr_idx.idx >= 0 and tckr_idx.idx < self.header.quote_capacity)
        return &self.records[tckr_idx.idx]

    cdef QCSourceHeader * get_source(self, char * data_source_id) nogil:
        cyassert(self.is_server == 0) # Only for clients!

        if data_source_id == NULL:
            return NULL

        if <size_t>self.header.source_count != self.source_map.count():
            self._reload_sources_or_srvreset()

        cdef Name2Idx * src_idx = <Name2Idx *> self.source_map.get(data_source_id)
        if src_idx == NULL:
            return NULL
        cyassert(src_idx.idx >= 0 and src_idx.idx < self.header.quote_capacity)

        return &self.sources[src_idx.idx]

    cdef close(self):
        # TODO: decide - unlink doesn't keep memopy for open client
        #if self.is_server:
        #    shm_unlink(SHARED_FN)

        if self.is_server:
            if self.mmap_data != NULL:
                # Setting all sources as inactive and closing
                self._reload_sources_or_srvreset()

        if self.mmap_data != NULL:
            munmap(self.mmap_data, self.mmap_size)
            self.mmap_data = NULL

        if self.shmem_fd != -1:
            close(self.shmem_fd)

        if self.lock_fd != -1:
            close(self.lock_fd)

        if self.lock_acquired:
            lock.release()
            self.lock_acquired = 0

    def __dealloc__(self):
        self.close()
