# distutils: sources = uberhf/includes/hashmapsrc.c
from libc.string cimport memcpy, strlen, strcmp
from uberhf.includes.asserts cimport cyassert

cdef class HashMapBase:
    """
    Default hashmap algorithm works with strings or structures which have 1st member as a char*

    Override __cinit__, item_compare, item_hash for customization!
    """

    cdef void _new(self,
                   size_t item_size,
                   uint64_t (*item_hash_f)(const void *item, uint64_t seed0, uint64_t seed1) nogil,
                   int (*item_compare_f)(const void *a, const void *b, void *udata) nogil,
                   size_t capacity) nogil:
        self.item_size = item_size
        self._hash_map = hashmap_new(item_size,
                                     capacity,
                                     641234,
                                     290517,
                                     item_hash_f,
                                     item_compare_f,
                                     NULL,
                                     NULL)

        # Returned NULL possible error
        cyassert(self._hash_map != NULL)

    cdef void * set(self, void *item)  nogil:
        """
        // hashmap_set inserts or replaces an item in the hash map. If an item is
        // replaced then it is returned otherwise NULL is returned. This operation
        // may allocate memory. If the system is unable to allocate additional
        // memory then NULL is returned and hashmap_oom() returns true.
        :param item: 
        :return: 
        """
        cyassert(self._hash_map != NULL)

        return hashmap_set(self._hash_map, item)

    cdef void * get(self, void *item)  nogil:
        """
        // hashmap_get returns the item based on the provided key. If the item is not
        // found then NULL is returned.
        :param item: 
        :return: 
        """
        cyassert(self._hash_map != NULL)

        return hashmap_get(self._hash_map, item)

    cdef size_t count(self)  nogil:
        """
        hashmap_count returns the number of items in the hash map.
        :return: 
        """
        cyassert(self._hash_map != NULL)

        return hashmap_count(self._hash_map)

    cdef void clear(self)  nogil:
        """
        // hashmap_clear quickly clears the map.
        // Every item is called with the element-freeing function given in hashmap_new,
        // if present, to free any data referenced in the elements of the hashmap.
        // When the update_cap is provided, the map's capacity will be updated to match
        // the currently number of allocated buckets. This is an optimization to ensure
        // that this operation does not perform any allocations.
        :return: 
        """
        cyassert(self._hash_map != NULL)

        hashmap_clear(self._hash_map, 0)

    cdef void * delete(self, void *item)  nogil:
        """
        // hashmap_delete removes an item from the hash map and returns it. If the
        // item is not found then NULL is returned.
        :param item: 
        :return: 
        """
        cyassert(self._hash_map != NULL)

        return hashmap_delete(self._hash_map, item)

    cdef bint iter(self, size_t *i, void ** item)  nogil:
        """
        // hashmap_iter iterates one key at a time yielding a reference to an
        // entry at each iteration. Useful to write simple loops and avoid writing
        // dedicated callbacks and udata structures, as in hashmap_scan.
        //
        // map is a hash map handle. i is a pointer to a size_t cursor that
        // should be initialized to 0 at the beginning of the loop. item is a void
        // pointer pointer that is populated with the retrieved item. Note that this
        // is NOT a copy of the item stored in the hash map and can be directly
        // modified.
        //
        // Note that if hashmap_delete() is called on the hashmap being iterated,
        // the buckets are rearranged and the iterator must be reset to 0, otherwise
        // unexpected results may be returned after deletion.
        //
        // This function has not been tested for thread safety.
        //
        // The function returns true if an item was retrieved; false if the end of the
        // iteration has been reached.
        
        Example:
            cdef size_t i = 0
            cdef void * hm_data = NULL
            while hm.iter(&i, &hm_data):
                ps = <Struct1*> hm_data
        
        Data is unordered
        :param i: must be 0 at the beginning of the loop, represents internal hashmap index (not sequential 0..N!)
        :param item: 
        :return: 
        """
        cyassert(self._hash_map != NULL)

        return hashmap_iter(self._hash_map, i, item)

    def __dealloc__(self):
        if self._hash_map != NULL:
            hashmap_free(self._hash_map)
            self._hash_map = NULL


cdef class HashMap(HashMapBase):
    def __cinit__(self, int item_size, min_capacity=16):
        self._new(item_size, HashMap.item_hash, HashMap.item_compare, min_capacity)

    @staticmethod
    cdef int item_compare(const void *a, const void *b, void *udata) nogil:
        return strcmp(<char*>a, <char*>b)

    @staticmethod
    cdef uint64_t item_hash(const void *item, uint64_t seed0, uint64_t seed1) nogil:
        return HashMap.hash_func(<char*>item, strlen(<char*>item), seed0, seed1)

    @staticmethod
    cdef uint64_t hash_func(const void *data, size_t data_len, uint64_t seed0, uint64_t seed1) nogil:
        return hashmap_sip(data, data_len, seed0, seed1)
