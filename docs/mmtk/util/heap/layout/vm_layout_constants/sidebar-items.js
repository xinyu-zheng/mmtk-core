initSidebarItems({"constant":[["AVAILABLE_BYTES","Size of the address space available to the MMTk heap. "],["AVAILABLE_END","Highest virtual address available for MMTk to manage.  The address space between HEAP_END and AVAILABLE_END comprises memory directly managed by the VM, and not available to MMTk."],["AVAILABLE_START","Lowest virtual address available for MMTk to manage.  The address space between HEAP_START and AVAILABLE_START comprises memory directly managed by the VM, and not available to MMTk."],["BYTES_IN_CHUNK","Coarsest unit of address space allocation. "],["HEAP_END",""],["HEAP_START",""],["LOG_ADDRESS_SPACE","log_2 of the addressable virtual space "],["LOG_BYTES_IN_CHUNK","log_2 of the coarsest unit of address space allocation.  In the 32-bit VM layout, this determines the granularity of allocation in a discontigouous space.  In the 64-bit layout, this determines the growth factor of the large contiguous spaces that we provide."],["LOG_MAX_CHUNKS","log_2 of the maximum number of chunks we need to track.  Only used in 32-bit layout."],["LOG_MMAP_CHUNK_BYTES","Granularity at which we map and unmap virtual address space in the heap "],["LOG_PAGES_IN_SPACE64","log_2 of the number of pages in a 64-bit space "],["LOG_SPACE_EXTENT","An upper bound on the extent of any space in the current memory layout"],["MAX_CHUNKS","Maximum number of chunks we need to track.  Only used in 32-bit layout. "],["MAX_SPACE_EXTENT","An upper bound on the extent of any space in the current memory layout"],["MMAP_CHUNK_BYTES",""],["PAGES_IN_CHUNK","Coarsest unit of address space allocation, in pages "],["PAGES_IN_SPACE64","The number of pages in a 64-bit space "],["SPACE_MASK_64",""],["SPACE_SHIFT_64",""],["SPACE_SIZE_64",""],["VM_SPACE_SIZE",""]]});