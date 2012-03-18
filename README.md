Background
==========

*kollekt++* is the C++ version of *kollekt* (written in Scala) and there is also a
NodeJS version, *kollekt-node*.

These tools do collect data streams via multiple UDP packets and flushes them to disk
on different criteria.

Research Subjects
=================

- time effort (cost) vs. productivity/usability/maintenance-time
- source code readability
- performance
    - memory usage
    - CPU load
- scalability (single core vs multi core distribution)
- stability
    - out-of-memory handling
    - CPU exceeding

kollekt++ Resource Usage
========================

File Descriptors
----------------

- stdio: 3 by default (0, 1, 2)
    - stdin (0) could be closed
    - stdout (1) should be connected to the same log stream as stderr (2)
- UDP listener: 1
- event polling (libev): 2
    - one for epoll
    - one for eventfd
- bucking writing to disk: 1
- per bucket:
    - pipe: 2 (reader and writer)


    stdio_fd = 3
    event_fd = 2
    listener_fd = 1
    log_fd = 1
    core_fd = stdio_fd + event_fd + listener_fd + log_fd
    bucket_fd = 2
    
    max_fd(max_buckets) = core_fd + max_buckets * bucket_fd

Memory
------

- sizeof Bucket: N

Memory should grow linear + N with the number of buckets
in userspace plus the buckets buffer size in kernel-space.

CPU
---

CPU load should not increase with the number of buckets.

New Strategie Try
=================

- the main thread is acting as message reader and coordinator thread
- 1 writer thread
- N worker threads for, each managing a set of buckets in a hash.
    - each worker has its own distinct key space
      (with 2 workers: 1 for the first half (a.g. 0..8) and the second for 9..f)

