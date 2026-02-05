# Time Series Database (TSDB)

This project is a simple **Time Series Database (TSDB)** implemented in C++ from scratch. It is designed to store sequential timestamped numeric data efficiently and provide a simple interface for reading and writing records. This project is in its early stages of development.

## Features

- Compact binary layout achieving 24 bytes per record disk footprint and enabling high speed operations
- Atomic writes and data integrity checks with CRC32 checksum, partial write detection and truncation
recovery protocols, ensuring durable and consistent storage
- Lightweight sparse index structure to support O(log n) range scans and timestamp specific queries
whilst controlling memory usage, delivering average read latency 35 ms, p99 94 ms
- In-memory write buffers with periodic 5 ms fsync flushes while reducing syscall overhead, achieving average append latency 103 ns,
p99 250 ns
- Multithreaded write support with robust synchronisation achieving high throughput under load
- Command line tools for data insertion, queries including aggregate functions, database management and performance metrics

## Technology Stack
![C++](https://img.shields.io/badge/C++-00599C?style=flat-square&logo=C%2B%2B&logoColor=white)
![CMake](https://img.shields.io/badge/CMake-064F8C?logo=cmake&logoColor=fff)
