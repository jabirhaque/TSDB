# Time Series Database (TSDB)

This project is a simple **Time Series Database (TSDB)** implemented in C++ from scratch. It is designed to store sequential timestamped numeric data efficiently and provide a simple interface for reading and writing records. This project is in its early stages of development.

## Planned Features

- **Binary Storage Format**: Store records in a compact binary format for faster read/write and smaller disk usage.
- **Random Access Reads**: Allow querying specific time ranges or individual timestamps without reading the entire file.
- **Data Integrity and Rollback**: Implement safe rollback mechanisms and atomic writes to prevent corruption during crashes.
- **Multi-Series Support**: Manage multiple independent time series in a single database instance.
- **Indexing**: Build lightweight indexes to speed up queries and range scans.
- **Concurrency**: Support multi-threaded reads and writes with proper synchronization.
- **Persistence and Recovery**: Ensure fast recovery and durable storage across program restarts.
- **CLI and API**: Provide a command-line interface and C++ API for inserting, querying, and managing time series data.
- **Performance Metrics**: Track append rates, query times, and storage size to benchmark improvements.

## Technology Stack
![C++](https://img.shields.io/badge/C++-00599C?style=flat-square&logo=C%2B%2B&logoColor=white)
![CMake](https://img.shields.io/badge/CMake-064F8C?logo=cmake&logoColor=fff)
