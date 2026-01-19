# Time Series Database (TSDB)

This project is a simple **Time Series Database (TSDB)** implemented in C++ from scratch. It is designed to store sequential timestamped numeric data efficiently and provide a simple interface for reading and writing records. This project is in its early stages of development.

## Features

| Feature                         | Description                                                                                                  | Progress                                                        |
|---------------------------------|--------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------|
| **Binary Storage Format**       | Store records in a compact binary format for faster read/write and smaller disk usage                        | ![Progress Bar](https://progress-bar.xyz/100/?title=Completed)  |
| **Data Integrity and Recovery** | Implement safe recovery mechanisms and atomic writes to durable storage to prevent corruption during crashes | ![Progress Bar](https://progress-bar.xyz/100/?title=Completed)  |
| **Indexing**                    | Build lightweight indexes to speed up queries and range scans                                                | ![Progress Bar](https://progress-bar.xyz/100/?title=Completed)  |
| **Random Access Reads**         | Allow querying specific time ranges or individual timestamps without reading the entire file                 | ![Progress Bar](https://progress-bar.xyz/100/?title=Completed)  |
| **Concurrency**                 | Support multi-threaded reads and writes with proper synchronization                                          | ![Progress Bar](https://progress-bar.xyz/100/?title=Completed)  |
| **Command Line Interface**      | Provide a command-line interface for inserting, querying, and managing time series data                      | ![Progress Bar](https://progress-bar.xyz/40/?title=Developing) |
| **Performance Metrics**         | Track append rates, query times, and storage size to benchmark improvements                                  | ![Progress Bar](https://progress-bar.xyz/70/?title=Developing)    |

## Technology Stack
![C++](https://img.shields.io/badge/C++-00599C?style=flat-square&logo=C%2B%2B&logoColor=white)
![CMake](https://img.shields.io/badge/CMake-064F8C?logo=cmake&logoColor=fff)
