#include "TSDBCLI.hpp"
#include <iostream>
#include <filesystem>
#include <algorithm>

TSDBCLI::TSDBCLI() : storage(nullptr)
{
}

// void TSDBCLI::handleCommand(const std::string& command)
// {
//     if (command == "help")
//     {
//         printHelp();
//     }
//     else if (command == "performance")
//     {
//         std::cout << "Entering performance metric mode...\n";
//         std::string db = "performance.tsdb";
//
//         if (std::filesystem::exists(db))
//         {
//             std::cout << "Database already exists\n";
//             return;
//         }
//
//         if (storage)
//         {
//             storage.reset();
//         }
//         storage = std::make_unique<Storage>(db);
//
//         std::cout << "Performance metric mode activated. Starting performance tests...\n";
//
//         const int producerCount = 4;
//         const int recordsPerProducer = 1'000'000 / producerCount;
//
//         std::vector<std::thread> producers;
//         std::vector<long long> appendTimes;
//         std::vector<int64_t> timestamps;
//         long long totalAppendTime = 0;
//         std::mutex mtx;
//
//         for (int p = 0; p < producerCount; ++p)
//         {
//             producers.emplace_back([&, p]() {
//                 std::vector<long long> local_times;
//                 std::vector<int64_t> local_timestamps;
//                 local_times.reserve(recordsPerProducer);
//                 long long local_total = 0;
//
//                 for (int i = 0; i < recordsPerProducer; ++i)
//                 {
//                     Record r;
//                     r.timestamp = p * 1'000'000 + i;
//                     r.value = static_cast<double>(i);
//
//                     auto start = std::chrono::high_resolution_clock::now();
//                     bool accepted = (*storage).append(r);
//                     auto end = std::chrono::high_resolution_clock::now();
//
//                     long long duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
//                     local_times.push_back(duration_ns);
//                     if (accepted) local_timestamps.push_back(r.timestamp);
//                     local_total += duration_ns;
//                 }
//
//                 {
//                     std::lock_guard<std::mutex> lock(mtx);
//                     appendTimes.insert(appendTimes.end(), local_times.begin(), local_times.end());
//                     timestamps.insert(timestamps.end(), local_timestamps.begin(), local_timestamps.end());
//                     totalAppendTime += local_total;
//                 }
//             });
//         }
//
//         for (auto& t : producers)
//         {
//             t.join();
//         }
//
//         std::cout << "Average append time: " << (totalAppendTime / appendTimes.size()) << " ns\n";
//
//         std::sort(appendTimes.begin(), appendTimes.end());
//         long long p99 = appendTimes[appendTimes.size() * 99 / 100];
//         long long p95 = appendTimes[appendTimes.size() * 95 / 100];
//         long long p50 = appendTimes[appendTimes.size() / 2];
//
//         std::cout << "p50 append time: " << p50 << " ns\n";
//         std::cout << "p95 append time: " << p95 << " ns\n";
//         std::cout << "p99 append time: " << p99 << " ns\n";
//
//         std::this_thread::sleep_for(std::chrono::milliseconds(1000));
//
//         std::vector<long long> readFromTimes;
//         long long totalReadFromTime = 0;
//
//         for (int i=0; i<timestamps.size(); i+=100)
//         {
//             int64_t ts = timestamps[i];
//             auto start = std::chrono::high_resolution_clock::now();
//             (*storage).readFromTime(ts);
//             auto end = std::chrono::high_resolution_clock::now();
//
//             long long duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
//             readFromTimes.push_back(duration_ns);
//             totalReadFromTime += duration_ns;
//         }
//
//         std::cout << "\nAverage read from time: " << static_cast<double>((totalReadFromTime / readFromTimes.size()))/1000 << " ms\n";
//
//         std::sort(readFromTimes.begin(), readFromTimes.end());
//         p99 = readFromTimes[readFromTimes.size() * 99 / 100];
//         p95 = readFromTimes[readFromTimes.size() * 95 / 100];
//         p50 = readFromTimes[readFromTimes.size() / 2];
//
//         std::cout << "p50 read from time: " << static_cast<double>(p50)/1000 << " ms\n";
//         std::cout << "p95 read from time: " << static_cast<double>(p95)/1000 << " ms\n";
//         std::cout << "p99 read from time: " << static_cast<double>(p99)/1000 << " ms\n";
//
//         storage.reset();
//         std::filesystem::remove(db);
//         std::cout << "Performance metric mode exited. Database deleted.\n";
//     }
//     else if (command.rfind("create ", 0) == 0)
//     {
//         if (!validateCreateCommand(command))
//         {
//             std::cout << "Invalid create command. Usage: create <database> where <database> contains letters and numbers only\n";
//             return;
//         }
//
//         if (command == "create performance")
//         {
//             std::cout << "The database name 'performance' is reserved for performance metric mode. Please choose a different name.\n";
//             return;
//         }
//
//         std::istringstream iss(command);
//         std::string ignore;
//         std::string db;
//
//         iss >> ignore >> db;
//
//         db += ".tsdb";
//
//         if (std::filesystem::exists(db))
//         {
//             std::cout << "Database already exists\n";
//             return;
//         }
//         if (storage)
//         {
//             storage.reset();
//         }
//         storage = std::make_unique<Storage>(db);
//     }
//     else if (command.rfind("use ", 0) == 0)
//     {
//         if (!validateUseCommand(command))
//         {
//             std::cout << "Invalid use command. Usage: use <database> where <database> contains letters and numbers only\n";
//             return;
//         }
//         std::istringstream iss(command);
//         std::string ignore;
//         std::string db;
//
//         iss >> ignore >> db;
//
//         db += ".tsdb";
//
//         if (!std::filesystem::exists(db))
//         {
//             std::cout << "Database not recognised\n";
//             return;
//         }
//         if (storage)
//         {
//             storage.reset();
//         }
//         storage = std::make_unique<Storage>(db);
//     }
//     else if (command == "readall")
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         std::vector<Record> records = (*storage).readAll();
//         for (const Record& r : records)
//         {
//             std::cout << "Timestamp: " << r.timestamp << ", Value: " << r.value << "\n";
//         }
//     }
//     else if (command.rfind("readfrom ", 0) == 0)
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         if (!validateReadFromCommand(command))
//         {
//             std::cout << "Invalid readfrom command. Usage: readfrom <timestamp>\n";
//             return;
//         }
//         std::istringstream iss(command);
//         std::string ignore;
//         int64_t number1;
//
//         iss >> ignore >> number1;
//
//         std::optional<Record> record = (*storage).readFromTime(number1);
//
//         if (record.has_value())
//         {
//             std::cout << "Timestamp: " << record.value().timestamp << ", Value: " << record.value().value << "\n";
//         }
//         else
//         {
//             std::cout << "No record found\n";
//         }
//     }
//     else if (command.rfind("readrange ", 0) == 0)
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         if (!validateGeneralRangeCommand("readrange ", command))
//         {
//             std::cout << "Invalid readrange command. Usage: readrange <start> <end>\n";
//             return;
//         }
//         std::istringstream iss(command);
//         std::string ignore;
//         int64_t number1, number2;
//
//         iss >> ignore >> number1 >> number2;
//
//         if (number1 > number2)
//         {
//             std::cout << "Invalid time range: start time is greater than end time.\n";
//             return;
//         }
//
//         std::vector<Record> records = (*storage).readRange(number1, number2);
//         if (records.empty())
//         {
//             std::cout << "No record found\n";
//         }
//         else
//         {
//             for (const Record& r : records)
//             {
//                 std::cout << "Timestamp: " << r.timestamp << ", Value: " << r.value << "\n";
//             }
//         }
//     }
//     else if (command.rfind("append ", 0) == 0)
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         if (!validateAppendCommand(command))
//         {
//             std::cout << "Invalid append command. Usage: append <timestamp> <value>\n";
//             return;
//         }
//         std::istringstream iss(command);
//         std::string ignore;
//         int64_t timestamp;;
//         double value;
//
//         iss >> ignore >> timestamp >> value;
//
//         bool success = (*storage).append(Record{timestamp, value});
//
//         if (success) std::cout << "Record accepted, pending persistence\n";
//         else std::cout << "Failed to accept record.\n";
//     }
//     else if (command == "count")
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         size_t count = (*storage).readAll().size();
//         std::cout << "Total records: " << count << "\n";
//     }
//     else if (command.rfind("count ", 0) == 0)
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         if (!validateGeneralRangeCommand("count ", command))
//         {
//             std::cout << "Invalid count command. Usage: count <start> <end>\n";
//             return;
//         }
//         std::istringstream iss(command);
//         std::string ignore;
//         int64_t number1, number2;
//
//         iss >> ignore >> number1 >> number2;
//
//         if (number1 > number2)
//         {
//             std::cout << "Invalid time range: start time is greater than end time.\n";
//             return;
//         }
//
//         size_t count = (*storage).readRange(number1, number2).size();
//         std::cout << "Total records: " << count << "\n";
//     }
//     else if (command == "first")
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         std::vector<Record> records = (*storage).readAll();
//         if (records.empty())
//         {
//             std::cout << "No record found\n";
//         }
//         else
//         {
//             std::cout << "Timestamp: " << records.front().timestamp << ", Value: " << records.front().value << "\n";
//         }
//     }
//     else if (command.rfind("first ", 0) == 0)
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         if (!validateGeneralRangeCommand("first ", command))
//         {
//             std::cout << "Invalid first command. Usage: first <start> <end>\n";
//             return;
//         }
//         std::istringstream iss(command);
//         std::string ignore;
//         int64_t number1, number2;
//
//         iss >> ignore >> number1 >> number2;
//
//         if (number1 > number2)
//         {
//             std::cout << "Invalid time range: start time is greater than end time.\n";
//             return;
//         }
//
//         std::vector<Record> records = (*storage).readRange(number1, number2);
//         if (records.empty())
//         {
//             std::cout << "No record found\n";
//         }
//         else
//         {
//             std::cout << "Timestamp: " << records.front().timestamp << ", Value: " << records.front().value << "\n";
//         }
//     }
//     else if (command == "last")
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         std::vector<Record> records = (*storage).readAll();
//         if (records.empty())
//         {
//             std::cout << "No record found\n";
//         }
//         else
//         {
//             std::cout << "Timestamp: " << records.back().timestamp << ", Value: " << records.back().value << "\n";
//         }
//     }
//     else if (command.rfind("last ", 0) == 0)
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         if (!validateGeneralRangeCommand("last ", command))
//         {
//             std::cout << "Invalid last command. Usage: last <start> <end>\n";
//             return;
//         }
//         std::istringstream iss(command);
//         std::string ignore;
//         int64_t number1, number2;
//
//         iss >> ignore >> number1 >> number2;
//
//         if (number1 > number2)
//         {
//             std::cout << "Invalid time range: start time is greater than end time.\n";
//             return;
//         }
//
//         std::vector<Record> records = (*storage).readRange(number1, number2);
//         if (records.empty())
//         {
//             std::cout << "No record found\n";
//         }
//         else
//         {
//             std::cout << "Timestamp: " << records.back().timestamp << ", Value: " << records.back().value << "\n";
//         }
//     }
//     else if (command == "sum")
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         std::vector<Record> records = (*storage).readAll();
//         double sum = 0;
//         for (const Record& r: records) sum += r.value;
//         std::cout << "Sum of values: " << sum << "\n";
//     }
//     else if (command.rfind("sum ", 0) == 0)
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         if (!validateGeneralRangeCommand("sum ", command))
//         {
//             std::cout << "Invalid last command. Usage: sum <start> <end>\n";
//             return;
//         }
//         std::istringstream iss(command);
//         std::string ignore;
//         int64_t number1, number2;
//
//         iss >> ignore >> number1 >> number2;
//
//         if (number1 > number2)
//         {
//             std::cout << "Invalid time range: start time is greater than end time.\n";
//             return;
//         }
//
//         std::vector<Record> records = (*storage).readRange(number1, number2);
//         double sum = 0;
//         for (const Record& r: records) sum += r.value;
//         std::cout << "Sum of values: " << sum << "\n";
//     }
//     else if (command == "min")
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         std::vector<Record> records = (*storage).readAll();
//         if (records.empty()) {
//             std::cout << "No record found\n";
//             return;
//         }
//         double min = double(records.front().value);
//         for (const Record& r: records) min = std::min(min, r.value);
//         std::cout << "Minimum of values: " << min << "\n";
//     }
//     else if (command.rfind("min ", 0) == 0)
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         if (!validateGeneralRangeCommand("min ", command))
//         {
//             std::cout << "Invalid last command. Usage: min <start> <end>\n";
//             return;
//         }
//         std::istringstream iss(command);
//         std::string ignore;
//         int64_t number1, number2;
//
//         iss >> ignore >> number1 >> number2;
//
//         if (number1 > number2)
//         {
//             std::cout << "Invalid time range: start time is greater than end time.\n";
//             return;
//         }
//
//         std::vector<Record> records = (*storage).readRange(number1, number2);
//         if (records.empty()) {
//             std::cout << "No record found\n";
//             return;
//         }
//         double min = double(records.front().value);
//         for (const Record& r: records) min = std::min(min, r.value);
//         std::cout << "Minimum of values: " << min << "\n";
//     }
//     else if (command == "max")
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         std::vector<Record> records = (*storage).readAll();
//         if (records.empty()) {
//             std::cout << "No record found\n";
//             return;
//         }
//         double max = double(records.front().value);
//         for (const Record& r: records) max = std::max(max, r.value);
//         std::cout << "Maximum of values: " << max << "\n";
//     }
//     else if (command.rfind("max ", 0) == 0)
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         if (!validateGeneralRangeCommand("max ", command))
//         {
//             std::cout << "Invalid last command. Usage: max <start> <end>\n";
//             return;
//         }
//         std::istringstream iss(command);
//         std::string ignore;
//         int64_t number1, number2;
//
//         iss >> ignore >> number1 >> number2;
//
//         if (number1 > number2)
//         {
//             std::cout << "Invalid time range: start time is greater than end time.\n";
//             return;
//         }
//
//         std::vector<Record> records = (*storage).readRange(number1, number2);
//         if (records.empty()) {
//             std::cout << "No record found\n";
//             return;
//         }
//         double max = double(records.front().value);
//         for (const Record& r: records) max = std::max(max, r.value);
//         std::cout << "Maximum of values: " << max << "\n";
//     }
//     else if (command == "avg")
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         std::vector<Record> records = (*storage).readAll();
//         if (records.empty()) {
//             std::cout << "No record found\n";
//             return;
//         }
//         double sum = 0;
//         for (const Record& r: records) sum += r.value;
//         std::cout << "Average of values: " << sum/records.size() << "\n";
//     }
//     else if (command.rfind("avg ", 0) == 0)
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         if (!validateGeneralRangeCommand("avg ", command))
//         {
//             std::cout << "Invalid last command. Usage: avg <start> <end>\n";
//             return;
//         }
//         std::istringstream iss(command);
//         std::string ignore;
//         int64_t number1, number2;
//
//         iss >> ignore >> number1 >> number2;
//
//         if (number1 > number2)
//         {
//             std::cout << "Invalid time range: start time is greater than end time.\n";
//             return;
//         }
//
//         std::vector<Record> records = (*storage).readRange(number1, number2);
//         if (records.empty()) {
//             std::cout << "No record found\n";
//             return;
//         }
//         double sum = 0;
//         for (const Record& r: records) sum += r.value;
//         std::cout << "Average of values: " << sum/records.size() << "\n";
//     }
//     else if (command == "median")
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         std::vector<Record> records = (*storage).readAll();
//         if (records.empty()) {
//             std::cout << "No record found\n";
//             return;
//         }
//         std::sort(records.begin(), records.end(), [](const Record& a, const Record& b) {
//                   return a.value < b.value;
//               });
//         size_t i = (records.size()-1)/2;
//         std::cout << "Median of values: " << records[i].value << "\n";
//     }
//     else if (command.rfind("median ", 0) == 0)
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         if (!validateGeneralRangeCommand("median ", command))
//         {
//             std::cout << "Invalid last command. Usage: median <start> <end>\n";
//             return;
//         }
//         std::istringstream iss(command);
//         std::string ignore;
//         int64_t number1, number2;
//
//         iss >> ignore >> number1 >> number2;
//
//         if (number1 > number2)
//         {
//             std::cout << "Invalid time range: start time is greater than end time.\n";
//             return;
//         }
//
//         std::vector<Record> records = (*storage).readRange(number1, number2);
//         if (records.empty()) {
//             std::cout << "No record found\n";
//             return;
//         }
//         std::sort(records.begin(), records.end(), [](const Record& a, const Record& b) {
//                   return a.value < b.value;
//               });
//         size_t index = (records.size()-1)/2;
//         std::cout << "Median of values: " << records[index].value << "\n";
//     }
//     else if (command.rfind("percentile ", 0) == 0)
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         if (validatePercentileCommand(command))
//         {
//             const std::string prefix = "percentile ";
//             std::string remainder = command.substr(prefix.size());
//
//             std::istringstream iss(remainder);
//
//             int p;
//             iss >> p;
//             if (p < 0 || p > 100)
//             {
//                 std::cout << "Percentile value must be between 0 and 100.\n";
//                 return;
//             }
//             std::vector<Record> records = (*storage).readAll();
//             if (records.empty())
//             {
//                 std::cout << "No records founds.\n";
//                 return;
//             }
//             std::sort(records.begin(), records.end(), [](const Record& a, const Record& b) {
//                   return a.value < b.value;
//               });
//             size_t index = (records.size()-1) * p / 100;
//             std::cout << p << "th Percentile of values: " << records[index].value << "\n";
//         }
//         else if (validatePercentileRangeCommand(command))
//         {
//             const std::string prefix = "percentile ";
//             std::string remainder = command.substr(prefix.size());
//
//             std::istringstream iss(remainder);
//
//             int p;
//             int64_t startTs, endTs;
//             iss >> p >> startTs >> endTs;
//             if (p < 0 || p > 100)
//             {
//                 std::cout << "Percentile value must be between 0 and 100.\n";
//                 return;
//             }
//             std::vector<Record> records = (*storage).readRange(startTs, endTs);
//             if (records.empty())
//             {
//                 std::cout << "No records founds.\n";
//                 return;
//             }
//             std::sort(records.begin(), records.end(), [](const Record& a, const Record& b) {
//                   return a.value < b.value;
//               });
//             size_t index = (records.size()-1) * p / 100;
//             std::cout << p << "th Percentile of values: " << records[index].value << "\n";
//         }
//         else
//         {
//             std::cout << "Invalid percentile command. Usage: percentile <p> OR percentile <p> <start> <end>\n";
//             return;
//         }
//     }
//     else if (command == "stddev")
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         std::vector<Record> records = (*storage).readAll();
//         if (records.empty())
//         {
//             std::cout << "No record found\n";
//             return;
//         }
//         double sum = 0;
//         double sqrSum = 0;
//         for (Record& record: records)
//         {
//             sum += record.value;
//             sqrSum += record.value * record.value;
//         }
//         double exp = sum / records.size();
//         double expSqr = sqrSum / records.size();
//         double variance = expSqr - (exp * exp);
//         double stddev = std::sqrt(variance);
//         std::cout << "Standard Deviation of values: " << stddev << "\n";
//     }
//     else if (command.rfind("stddev ", 0) == 0)
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         if (!validateGeneralRangeCommand("stddev ", command))
//         {
//             std::cout << "Invalid last command. Usage: stddev <start> <end>\n";
//             return;
//         }
//         std::istringstream iss(command);
//         std::string ignore;
//         int64_t number1, number2;
//
//         iss >> ignore >> number1 >> number2;
//
//         if (number1 > number2)
//         {
//             std::cout << "Invalid time range: start time is greater than end time.\n";
//             return;
//         }
//
//         std::vector<Record> records = (*storage).readRange(number1, number2);
//         if (records.empty())
//         {
//             std::cout << "No record found\n";
//             return;
//         }
//         double sum = 0;
//         double sqrSum = 0;
//         for (Record& record: records)
//         {
//             sum += record.value;
//             sqrSum += record.value * record.value;
//         }
//         double exp = sum / records.size();
//         double expSqr = sqrSum / records.size();
//         double variance = expSqr - (exp * exp);
//         double stddev = std::sqrt(variance);
//         std::cout << "Standard Deviation of values: " << stddev << "\n";
//     }
//     else if (command == "variance")
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         std::vector<Record> records = (*storage).readAll();
//         if (records.empty())
//         {
//             std::cout << "No record found\n";
//             return;
//         }
//         double sum = 0;
//         double sqrSum = 0;
//         for (Record& record: records)
//         {
//             sum += record.value;
//             sqrSum += record.value * record.value;
//         }
//         double exp = sum / records.size();
//         double expSqr = sqrSum / records.size();
//         double variance = expSqr - (exp * exp);
//         std::cout << "Variance of values: " << variance << "\n";
//     }
//     else if (command.rfind("variance ", 0) == 0)
//     {
//         if (!storage)
//         {
//             std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
//             return;
//         }
//         if (!validateGeneralRangeCommand("variance ", command))
//         {
//             std::cout << "Invalid last command. Usage: variance <start> <end>\n";
//             return;
//         }
//         std::istringstream iss(command);
//         std::string ignore;
//         int64_t number1, number2;
//
//         iss >> ignore >> number1 >> number2;
//
//         if (number1 > number2)
//         {
//             std::cout << "Invalid time range: start time is greater than end time.\n";
//             return;
//         }
//
//         std::vector<Record> records = (*storage).readRange(number1, number2);
//         if (records.empty())
//         {
//             std::cout << "No record found\n";
//             return;
//         }
//         double sum = 0;
//         double sqrSum = 0;
//         for (Record& record: records)
//         {
//             sum += record.value;
//             sqrSum += record.value * record.value;
//         }
//         double exp = sum / records.size();
//         double expSqr = sqrSum / records.size();
//         double variance = expSqr - (exp * exp);
//         std::cout << "Variance of values: " << variance << "\n";
//     }
//     else
//     {
//         std::cout << "Unknown command: " << command << "\n";
//     }
// }

void TSDBCLI::performance()
{
        std::cout << "Entering performance metric mode...\n";
        std::string db = "performance.tsdb";

        if (std::filesystem::exists(db))
        {
            std::cout << "Database already exists\n";
            return;
        }

        if (storage)
        {
            storage.reset();
        }
        storage = std::make_unique<Storage>(db);

        std::cout << "Performance metric mode activated. Starting performance tests...\n";

        const int producerCount = 4;
        const int recordsPerProducer = 1'000'000 / producerCount;

        std::vector<std::thread> producers;
        std::vector<long long> appendTimes;
        std::vector<int64_t> timestamps;
        long long totalAppendTime = 0;
        std::mutex mtx;

        for (int p = 0; p < producerCount; ++p)
        {
            producers.emplace_back([&, p]() {
                std::vector<long long> local_times;
                std::vector<int64_t> local_timestamps;
                local_times.reserve(recordsPerProducer);
                long long local_total = 0;

                for (int i = 0; i < recordsPerProducer; ++i)
                {
                    Record r;
                    r.timestamp = p * 1'000'000 + i;
                    r.value = static_cast<double>(i);

                    auto start = std::chrono::high_resolution_clock::now();
                    bool accepted = (*storage).append(r);
                    auto end = std::chrono::high_resolution_clock::now();

                    long long duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                    local_times.push_back(duration_ns);
                    if (accepted) local_timestamps.push_back(r.timestamp);
                    local_total += duration_ns;
                }

                {
                    std::lock_guard<std::mutex> lock(mtx);
                    appendTimes.insert(appendTimes.end(), local_times.begin(), local_times.end());
                    timestamps.insert(timestamps.end(), local_timestamps.begin(), local_timestamps.end());
                    totalAppendTime += local_total;
                }
            });
        }

        for (auto& t : producers)
        {
            t.join();
        }

        std::cout << "Average append time: " << (totalAppendTime / appendTimes.size()) << " ns\n";

        std::sort(appendTimes.begin(), appendTimes.end());
        long long p99 = appendTimes[appendTimes.size() * 99 / 100];
        long long p95 = appendTimes[appendTimes.size() * 95 / 100];
        long long p50 = appendTimes[appendTimes.size() / 2];

        std::cout << "p50 append time: " << p50 << " ns\n";
        std::cout << "p95 append time: " << p95 << " ns\n";
        std::cout << "p99 append time: " << p99 << " ns\n";

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        std::vector<long long> readFromTimes;
        long long totalReadFromTime = 0;

        for (int i=0; i<timestamps.size(); i+=100)
        {
            int64_t ts = timestamps[i];
            auto start = std::chrono::high_resolution_clock::now();
            (*storage).readFromTime(ts);
            auto end = std::chrono::high_resolution_clock::now();

            long long duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            readFromTimes.push_back(duration_ns);
            totalReadFromTime += duration_ns;
        }

        std::cout << "\nAverage read from time: " << static_cast<double>((totalReadFromTime / readFromTimes.size()))/1000 << " ms\n";

        std::sort(readFromTimes.begin(), readFromTimes.end());
        p99 = readFromTimes[readFromTimes.size() * 99 / 100];
        p95 = readFromTimes[readFromTimes.size() * 95 / 100];
        p50 = readFromTimes[readFromTimes.size() / 2];

        std::cout << "p50 read from time: " << static_cast<double>(p50)/1000 << " ms\n";
        std::cout << "p95 read from time: " << static_cast<double>(p95)/1000 << " ms\n";
        std::cout << "p99 read from time: " << static_cast<double>(p99)/1000 << " ms\n";

        storage.reset();
        std::filesystem::remove(db);
        std::cout << "Performance metric mode exited. Database deleted.\n";
    }

void TSDBCLI::create(std::string name)
{
    if (name == "performance")
    {
        std::cout << "The database name 'performance' is reserved for performance metric mode. Please choose a different name.\n";
        return;
    }

    if (!validateFileName(name))
    {
        std::cout << "The database name must consist of A-Z, a-z or 0-9 characters only. Please choose a different name.\n";
        return;
    }

    name += ".tsdb";

    if (std::filesystem::exists(name))
    {
        std::cout << "Database already exists\n";
        return;
    }
    if (storage)
    {
        storage.reset();
    }
    storage = std::make_unique<Storage>(name);
}

void TSDBCLI::use(std::string name)
{
    if (!validateFileName(name))
    {
        std::cout << "The database name must consist of A-Z, a-z or 0-9 characters only. Please choose a different name.\n";
        return;
    }

    name += ".tsdb";

    if (!std::filesystem::exists(name))
    {
        std::cout << "Database doesn't exist\n";
        return;
    }
    if (storage)
    {
        storage.reset();
    }
    storage = std::make_unique<Storage>(name);
}

bool TSDBCLI::validateFileName(const std::string& name)
{
    if (name.empty()) return false;
    for (char c : name)
    {
        if (!std::isalnum(c))
        {
            return false;
        }
    }
    return true;
}

void TSDBCLI::readall()
{
    if (!storage)
    {
        std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
        return;
    }
    std::vector<Record> records = (*storage).readAll();
    for (Record& record: records)
    {
        std::cout << "Timestamp: " << record.timestamp << ", Value: " << record.value << "\n";
    }
}

void TSDBCLI::readfrom(int64_t timestamp)
{
    if (!storage)
    {
        std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
        return;
    }
    if (timestamp < 0)
    {
        std::cout << "Timestamp cannot be negative.\n";
    }
    std::optional<Record> record = (*storage).readFromTime(timestamp);
    if (record.has_value())
    {
        std::cout << "Timestamp: " << record.value().timestamp << ", Value: " << record.value().value << "\n";
    }
    else
    {
        std::cout << "No record found\n";
    }
}

void TSDBCLI::readrange(int64_t start, int64_t end)
{
    if (!storage)
    {
        std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
        return;
    }

    if (start < 0)
    {
        std::cout << "Timestamps cannot be negative.\n";
        return;
    }
    if (start > end)
    {
        std::cout << "Invalid range. Start timestamp cannot come after end timestamp.\n";
    }
    std::vector<Record> records = (*storage).readRange(start, end);
    for (Record& record: records)
    {
        std::cout << "Timestamp: " << record.timestamp << ", Value: " << record.value << "\n";
    }
}

void TSDBCLI::append(int64_t timestamp, double value)
{
    if (!storage)
    {
        std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
        return;
    }
    bool success = (*storage).append(Record{timestamp, value});

    if (success) std::cout << "Record accepted, pending persistence\n";
    else std::cout << "Failed to accept record.\n";
}