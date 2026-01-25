#include "TSDBCLI.hpp"
#include <iostream>
#include <sstream>
#include <filesystem>
#include <algorithm>

TSDBCLI::TSDBCLI() : storage(nullptr)
{
}

void TSDBCLI::run()
{
    printHelp();
    std::string command;
    while (true) {
        std::cout << "tsdb> ";
        std::getline(std::cin, command);
        if (command == "exit" || command == "quit") {
            break;
        }
        handleCommand(command);
    }
}

void TSDBCLI::printHelp() const
{
    std::cout << "TSDB Command Line Interface\n";
    std::cout << "===========================\n\n";

    std::cout << "General Commands:\n";
    std::cout << "  help                          Show this help message\n";
    std::cout << "  performance                   Enter performance metric mode\n";
    std::cout << "  exit | quit                   Exit the CLI\n\n";

    std::cout << "Database Commands:\n";
    std::cout << "  create <database>             Create a new database\n";
    std::cout << "  use <database>                Switch to an existing database\n\n";

    std::cout << "Data Ingestion:\n";
    std::cout << "  append <timestamp> <value>    Append a new data point\n\n";

    std::cout << "Read Queries:\n";
    std::cout << "  readall                       Read and display all records\n";
    std::cout << "  readfrom <timestamp>          Read records from a timestamp\n";
    std::cout << "  readrange <start> <end>       Read records in a time range\n\n";

    std::cout << "Aggregate Functions:\n";
    std::cout << "  count <start> <end>           Count records\n";
    std::cout << "  first <start> <end>           First values\n";
    std::cout << "  last <start> <end>            Last value\n";
    std::cout << "  sum <start> <end>             Sum of values\n";
    std::cout << "  min <start> <end>             Minimum value\n";
    std::cout << "  max <start> <end>             Maximum value\n";
    std::cout << "  avg <start> <end>             Average value\n";
    std::cout << "  median <start> <end>          Median value\n";
    std::cout << "  percentile <p> <start> <end>  Pth percentile (0–100)\n";
    std::cout << "  stddev <start> <end>          Standard deviation\n";
    std::cout << "  variance <start> <end>        Variance\n\n";

    std::cout << "  Note: if <start> <end> are omitted, the full time series is used\n\n";

}

void TSDBCLI::handleCommand(const std::string& command)
{
    if (command == "help")
    {
        printHelp();
    }
    else if (command == "performance")
    {
        std::cout << "Entering performance metric mode...\n";
        std::string db = "performance.tsdb";

        if (std::filesystem::exists(db))
        {
            std::cout << "Database already exists\n"; //TODO: protect this database name
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
    else if (command.rfind("create ", 0) == 0)
    {
        if (!validateCreateCommand(command))
        {
            std::cout << "Invalid create command. Usage: create <database> where <database> contains letters and numbers only\n";
            return;
        }

        if (command == "create performance")
        {
            std::cout << "The database name 'performance' is reserved for performance metric mode. Please choose a different name.\n";
            return;
        }

        std::istringstream iss(command);
        std::string ignore;
        std::string db;

        iss >> ignore >> db;

        db += ".tsdb";

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
    }
    else if (command.rfind("use ", 0) == 0)
    {
        if (!validateUseCommand(command))
        {
            std::cout << "Invalid use command. Usage: use <database> where <database> contains letters and numbers only\n";
            return;
        }
        std::istringstream iss(command);
        std::string ignore;
        std::string db;

        iss >> ignore >> db;

        db += ".tsdb";

        if (!std::filesystem::exists(db))
        {
            std::cout << "Database not recognised\n";
            return;
        }
        if (storage)
        {
            storage.reset();
        }
        storage = std::make_unique<Storage>(db);
    }
    else if (command == "readall")
    {
        if (!storage)
        {
            std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
            return;
        }
        std::vector<Record> records = (*storage).readAll();
        for (const Record& r : records)
        {
            std::cout << "Timestamp: " << r.timestamp << ", Value: " << r.value << "\n";
        }
    }
    else if (command.rfind("readfrom ", 0) == 0)
    {
        if (!storage)
        {
            std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
            return;
        }
        if (!validateReadFromCommand(command))
        {
            std::cout << "Invalid readfrom command. Usage: readfrom <timestamp>\n";
            return;
        }
        std::istringstream iss(command);
        std::string ignore;
        int64_t number1;

        iss >> ignore >> number1;

        std::optional<Record> record = (*storage).readFromTime(number1);

        if (record.has_value())
        {
            std::cout << "Timestamp: " << record.value().timestamp << ", Value: " << record.value().value << "\n";
        }
        else
        {
            std::cout << "No record found\n";
        }
    }
    else if (command.rfind("readrange ", 0) == 0)
    {
        if (!storage)
        {
            std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
            return;
        }
        if (!validateGeneralRangeCommand("readrange ", command))
        {
            std::cout << "Invalid readrange command. Usage: readrange <start> <end>\n";
            return;
        }
        std::istringstream iss(command);
        std::string ignore;
        int64_t number1, number2;

        iss >> ignore >> number1 >> number2;

        if (number1 > number2)
        {
            std::cout << "Invalid time range: start time is greater than end time.\n";
            return;
        }

        std::vector<Record> records = (*storage).readRange(number1, number2);
        if (records.empty())
        {
            std::cout << "No record found\n";
        }
        else
        {
            for (const Record& r : records)
            {
                std::cout << "Timestamp: " << r.timestamp << ", Value: " << r.value << "\n";
            }
        }
    }
    else if (command.rfind("append ", 0) == 0)
    {
        if (!storage)
        {
            std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
            return;
        }
        if (!validateAppendCommand(command))
        {
            std::cout << "Invalid append command. Usage: append <timestamp> <value>\n";
            return;
        }
        std::istringstream iss(command);
        std::string ignore;
        int64_t timestamp;;
        double value;

        iss >> ignore >> timestamp >> value;

        bool success = (*storage).append(Record{timestamp, value});

        if (success) std::cout << "Record accepted, pending persistence\n";
        else std::cout << "Failed to accept record.\n";
    }
    else if (command == "count")
    {
        if (!storage)
        {
            std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
            return;
        }
        size_t count = (*storage).readAll().size();
        std::cout << "Total records: " << count << "\n";
    }
    else if (command.rfind("count ", 0) == 0)
    {
        if (!storage)
        {
            std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
            return;
        }
        if (!validateGeneralRangeCommand("count ", command))
        {
            std::cout << "Invalid count command. Usage: count <start> <end>\n";
            return;
        }
        std::istringstream iss(command);
        std::string ignore;
        int64_t number1, number2;

        iss >> ignore >> number1 >> number2;

        if (number1 > number2)
        {
            std::cout << "Invalid time range: start time is greater than end time.\n";
            return;
        }

        size_t count = (*storage).readRange(number1, number2).size();
        std::cout << "Total records: " << count << "\n";
    }
    else if (command == "first")
    {
        if (!storage)
        {
            std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
            return;
        }
        std::vector<Record> records = (*storage).readAll();
        if (records.empty())
        {
            std::cout << "No record found\n";
        }
        else
        {
            std::cout << "Timestamp: " << records.front().timestamp << ", Value: " << records.front().value << "\n";
        }
    }
    else if (command.rfind("first ", 0) == 0)
    {
        if (!storage)
        {
            std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
            return;
        }
        if (!validateGeneralRangeCommand("first ", command))
        {
            std::cout << "Invalid first command. Usage: first <start> <end>\n";
            return;
        }
        std::istringstream iss(command);
        std::string ignore;
        int64_t number1, number2;

        iss >> ignore >> number1 >> number2;

        if (number1 > number2)
        {
            std::cout << "Invalid time range: start time is greater than end time.\n";
            return;
        }

        std::vector<Record> records = (*storage).readRange(number1, number2);
        if (records.empty())
        {
            std::cout << "No record found\n";
        }
        else
        {
            std::cout << "Timestamp: " << records.front().timestamp << ", Value: " << records.front().value << "\n";
        }
    }
    else
    {
        std::cout << "Unknown command: " << command << "\n";
    }
}

bool TSDBCLI::validateCreateCommand(const std::string& command)
{
    const std::string prefix = "create ";
    if (command.rfind(prefix, 0) != 0) {
        return false;
    }
    std::string remainder = command.substr(7);
    if (remainder.empty()) {
        return false;
    }
    for (char c: remainder) {
        if (!(int(c) >= 48 && int(c) <= 57) && !(int(c) >= 65 && int(c) <= 90) && !(int(c) >= 97 && int(c) <= 122)){
            return false;
        }
    }
    return true;
}

bool TSDBCLI::validateUseCommand(const std::string& command)
{
    const std::string prefix = "use ";
    if (command.rfind(prefix, 0) != 0) {
        return false;
    }
    std::string remainder = command.substr(4);
    if (remainder.empty()) {
        return false;
    }
    for (char c: remainder) {
        if (!(int(c) >= 48 && int(c) <= 57) && !(int(c) >= 65 && int(c) <= 90) && !(int(c) >= 97 && int(c) <= 122)){
            return false;
        }
    }
    return true;
}

bool TSDBCLI::validateReadFromCommand(const std::string& command)
{
    const std::string prefix = "readfrom ";
    if (command.rfind(prefix, 0) != 0) {
        return false;
    }
    std::string remainder = command.substr(prefix.size());
    if (remainder.empty()) {
        return false;
    }

    std::istringstream iss(remainder);

    int64_t number1;
    std::string extra;
    if (!(iss >> number1)) {
        return false;
    }
    if (iss >> extra) {
        return false;
    }
    return true;
}

bool TSDBCLI::validateAppendCommand(const std::string& command)
{
    const std::string prefix = "append ";
    if (command.rfind(prefix, 0) != 0) {
        return false;
    }
    std::string remainder = command.substr(prefix.size());
    if (remainder.empty()) {
        return false;
    }

    std::istringstream iss(remainder);

    int64_t timestamp;
    double value;

    std::string extra;
    if (!(iss >> timestamp >> value)) {
        return false;
    }
    if (iss >> extra) {
        return false;
    }
    return true;
}

bool TSDBCLI::validateGeneralRangeCommand(std::string prefix, const std::string& command)
{
    if (command.rfind(prefix, 0) != 0) {
        return false;
    }
    std::string remainder = command.substr(prefix.size());
    if (remainder.empty()) {
        return false;
    }

    std::istringstream iss(remainder);

    int64_t number1, number2;
    std::string extra;
    if (!(iss >> number1 >> number2)) {
        return false;
    }
    if (iss >> extra) {
        return false;
    }
    return true;
}