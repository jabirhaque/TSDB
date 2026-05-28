#include "TSDBCLI.hpp"
#include <iostream>
#include <filesystem>
#include <algorithm>

TSDBCLI::TSDBCLI() : storage(nullptr){}

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
    if (!validateRange(start, end)) return;
    std::vector<Record> records = (*storage).readRange(start, end);
    for (Record& record: records)
    {
        std::cout << "Timestamp: " << record.timestamp << ", Value: " << record.value << "\n";
    }
}

void TSDBCLI::count(int64_t start, int64_t end)
{
    if (!storage)
    {
        std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
        return;
    }
    if (!validateRange(start, end)) return;

    size_t count = (*storage).readRange(start, end).size();
    std::cout << "Total records: " << count << "\n";
}

void TSDBCLI::first(int64_t start, int64_t end)
{
    if (!storage)
    {
        std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
        return;
    }
    if (!validateRange(start, end)) return;

    Record record = (*storage).readRange(start, end).front();
    std::cout << "Timestamp: " << record.timestamp << ", Value: " << record.value << "\n";
}

void TSDBCLI::last(int64_t start, int64_t end)
{
    if (!storage)
    {
        std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
        return;
    }
    if (!validateRange(start, end)) return;

    Record record = (*storage).readRange(start, end).back();
    std::cout << "Timestamp: " << record.timestamp << ", Value: " << record.value << "\n";
}

float TSDBCLI::sum(int64_t start, int64_t end)
{
    if (!storage)
    {
        std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
        return 0;
    }
    if (!validateRange(start, end)) return 0;

    std::vector<Record> records = (*storage).readRange(start, end);
    double sum = 0;
    for (Record& record: records)
    {
        sum += record.value;
    }
    std::cout << "Sum: " << sum << "\n";
    return sum;
}

void TSDBCLI::min(int64_t start, int64_t end)
{
    if (!storage)
    {
        std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
        return;
    }
    if (!validateRange(start, end)) return;

    std::vector<Record> records = (*storage).readRange(start, end);
    size_t index = 0;
    for (size_t i=1; i<records.size(); i++)
    {
        if (records[i].value < records[index].value)
        {
            index = i;
        }
    }
    std::cout << "Min value: " << records[index].value << ", at timestamp: " << records[index].timestamp << "\n";
}

void TSDBCLI::max(int64_t start, int64_t end)
{
    if (!storage)
    {
        std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
        return;
    }
    if (!validateRange(start, end)) return;

    std::vector<Record> records = (*storage).readRange(start, end);
    size_t index = 0;
    for (size_t i=1; i<records.size(); i++)
    {
        if (records[i].value > records[index].value)
        {
            index = i;
        }
    }
    std::cout << "Max value: " << records[index].value << ", at timestamp: " << records[index].timestamp << "\n";
}

void TSDBCLI::avg(int64_t start, int64_t end)
{
    if (!storage)
    {
        std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
        return;
    }
    if (!validateRange(start, end)) return;

    size_t size = (*storage).readRange(start, end).size();
    float total = sum(start, end);
    std::cout << "Average: " << total/size << "\n";
}

void TSDBCLI::median(int64_t start, int64_t end)
{
    if (!storage)
    {
        std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
        return;
    }
    if (!validateRange(start, end)) return;

    std::vector<Record> records = (*storage).readRange(start, end);
    size_t index = (records.size()-1)/2;
    Record record = records[index];
    std::cout << "Timestamp: " << record.timestamp << ", Value: " << record.value << "\n";
}

void TSDBCLI::percentile(int64_t start, int64_t end, float p)
{
    if (!storage)
    {
        std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
        return;
    }
    if (!validateRange(start, end)) return;

    std::vector<Record> records = (*storage).readRange(start, end);
    size_t index = (records.size()-1) * p / 100;
    Record record = records[index];
    std::cout << "Timestamp: " << record.timestamp << ", Value: " << record.value << "\n";
}

void TSDBCLI::stddev(int64_t start, int64_t end)
{
    if (!storage)
    {
        std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
        return;
    }
    if (!validateRange(start, end)) return;

    float var = variance(start, end);
    std::cout << "Standard Deviation: " << std::sqrt(var) << "\n";
}

float TSDBCLI::variance(int64_t start, int64_t end)
{
    if (!storage)
    {
        std::cout << "No database selected. Use the 'use <database>' command to select a database.\n";
        return 0;
    }
    if (!validateRange(start, end)) return 0;

    std::vector<Record> records = (*storage).readRange(start, end);
    float avg = sum(start, end)/records.size();
    float expsqr = 0;
    for (Record& record: records)
    {
        expsqr += record.value * record.value;
    }
    expsqr /= records.size();
    float variance = expsqr - avg*avg;
    std::cout << "Variance: " << variance << "\n";
}

bool TSDBCLI::validateRange(int64_t start, int64_t end)
{
    if (start < 0)
    {
        std::cout << "Timestamps cannot be negative.\n";
        return false;
    }
    if (start > end)
    {
        std::cout << "Invalid range. Start timestamp cannot come after end timestamp.\n";
        return false;
    }
    return true;
}