#pragma once
#include "Storage.hpp"

class TSDBCLI
{
public:
    explicit TSDBCLI();
    void performance();
    void create(std::string name);
    void use(std::string name);
    void readall();
    void readfrom(int64_t timestamp);
    void readrange(int64_t start, int64_t end);
    void append(int64_t timestamp, double value);
    void count(int64_t start = 0, int64_t end = std::numeric_limits<int64_t>::max());
    void first(int64_t start = 0, int64_t end = std::numeric_limits<int64_t>::max());
    void last(int64_t start = 0, int64_t end = std::numeric_limits<int64_t>::max());
    void sum(int64_t start = 0, int64_t end = std::numeric_limits<int64_t>::max());
    void min(int64_t start = 0, int64_t end = std::numeric_limits<int64_t>::max());
    void max(int64_t start = 0, int64_t end = std::numeric_limits<int64_t>::max());
    void avg(int64_t start = 0, int64_t end = std::numeric_limits<int64_t>::max());
    void median(int64_t start = 0, int64_t end = std::numeric_limits<int64_t>::max());
    void percentile(int64_t start = 0, int64_t end = std::numeric_limits<int64_t>::max(), size_t p = 95);
    void stddev(int64_t start = 0, int64_t end = std::numeric_limits<int64_t>::max());
    void variance(int64_t start = 0, int64_t end = std::numeric_limits<int64_t>::max());

private:
    std::unique_ptr<Storage> storage;

    bool validateRange(int64_t start, int64_t end);
    bool validateFileName(const std::string& name);
    float calculateSum(int64_t start, int64_t end);
    float calculateVariance(int64_t start, int64_t end);
};