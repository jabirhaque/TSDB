#pragma once
#include <string>
#include "Record.hpp"
#include <vector>

class Storage
{
public:
    explicit Storage(const std::string& filename);

    bool append(const Record& r);
    std::vector<Record> readAll() const;
    int64_t getLastTimestamp();

private:
    std::string filename;
    int64_t lastTimestamp;
};
