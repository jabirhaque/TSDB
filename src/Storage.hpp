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

private:
    std::string filename;
    int64_t lastTimestamp;
};
