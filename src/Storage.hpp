#pragma once
#include <string>
#include "Record.hpp"
#include "TSDBHeader.hpp"
#include <vector>
#include <optional>

class Storage
{
public:
    explicit Storage(const std::string& filename);

    bool append(const Record& r);
    std::vector<Record> readAll() const;
    int64_t getLastTimestamp();
    TSDBHeader getHeader();
    std::optional<Record> getLastRecord() const;

private:
    std::string filename;
    int64_t lastTimestamp;
    TSDBHeader header;
};
