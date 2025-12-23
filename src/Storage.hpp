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

    bool append(Record r);
    std::vector<Record> readAll() const;
    int64_t getLastTimestamp() const;
    TSDBHeader getHeader() const;
    std::optional<Record> getLastRecord() const;
    Record getRecord(size_t index) const;
    std::vector<Record> readRange(int64_t startTs, int64_t endTs) const;

private:
    std::string filename;
    int64_t lastTimestamp;
    TSDBHeader header;
};
