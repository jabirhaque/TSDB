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
    int64_t getLastTimestamp() const;
    TSDBHeader getHeader() const;
    std::optional<Record> getLastRecord() const;
    Record getRecord(size_t index) const;
    std::vector<Record> readRange(int64_t startTs, int64_t endTs) const;
    std::optional<size_t> lowestIndexGreaterThanTimeStamp(int64_t timestamp, size_t numRecords) const;
    std::optional<size_t> greatestIndexLessThanTimeStamp(int64_t timestamp, size_t numRecords) const;

private:
    std::string filename;
    int64_t lastTimestamp;
    TSDBHeader header;
};
