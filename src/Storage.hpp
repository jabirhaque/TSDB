#pragma once
#include <string>
#include "Record.hpp"
#include "TSDBHeader.hpp"
#include <vector>
#include <optional>

class Storage
{
public:
    //constructor
    explicit Storage(const std::string& filename);

    //header functions
    TSDBHeader getHeader() const;

    //write functions
    bool append(Record r);

    //read functions
    std::vector<Record> readAll() const;
    std::vector<Record> readRange(int64_t startTs, int64_t endTs) const;
    std::optional<Record> getLastRecord() const;
    Record getRecord(size_t index) const;

    //helper functions
    void validateFile();
    uint32_t computeCRC(const Record& r) const;

    //getters
    int64_t getLastTimestamp() const;

private:
    std::string filename;
    int64_t lastTimestamp;
    TSDBHeader header;
    size_t sparseIndexStep;
};
