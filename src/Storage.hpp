#pragma once
#include <string>
#include "Record.hpp"
#include "TSDBHeader.hpp"
#include "IndexEntry.hpp"
#include <vector>
#include <optional>

class Storage
{
public:
    //constructor
    explicit Storage(const std::string& filename);

    //write functions
    bool append(Record r);

    //read functions
    std::vector<Record> readAll() const;
    std::vector<Record> readRange(int64_t startTs, int64_t endTs) const;
    std::optional<Record> getLastRecord() const;
    Record getRecord(size_t index) const;

    //getters
    int64_t getLastTimestamp() const;
    TSDBHeader getHeader() const;
    size_t getRecordCount() const;
    size_t getSparseIndexStep() const;
    const std::vector<IndexEntry>& getSparseIndex() const;

private:
    std::string filename;
    TSDBHeader header;
    int64_t lastTimestamp;
    size_t recordCount;

    size_t sparseIndexStep;
    std::vector<IndexEntry> sparseIndex;

    TSDBHeader validateAndReadHeader(std::ifstream& file);
    uint32_t computeCRC(const Record& r) const;
    void buildSparseIndex();
};
