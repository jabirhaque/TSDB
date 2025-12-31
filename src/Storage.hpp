#pragma once
#include <string>
#include "Record.hpp"
#include "TSDBHeader.hpp"
#include "IndexEntry.hpp"
#include <vector>
#include <optional>
#include <thread>

class Storage
{
public:
    //constructor
    explicit Storage(const std::string& filename);

    //destructor
    ~Storage();

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
    //file info
    const std::string filename;
    TSDBHeader header;
    int64_t lastTimestamp;
    size_t recordCount;
    int fd;

    //sparse index
    const size_t sparseIndexStep{4};
    std::vector<IndexEntry> sparseIndex;

    //buffers
    std::vector<Record> activeBuffer;
    std::vector<Record> flushBuffer;

    //synchronisation
    std::atomic<bool> running{true};
    mutable std::mutex bufferMutex;
    std::thread flushThread;
    const std::chrono::milliseconds flushInterval{5};


    //private methods
    TSDBHeader validateAndReadHeader(std::ifstream& inFile);
    size_t recoverPartialWriteAndReturnRecordCount(std::ifstream& inFile);
    uint32_t computeCRC(const Record& r) const;
    void buildSparseIndex();
    void flushLoop();
    void flushBufferToDisk(const std::vector<Record>& buffer);
};