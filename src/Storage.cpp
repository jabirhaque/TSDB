#include "Storage.hpp"
#include <fstream>
#include <sstream>
#include <limits>
#include <cstdint>
#include <optional>



Storage::Storage(const std::string& filename) : filename(filename)
{
    lastTimestamp = std::numeric_limits<int64_t>::min();

    std::ifstream inFile(filename, std::ios::binary);
    if (inFile.is_open()) {

        inFile.seekg(0, std::ios::end);
        std::streampos fileSize = inFile.tellg();

        if (fileSize < static_cast<std::streampos>(sizeof(TSDBHeader))) {
            throw std::runtime_error("File too small to contain valid TSDB header: " + filename);
        }

        TSDBHeader temporaryHeader;
        inFile.seekg(0, std::ios::beg);

        if (!inFile.read(reinterpret_cast<char*>(&temporaryHeader), sizeof(TSDBHeader))) {
            throw std::runtime_error("Failed to read TSDB header: " + filename);
        }

        if (temporaryHeader.magic[0] != 'T' || temporaryHeader.magic[1] != 'S' ||
            temporaryHeader.magic[2] != 'D' || temporaryHeader.magic[3] != 'B') {
            throw std::runtime_error("Invalid TSDB file magic number: " + filename);
        }

        if (temporaryHeader.version != 1) {
            throw std::runtime_error("Unsupported TSDB file version: " + filename);
        }

        if (temporaryHeader.recordSize != sizeof(Record)) {
            throw std::runtime_error("Record size mismatch: " + filename);
        }

        header = temporaryHeader;

        std::streampos dataSize = fileSize - static_cast<std::streampos>(sizeof(TSDBHeader));

        if (dataSize % static_cast<std::streampos>(sizeof(Record)) != 0) {
            throw std::runtime_error("Corrupted TSDB file: misaligned record section");
        }
    }
    else
    {
        header = {'T', 'S', 'D', 'B', 1, {0, 0, 0}, static_cast<uint16_t>(sizeof(Record))};
        std::ofstream outFile(filename, std::ios::binary | std::ios::app);
        if (!outFile.is_open()) {
            throw std::runtime_error("Failed to open file for writing: " + filename);
        }
        outFile.write(reinterpret_cast<const char*>(&header), sizeof(TSDBHeader));
        outFile.close();
    }
    std::optional<Record> lastRecord = getLastRecord();
    if (lastRecord.has_value())
    {
        lastTimestamp = lastRecord->timestamp;
    }
}

int64_t Storage::getLastTimestamp() const
{
    return lastTimestamp;
}

TSDBHeader Storage::getHeader() const
{
    return header;
}

bool Storage::append(const Record& r)
{
    if (r.timestamp <= lastTimestamp) return false;

    std::ofstream outFile(filename, std::ios::binary | std::ios::app);
    if (!outFile.is_open()) {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }

    outFile.write(reinterpret_cast<const char*>(&r), sizeof(Record));

    outFile.close();

    lastTimestamp = r.timestamp;
    return true;
}


std::vector<Record> Storage::readAll() const {
    std::ifstream inFile(filename, std::ios::binary);
    if (!inFile.is_open()) {
        throw std::runtime_error("Failed to open file for reading: " + filename);
    }

    inFile.seekg(0, std::ios::end);
    std::streampos fileSize = inFile.tellg();
    inFile.seekg(static_cast<std::streampos>(sizeof(TSDBHeader)), std::ios::beg);

    std::streampos dataSize = fileSize - static_cast<std::streampos>(sizeof(TSDBHeader));

    std::vector<Record> records;
    if (dataSize == 0) return records;

    if (dataSize % sizeof(Record) != 0) {
        throw std::runtime_error("Corrupted TSDB file: misaligned record section");
    }

    size_t numRecords = dataSize / sizeof(Record);
    records.resize(numRecords);

    if (!inFile.read(reinterpret_cast<char*>(records.data()), dataSize)) {;
        throw std::runtime_error("Failed to read records from file: " + filename);
    }

    inFile.close();
    return records;
}

std::optional<Record> Storage::getLastRecord() const
{
    std::ifstream inFile(filename, std::ios::binary);
    if (!inFile.is_open()) throw std::runtime_error("Failed to open file: " + filename);

    inFile.seekg(0, std::ios::end);
    std::streampos fileSize = inFile.tellg();

    std::streampos dataSize = fileSize - static_cast<std::streampos>(sizeof(TSDBHeader));
    if (dataSize == 0) return std::nullopt;
    inFile.seekg(-static_cast<std::streamoff>(sizeof(Record)), std::ios::end);

    Record last;
    if (!inFile.read(reinterpret_cast<char*>(&last), sizeof(Record))) {
        throw std::runtime_error("Failed to read last record: " + filename);
    }

    return last;
}

Record Storage::getRecord(size_t index) const
{
    std::ifstream inFile(filename, std::ios::binary);
    if (!inFile.is_open()) throw std::runtime_error("Failed to open file: " + filename);

    inFile.seekg(0, std::ios::end);
    std::streampos fileSize = inFile.tellg();
    std::streampos dataSize = fileSize - static_cast<std::streampos>(sizeof(TSDBHeader));
    size_t numRecords = dataSize / sizeof(Record);

    if (index >= static_cast<int>(numRecords)) throw std::out_of_range("Record index out of range");
    inFile.seekg(static_cast<std::streamoff>(sizeof(TSDBHeader)) + static_cast<std::streamoff>(index*sizeof(Record)), std::ios::beg);

    Record record;
    if (!inFile.read(reinterpret_cast<char*>(&record), sizeof(Record))) {
        throw std::runtime_error("Failed to read record: " + filename);
    }
    return record;
}

std::vector<Record> Storage::readRange(int64_t startTs, int64_t endTs) const
{
    if (startTs > endTs) throw std::runtime_error("Invalid time range");

    std::ifstream inFile(filename, std::ios::binary);
    if (!inFile.is_open()) throw std::runtime_error("Failed to open file: " + filename);

    inFile.seekg(0, std::ios::end);
    std::streampos fileSize = inFile.tellg();
    std::streampos dataSize = fileSize - static_cast<std::streampos>(sizeof(TSDBHeader));
    size_t numRecords = dataSize / sizeof(Record);

    std::optional<size_t> startIndex = lowestIndexGreaterThanTimeStamp(startTs, numRecords);
    if (!startIndex.has_value()) return {};
    std::optional<size_t> endIndex = greatestIndexLessThanTimeStamp(endTs, numRecords);
    if (!endIndex.has_value()) return {};

    if (startIndex.value()>endIndex.value()) return {};

    size_t recordCount = endIndex.value()-startIndex.value()+1;
    std::vector<Record> records(recordCount);

    inFile.seekg(static_cast<std::streampos>(sizeof(TSDBHeader))+static_cast<std::streampos>(startIndex.value())*static_cast<std::streampos>(sizeof(Record)), std::ios::beg);

    if (!inFile.read(reinterpret_cast<char*>(records.data()), recordCount*static_cast<std::streampos>(sizeof(Record)))) {;
        throw std::runtime_error("Failed to read records from file: " + filename);
    }

    inFile.close();
    return records;
}

std::optional<size_t> Storage::lowestIndexGreaterThanTimeStamp(int64_t timestamp, size_t numRecords) const
{
    if (numRecords == 0) return std::nullopt;

    size_t left = 0;
    size_t right = numRecords - 1;
    std::optional<size_t> result;

    while (left <= right) {
        size_t mid = left + (right - left) / 2;
        Record midRecord = getRecord(mid);

        if (midRecord.timestamp >= timestamp) {
            result = mid;
            if (mid == 0) break;
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    return result;
}


std::optional<size_t> Storage::greatestIndexLessThanTimeStamp(int64_t timestamp, size_t numRecords) const
{
    if (numRecords == 0) return std::nullopt;

    size_t left = 0;
    size_t right = numRecords - 1;
    std::optional<size_t> result;

    while (left <= right) {
        size_t mid = left + (right - left) / 2;
        Record midRecord = getRecord(mid);

        if (midRecord.timestamp <= timestamp) {
            result = mid;
            left = mid + 1;
        } else {
            if (mid == 0) break;
            right = mid - 1;
        }
    }

    return result;
}