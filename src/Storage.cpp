#include "Storage.hpp"
#include <fstream>
#include <sstream>
#include <limits>
#include <cstdint>
#include <optional>
#include <zlib.h>



Storage::Storage(const std::string& filename) : filename(filename)
{
    sparseIndexStep = 4; //every 4th record
    lastTimestamp = std::numeric_limits<int64_t>::min();

    std::ifstream inFile(filename, std::ios::binary);
    if (inFile.is_open()) {
        header = validateAndReadHeader(inFile);
        inFile.seekg(0, std::ios::end);
        std::streampos dataSize = (inFile.tellg() - static_cast<std::streampos>(sizeof(TSDBHeader)))/static_cast<std::streampos>(sizeof(Record));
        recordCount = static_cast<size_t>(dataSize);
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
        recordCount = 0;
    }
    std::optional<Record> lastRecord = getLastRecord();
    if (lastRecord.has_value())
    {
        lastTimestamp = lastRecord->timestamp;
    }

    buildSparseIndex();
}

bool Storage::append(Record r)
{
    if (r.timestamp <= lastTimestamp) return false;

    r.crc = computeCRC(r);

    std::ofstream outFile(filename, std::ios::binary | std::ios::app);
    if (!outFile.is_open()) {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }

    outFile.write(reinterpret_cast<const char*>(&r), sizeof(Record));

    outFile.close();

    lastTimestamp = r.timestamp;
    if (recordCount % sparseIndexStep == 0)
    {
        IndexEntry indexEntry;
        indexEntry.timestamp = r.timestamp;
        indexEntry.recordIndex = recordCount;
        sparseIndex.push_back(indexEntry);
    }
    recordCount++;
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

    for (const Record& r: records)
    {
        uint32_t expected = computeCRC(r);

        if (expected != static_cast<uint32_t>(r.crc)) {
            throw std::runtime_error("Data corruption detected in record with timestamp: " + std::to_string(r.timestamp));
        }
    }

    inFile.close();
    return records;
}

std::vector<Record> Storage::readRange(int64_t startTs, int64_t endTs) const
{
    if (startTs > endTs) throw std::runtime_error("Invalid time range");

    if (startTs > lastTimestamp) return {};

    if (sparseIndex.empty()) return {};

    if (endTs < sparseIndex[0].timestamp) return {};

    startTs = std::max(sparseIndex[0].timestamp, startTs);
    endTs = std::min(lastTimestamp, endTs);

    size_t left = 0;
    size_t right = sparseIndex.size()-1;
    size_t lastIndex = sparseIndex.size();
    while (left <= right)
    {
        size_t mid = left + (right - left) / 2;
        if (sparseIndex[mid].timestamp <= startTs)
        {
            lastIndex = mid;
            left = mid + 1;
        }
        else
        {
            if (mid == 0) break;
            right = mid - 1;
        }
    }

    if (lastIndex == sparseIndex.size()) return {};
    size_t startRecordIndex = sparseIndex[lastIndex].recordIndex;

    std::ifstream inFile(filename, std::ios::binary);
    if (!inFile.is_open()) throw std::runtime_error("Failed to open file: " + filename);

    inFile.seekg(0, std::ios::end);
    std::streampos fileSize = inFile.tellg();
    std::streampos dataSize = fileSize - static_cast<std::streampos>(sizeof(TSDBHeader));
    size_t numRecords = dataSize / sizeof(Record);

    std::vector<Record> records;
    inFile.seekg(static_cast<std::streamoff>(sizeof(TSDBHeader)) + static_cast<std::streamoff>(startRecordIndex*sizeof(Record)), std::ios::beg);
    for (size_t i=startRecordIndex; i<numRecords; i++)
    {
        Record record;
        if (!inFile.read(reinterpret_cast<char*>(&record), sizeof(Record))) {
            throw std::runtime_error("Failed to read records from file: " + filename);
        }
        if (record.timestamp > endTs) break;
        if (record.timestamp < startTs) continue;
        uint32_t expected = computeCRC(record);
        if (expected != static_cast<uint32_t>(record.crc)) {
            throw std::runtime_error("Data corruption detected in record with timestamp: " + std::to_string(record.timestamp));
        }
        records.push_back(record);
    }
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

    uint32_t expected = computeCRC(last);

    if (expected != static_cast<uint32_t>(last.crc)) {
        throw std::runtime_error("Data corruption detected in record with timestamp: " + std::to_string(last.timestamp));
    }

    return last;
}

Record Storage::getRecord(size_t index) const
{
    std::ifstream inFile(filename, std::ios::binary);
    if (!inFile.is_open()) throw std::runtime_error("Failed to open file: " + filename);

    inFile.seekg(0, std::ios::end);

    if (index >= static_cast<int>(recordCount)) throw std::out_of_range("Record index out of range");
    inFile.seekg(static_cast<std::streamoff>(sizeof(TSDBHeader)) + static_cast<std::streamoff>(index*sizeof(Record)), std::ios::beg);

    Record record;
    if (!inFile.read(reinterpret_cast<char*>(&record), sizeof(Record))) {
        throw std::runtime_error("Failed to read record: " + filename);
    }

    uint32_t expected = computeCRC(record);

    if (expected != static_cast<uint32_t>(record.crc)) {
        throw std::runtime_error("Data corruption detected in record with timestamp: " + std::to_string(record.timestamp));
    }

    return record;
}

int64_t Storage::getLastTimestamp() const
{
    return lastTimestamp;
}

TSDBHeader Storage::getHeader() const
{
    return header;
}

size_t Storage::getRecordCount() const
{
    return recordCount;
}

size_t Storage::getSparseIndexStep() const
{
    return sparseIndexStep;
}

const std::vector<IndexEntry>& Storage::getSparseIndex() const
{
    return sparseIndex;
}

TSDBHeader Storage::validateAndReadHeader(std::ifstream& inFile)
{
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
        std::streampos dataSize = fileSize - static_cast<std::streampos>(sizeof(TSDBHeader));

        if (dataSize % static_cast<std::streampos>(sizeof(Record)) != 0) {
            throw std::runtime_error("Corrupted TSDB file: misaligned record section");
        }

        return temporaryHeader;
    }
    throw std::runtime_error("File doesn't exist: " + filename);
}

uint32_t Storage::computeCRC(const Record& r) const
{
    uint32_t crc = crc32(0L, Z_NULL, 0);
    crc = crc32(crc, reinterpret_cast<const Bytef*>(&r.timestamp), sizeof(r.timestamp));
    crc = crc32(crc, reinterpret_cast<const Bytef*>(&r.value), sizeof(r.value));
    return crc;
}

void Storage::buildSparseIndex()
{
    std::ifstream inFile(filename, std::ios::binary);
    if (!inFile.is_open()) throw std::runtime_error("Failed to open file: " + filename);

    inFile.seekg(0, std::ios::end);

    size_t index = 0;

    while (index<recordCount)
    {
        inFile.seekg(static_cast<std::streamoff>(sizeof(TSDBHeader)) + static_cast<std::streamoff>(index*sizeof(Record)), std::ios::beg);
        int64_t ts;
        if (!inFile.read(reinterpret_cast<char*>(&ts), sizeof(ts))) {
            throw std::runtime_error("Failed to read timestamp from record: " + filename);
        }
        IndexEntry indexEntry;
        indexEntry.timestamp = ts;
        indexEntry.recordIndex = index;
        sparseIndex.push_back(indexEntry);

        index += sparseIndexStep;
    }
}