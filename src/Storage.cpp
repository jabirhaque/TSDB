#include "Storage.hpp"
#include <fstream>
#include <sstream>
#include <limits>
#include <cstdint>
#include <optional>
#include <zlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdexcept>
#include <algorithm>
#include <iostream>
#include <mutex>
#include <errno.h>
#include <sys/stat.h>


Storage::Storage(const std::string& filename, size_t sparseIndexStep) : filename(filename), sparseIndexStep(sparseIndexStep)
{
    lastTimestamp = std::numeric_limits<int64_t>::min();

    flushThread = std::thread(&Storage::flushLoop, this);

    read_fd = ::open(filename.c_str(), O_RDONLY);
    if (read_fd == -1) {
        if (errno != ENOENT){
            throw std::runtime_error("Failed to open file: " + filename);
        }
        header = {'T', 'S', 'D', 'B', 1, {0, 0, 0}, static_cast<uint16_t>(sizeof(Record))};
        int temp_fd = ::open(filename.c_str(),
                O_WRONLY | O_APPEND | O_CREAT,
                0644);
        if (temp_fd == -1){
            throw std::runtime_error("Failed to create new file: " + filename);
        }
        ssize_t bytes = ::write(temp_fd, &header, sizeof(TSDBHeader));
        ::close(temp_fd);
        if (bytes != sizeof(TSDBHeader)){
            throw std::runtime_error("Failed to write header to new file: " + filename);
        }
        read_fd = ::open(filename.c_str(), O_RDONLY);
        if (read_fd == -1){
            throw std::runtime_error("Failed to open file: " + filename);
        }
        recordCount = 0;
    }
    else
    {
        header = validateAndReadHeader(filename);
        recordCount = recoverPartialWriteAndReturnRecordCount(filename);
    }

    append_fd = ::open(filename.c_str(), O_WRONLY | O_APPEND);

    if (append_fd == -1) {
        throw std::runtime_error("Failed to open data file");
    }

    std::optional<Record> lastRecord = getLastRecord();
    if (lastRecord.has_value())
    {
        lastTimestamp = lastRecord->timestamp;
    }

    buildSparseIndex();
}

Storage::~Storage()
{
    running = false;
    if (flushThread.joinable()) flushThread.join();
    ::close(append_fd);
    ::close(read_fd);
}

bool Storage::append(Record r)
{
    r.crc = computeCRC(r);

    std::unique_lock lock(bufferMutex);
    if (r.timestamp <= lastTimestamp) return false;

    activeBuffer.push_back(r);
    lastTimestamp =  r.timestamp;
    return true;
}

std::vector<Record> Storage::readAll() const {
    std::shared_lock lock(diskMutex);

    struct stat st;
    if (::fstat(read_fd, &st) != 0){
        throw std::runtime_error("Failed to read file stats: " + filename); 
    }

    off_t fileSize = st.st_size;
    off_t dataSize = fileSize - sizeof(TSDBHeader);


    if (dataSize == 0) return {};

    if (dataSize % sizeof(Record) != 0) {
        throw std::runtime_error("Corrupted TSDB file: misaligned record section");
    }
    if (dataSize != recordCount * sizeof(Record)) {
        throw std::runtime_error("Corrupted TSDB file: record count not matching");
    }
    
    std::vector<Record> records(recordCount);

    if (::pread(read_fd, records.data(), dataSize, sizeof(TSDBHeader)) != dataSize){
        throw std::runtime_error("Failed to read records from file: " + filename);
    }

    for (const Record& r: records)
    {
        uint32_t expected = computeCRC(r);

        if (expected != r.crc) {
            throw std::runtime_error("Data corruption detected in record with timestamp: " + std::to_string(r.timestamp));
        }
    }
    return records;
}

std::vector<Record> Storage::readRange(int64_t startTs, int64_t endTs) const
{
    std::shared_lock lock(diskMutex);
    if (startTs > endTs) throw std::runtime_error("Invalid time range");

    if (startTs > lastTimestamp) return {};

    if (sparseIndex.empty()) return {};

    if (endTs < sparseIndex[0].timestamp) return {};

    startTs = std::max(sparseIndex[0].timestamp, startTs);
    endTs = std::min(lastTimestamp, endTs);

    size_t left = 0;
    size_t right = sparseIndex.size()-1;
    std::optional<size_t> lastIndex;
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

    if (!lastIndex.has_value()) return {};
    size_t startRecordIndex = sparseIndex[lastIndex.value()].recordIndex;

    size_t leftDisk = startRecordIndex;
    size_t rightDisk = std::min(startRecordIndex+sparseIndexStep, recordCount-1);
    std::optional<size_t> lastDiskIndex;

    while (leftDisk <= rightDisk)
    {
        size_t mid = leftDisk + (rightDisk - leftDisk) / 2;

        Record record;
        if (::pread(read_fd, &record, sizeof(Record), sizeof(TSDBHeader)+mid*sizeof(Record)) != sizeof(Record)){
            throw std::runtime_error("Failed to read records from file: " + filename);
        }

        if (record.timestamp >= startTs)
        {
            lastDiskIndex = mid;
            if (mid == 0) break;
            rightDisk = mid - 1;
        }
        else
        {
            leftDisk = mid + 1;
        }
    }

    if (!lastDiskIndex.has_value()) return {};

    std::vector<Record> records;
    for (size_t i=lastDiskIndex.value(); i<recordCount; i++)
    {
        Record record;
        if (::pread(read_fd, &record, sizeof(Record), sizeof(TSDBHeader)+i*sizeof(Record)) != sizeof(Record));
        if (record.timestamp > endTs) break;
        uint32_t expected = computeCRC(record);
        if (expected != static_cast<uint32_t>(record.crc)) {
            throw std::runtime_error("Data corruption detected in record with timestamp: " + std::to_string(record.timestamp));
        }
        records.push_back(record);
    }
    return records;
}

std::optional<Record> Storage::readFromTime(int64_t timestamp) const
{
    std::vector<Record> result = readRange(timestamp, timestamp);
    if (result.empty()) return std::nullopt;
    return result.front();
}

std::optional<Record> Storage::getLastRecord() const
{
    std::shared_lock lock(diskMutex);
    
    struct stat st;
    if (::fstat(read_fd, &st) != 0){
        throw std::runtime_error("Failed to read file stats: " + filename); 
    }

    off_t fileSize = st.st_size;
    off_t dataSize = fileSize - sizeof(TSDBHeader);
    
    if (dataSize == 0) {
        return std::nullopt;
    }

    Record last;

    ssize_t bytes = ::pread(read_fd, &last, sizeof(Record), fileSize-sizeof(Record));

    if (bytes != sizeof(Record)){
        throw std::runtime_error("Failed to read last record: " + filename); 
    }

    uint32_t expected = computeCRC(last);

    if (expected != last.crc){
        throw std::runtime_error("Data corruption detected in record with timestamp: " + std::to_string(last.timestamp));
    }

    return last;
}

Record Storage::getRecord(size_t index) const
{
    std::shared_lock lock(diskMutex);

    if (index >= recordCount) throw std::out_of_range("Record index out of range");

    Record record;
    if (::pread(read_fd, &record, sizeof(Record), sizeof(TSDBHeader)+index*sizeof(Record)) != sizeof(Record)){
        throw std::runtime_error("Failed to read record: " + filename);
    }

    uint32_t expected = computeCRC(record);

    if (expected != record.crc) {
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
    std::shared_lock lock(diskMutex);
    return recordCount;
}

size_t Storage::getSparseIndexStep() const
{
    return sparseIndexStep;
}

std::vector<IndexEntry> Storage::getSparseIndex() const
{
    std::shared_lock lock(diskMutex);
    return sparseIndex;
}

TSDBHeader Storage::validateAndReadHeader(const std::string& filename)
{
    int temp_fd = ::open(filename.c_str(), O_RDONLY);
    if (temp_fd == -1){
        throw std::runtime_error("Failed to open file: " + filename);
    }
    
    struct stat st;
    if (::fstat(temp_fd, &st) != 0){
        ::close(temp_fd);
        throw std::runtime_error("Failed to read file stats: " + filename); 
    }

    off_t fileSize = st.st_size;
    if (fileSize < sizeof(TSDBHeader)){
        ::close(temp_fd);
        throw std::runtime_error("File too small to contain valid TSDB header: " + filename);
    }
    
    TSDBHeader temporaryHeader;
    ssize_t bytes = ::read(temp_fd, &temporaryHeader, sizeof(TSDBHeader));
    ::close(temp_fd);
    if (bytes != sizeof(TSDBHeader)){
        throw std::runtime_error("Failed to read TSDB header: " + filename); 
    }

    if (temporaryHeader.magic[0] != 'T' || temporaryHeader.magic[1] != 'S' ||
        temporaryHeader.magic[2] != 'D' || temporaryHeader.magic[3] != 'B') {
            throw std::runtime_error("Invalid TSDB file magic number: " + filename);
        }

    if (temporaryHeader.version != 1){
        throw std::runtime_error("Unsupported TSDB file version: " + filename);
    }

    if (temporaryHeader.recordSize != sizeof(Record)){
        throw std::runtime_error("Record size mismatch: " + filename);
    }

    return temporaryHeader;
}

size_t Storage::recoverPartialWriteAndReturnRecordCount(const std::string& filename)
{   
    struct stat st;
    if (::fstat(read_fd, &st) != 0){
        throw std::runtime_error("Failed to read file stats: " + filename); 
    }

    off_t fileSize = st.st_size;
    off_t dataSize = fileSize - sizeof(TSDBHeader);
    size_t count = dataSize/sizeof(Record);
    size_t remainder = dataSize % sizeof(Record);

    if (remainder == 0) {
        return count;
    }

    std::streamoff newFileSize = fileSize - remainder;


    int temp_fd = ::open(filename.c_str(), O_WRONLY);
    if (temp_fd == -1) {
        throw std::runtime_error("Failed to open file for truncation");
    }

    if (::ftruncate(temp_fd, newFileSize) != 0) {
        ::close(temp_fd);
        throw std::runtime_error("Failed to truncate TSDB file");
    }

    ::fsync(temp_fd);

    ::close(temp_fd);

    return count;
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
    size_t index = 0;

    while (index<recordCount)
    {
        int64_t ts;
        if (::pread(read_fd, &ts, sizeof(int64_t), sizeof(TSDBHeader)+index*sizeof(Record)) != sizeof(int64_t)){
            throw std::runtime_error("Failed to read timestamp from record: " + filename);
        }
        IndexEntry indexEntry;
        indexEntry.timestamp = ts;
        indexEntry.recordIndex = index;
        sparseIndex.push_back(indexEntry);

        index += sparseIndexStep;
    }
}

void Storage::flushLoop()
{
    while (running) {
        std::this_thread::sleep_for(flushInterval);

        std::vector<Record> batch;

        {
            std::unique_lock lock(bufferMutex);
            batch.swap(activeBuffer);
        }

        if (batch.empty()) continue;

        flushBufferToDisk(batch);
    }
}

void Storage::flushBufferToDisk(std::vector<Record>& batch) {
    std::unique_lock lock(diskMutex);

    size_t bytes = batch.size() * sizeof(Record);

    ssize_t written = ::write(append_fd, batch.data(), bytes);
    if (written != static_cast<ssize_t>(bytes)) {
        throw std::runtime_error("Partial write");
    }

    if (::fsync(append_fd) != 0) {
        throw std::runtime_error("fsync failed");
    }

    for (const auto& r : batch) {

        if (recordCount % sparseIndexStep == 0) {
            sparseIndex.push_back({r.timestamp, recordCount});
        }
        ++recordCount;
    }
}