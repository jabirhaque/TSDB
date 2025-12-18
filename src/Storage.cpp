#include "Storage.hpp"
#include <fstream>
#include <sstream>


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

        if (dataSize >= static_cast<std::streampos>(sizeof(Record))) {
            inFile.seekg(-static_cast<std::streamoff>(sizeof(Record)), std::ios::end);

            Record last;
            if (!inFile.read(reinterpret_cast<char*>(&last), sizeof(Record))) {
                throw std::runtime_error("Failed to read last record: " + filename);
            }

            lastTimestamp = last.timestamp;
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
}

int64_t Storage::getLastTimestamp()
{
    return lastTimestamp;
}

TSDBHeader Storage::getHeader()
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