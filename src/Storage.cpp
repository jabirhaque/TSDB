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

        if (fileSize >= static_cast<std::streampos>(sizeof(Record))) {
            inFile.seekg(-static_cast<std::streamoff>(sizeof(Record)), std::ios::end);

            Record last;
            inFile.read(reinterpret_cast<char*>(&last), sizeof(Record));

            lastTimestamp = last.timestamp;
        }
    }
}

int64_t Storage::getLastTimestamp()
{
    return lastTimestamp;
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
    inFile.seekg(0, std::ios::beg);

    std::vector<Record> records;
    if (fileSize == 0) return records;

    size_t numRecords = fileSize / sizeof(Record);
    records.resize(numRecords);

    inFile.read(reinterpret_cast<char*>(records.data()), fileSize);

    inFile.close();
    return records;
}