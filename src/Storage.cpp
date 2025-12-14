#include "Storage.hpp"
#include <fstream>
#include <sstream>

Storage::Storage(const std::string& filename) : filename(filename)
{
    lastTimestamp = std::numeric_limits<int64_t>::min();
    {
        std::ifstream inFile(filename);
        if (inFile.is_open()) {
            Record last;
            bool hasRecord = false;

            while (inFile >> last.timestamp >> last.value) {
                hasRecord = true;
            }

            if (hasRecord) {
                lastTimestamp = last.timestamp;
            }
        }
    }
}

bool Storage::append(const Record& r)
{
    if (r.timestamp <= lastTimestamp) return false;

    std::ofstream outFile(filename, std::ios::app);
    if (!outFile.is_open()) {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }

    outFile << r.timestamp << " " << r.value << "\n";
    outFile.close();

    lastTimestamp = r.timestamp;
    return true;
}


std::vector<Record> Storage::readAll() const {
    std::ifstream inFile(filename);
    if (!inFile.is_open()) {
        throw std::runtime_error("Failed to open file for reading: " + filename);
    }

    std::vector<Record> records;
    std::string line;

    while (std::getline(inFile, line)) {
        std::istringstream iss(line);
        Record r;
        if (!(iss >> r.timestamp >> r.value)) {
            continue;
        }
        records.push_back(r);
    }

    inFile.close();
    return records;
}