#include "Storage.hpp"
#include <fstream>
#include <sstream>

Storage::Storage(const std::string& filename) : filename(filename) {}

void Storage::append(const Record& r) {
    std::ofstream outFile(filename, std::ios::app); // open in append mode
    if (!outFile.is_open()) {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }

    outFile << r.timestamp << " " << r.value << "\n";

    outFile.close();
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