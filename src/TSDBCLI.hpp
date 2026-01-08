#pragma once
#include "Storage.hpp"

class TSDBCLI
{
public:
    explicit TSDBCLI(Storage& storage);
    void run();
    void printHelp() const;
    bool validateReadRangeCommand(const std::string& command);
    bool validateReadFromCommand(const std::string& command);
    bool validateAppendCommand(const std::string& command);
    void handleCommand(const std::string& command);

private:
    Storage& storage;
};