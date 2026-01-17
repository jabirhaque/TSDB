#pragma once
#include "Storage.hpp"

class TSDBCLI
{
public:
    explicit TSDBCLI();
    void run();
    void printHelp() const;
    bool validateCreateCommand(const std::string& command);
    bool validateUseCommand(const std::string& command);
    bool validateReadRangeCommand(const std::string& command);
    bool validateReadFromCommand(const std::string& command);
    bool validateAppendCommand(const std::string& command);
    void handleCommand(const std::string& command);

private:
    std::unique_ptr<Storage> storage;
};