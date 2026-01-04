#pragma once
#include "Storage.hpp"

class TSDBCLI
{
public:
    explicit TSDBCLI(Storage& storage);
    void run();
    void printHelp() const;

private:
    Storage& storage;
    void handleCommand(const std::string& command);
};