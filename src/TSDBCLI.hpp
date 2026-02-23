#pragma once
#include "Storage.hpp"

class TSDBCLI
{
public:
    explicit TSDBCLI();
    void performance();
    void create(std::string name);
    void use(std::string name);
    bool validateFileName(const std::string& name);
    void readall();
    void readfrom(int64_t timestamp);
    void readrange(int64_t start, int64_t end);
    void append(int64_t timestamp, double value);

private:
    std::unique_ptr<Storage> storage;
};