#pragma once
#include "Storage.hpp"

class TSDBCLI
{
public:
    explicit TSDBCLI();
    void performance();
    void create(std::string name);
    void use(std::string name);

private:
    std::unique_ptr<Storage> storage;
};