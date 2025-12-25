#pragma once
#include <cstdint>


struct Record
{
    int64_t timestamp;
    double value;
    int32_t crc;
};