#pragma once
#include <cstdint>

struct TSDBHeader {
    char magic[4];
    uint8_t version;
    uint8_t reserved[3];
    uint16_t recordSize;
};