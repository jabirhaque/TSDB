#include <gtest/gtest.h>
#include "../src/Storage.hpp"

TEST(StorageTest, MonotonicAppend) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {900, 43.0};

    EXPECT_TRUE(s.append(r1));
    EXPECT_FALSE(s.append(r2));
}

TEST(StorageTest, ReadAll) {
    const char* filename = "testdb_readall.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};

    s.append(r1);
    s.append(r2);

    auto records = s.readAll();
    ASSERT_EQ(records.size(), 2);
    EXPECT_EQ(records[0].timestamp, 1000);
    EXPECT_EQ(records[1].value, 43.5);
}
