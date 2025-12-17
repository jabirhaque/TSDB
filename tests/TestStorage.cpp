#include <gtest/gtest.h>
#include "../src/Storage.hpp"

TEST(StorageTest, AppendSingleRecord) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    EXPECT_TRUE(s.append(r1));
    auto records = s.readAll();
    ASSERT_EQ(records.size(), 1);
}

TEST(StorageTest, AppendMultipleRecords) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 42.5};

    EXPECT_TRUE(s.append(r1));
    EXPECT_TRUE(s.append(r2));
    auto records = s.readAll();
    ASSERT_EQ(records.size(), 2);
}

TEST(StorageTest, ReadAll) {
    const char* filename = "testdb.txt";
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

TEST(StorageTest, MonotonicAppend) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {900, 43.0};

    EXPECT_TRUE(s.append(r1));
    EXPECT_FALSE(s.append(r2));
    auto records = s.readAll();
    ASSERT_EQ(records.size(), 1);
}

TEST(StorageTest, RestartPreservesData) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};

    s.append(r1);
    s.append(r2);

    Storage s2(filename);

    auto records = s2.readAll();
    ASSERT_EQ(records.size(), 2);
    EXPECT_EQ(records[0].timestamp, 1000);
    EXPECT_EQ(records[1].value, 43.5);
}

TEST(StorageTest, RestartPreservesLastTimestamp) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};

    s.append(r1);
    s.append(r2);

    Storage s2(filename);

    EXPECT_EQ(s2.getLastTimestamp(), 1100);
}

TEST(StorageTest, RestartPreservesMonotonicity) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};

    s.append(r1);
    s.append(r2);

    Storage s2(filename);

    Record r3 {900, 41.0};
    Record r4 {1200, 42.5};
    Record r5 {1000, 44.0};

    EXPECT_FALSE(s2.append(r3));
    EXPECT_TRUE(s2.append(r4));
    EXPECT_FALSE(s2.append(r5));

    auto records = s.readAll();
    auto records2 = s.readAll();
    ASSERT_EQ(records.size(), 3);
    ASSERT_EQ(records.size(), records2.size());

}
