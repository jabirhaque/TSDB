#include <gtest/gtest.h>
#include "../src/Storage.hpp"
#include <fstream>
#include <optional>

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

TEST(StorageTest, ValidHeaderCreation) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    TSDBHeader expectedHeader;
    expectedHeader.magic[0] = 'T';
    expectedHeader.magic[1] = 'S';
    expectedHeader.magic[2] = 'D';
    expectedHeader.magic[3] = 'B';
    expectedHeader.version = 1;
    expectedHeader.reserved[0] = 0;
    expectedHeader.reserved[1] = 0;
    expectedHeader.reserved[2] = 0;
    expectedHeader.recordSize = sizeof(Record);

    TSDBHeader actualHeader = s.getHeader();

    EXPECT_EQ(actualHeader.magic[0], expectedHeader.magic[0]);
    EXPECT_EQ(actualHeader.magic[1], expectedHeader.magic[1]);
    EXPECT_EQ(actualHeader.magic[2], expectedHeader.magic[2]);
    EXPECT_EQ(actualHeader.magic[3], expectedHeader.magic[3]);
    EXPECT_EQ(actualHeader.version, expectedHeader.version);
    EXPECT_EQ(actualHeader.reserved[0], expectedHeader.reserved[0]);
    EXPECT_EQ(actualHeader.reserved[1], expectedHeader.reserved[1]);
    EXPECT_EQ(actualHeader.reserved[2], expectedHeader.reserved[2]);
    EXPECT_EQ(actualHeader.recordSize, expectedHeader.recordSize);
}

TEST(StorageTest, InvalidMagicNumberThrows) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    TSDBHeader badHeader = {'B', 'A', 'D', '!', 1, {0,0,0}, static_cast<uint16_t>(sizeof(Record))};
    std::ofstream outFile(filename, std::ios::binary | std::ios::app);
    outFile.write(reinterpret_cast<const char*>(&badHeader), sizeof(TSDBHeader));
    outFile.close();

    try {
        Storage s(filename);
        FAIL() << "Expected std::runtime_error";
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ(e.what(), "Invalid TSDB file magic number: testdb.tsdb");
    } catch (...) {
        FAIL() << "Expected std::runtime_error";
    }
}

TEST(StorageTest, IncompatibleVersionThrows) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    TSDBHeader badHeader = {'T', 'S', 'D', 'B', 2, {0,0,0}, static_cast<uint16_t>(sizeof(Record))};
    std::ofstream outFile(filename, std::ios::binary | std::ios::app);
    outFile.write(reinterpret_cast<const char*>(&badHeader), sizeof(TSDBHeader));
    outFile.close();

    try {
        Storage s(filename);
        FAIL() << "Expected std::runtime_error";
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ(e.what(), "Unsupported TSDB file version: testdb.tsdb");
    } catch (...) {
        FAIL() << "Expected std::runtime_error";
    }
}

TEST(StorageTest, InvalidRecordSizeThrows) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    TSDBHeader badHeader = {'T', 'S', 'D', 'B', 1, {0,0,0}, 12};
    std::ofstream outFile(filename, std::ios::binary | std::ios::app);
    outFile.write(reinterpret_cast<const char*>(&badHeader), sizeof(TSDBHeader));
    outFile.close();

    try {
        Storage s(filename);
        FAIL() << "Expected std::runtime_error";
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ(e.what(), "Record size mismatch: testdb.tsdb");
    } catch (...) {
        FAIL() << "Expected std::runtime_error";
    }
}

TEST(StorageTest, GetLastRecord) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};

    s.append(r1);
    s.append(r2);

    std::optional<Record> lastRecord = s.getLastRecord();

    EXPECT_TRUE(lastRecord.has_value());
    EXPECT_EQ(lastRecord->timestamp, 1100);
    EXPECT_EQ(s.getLastTimestamp(), lastRecord->timestamp);
}

TEST(StorageTest, NoLastRecord) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    std::optional<Record> lastRecord = s.getLastRecord();

    EXPECT_FALSE(lastRecord.has_value());
    EXPECT_EQ(s.getLastTimestamp(), std::numeric_limits<int64_t>::min());
}

TEST(StorageTest, GetRecordByIndex) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};
    Record r3 {1200, 44.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);

    Record rec = s.getRecord(1);

    EXPECT_EQ(rec.timestamp, 1100);
    EXPECT_EQ(rec.value, 43.5);
}

TEST(StorageTest, GetFirstRecordByIndex) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};
    Record r3 {1200, 44.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);

    Record rec = s.getRecord(0);

    EXPECT_EQ(rec.timestamp, 1000);
    EXPECT_EQ(rec.value, 42.0);
}

TEST(StorageTest, GetLastRecordByIndex) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};
    Record r3 {1200, 44.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);

    Record rec = s.getRecord(2);

    EXPECT_EQ(rec.timestamp, 1200);
    EXPECT_EQ(rec.value, 44.0);
}

TEST(StorageTest, GetRecordByOutOfBoundsIndexThrows) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};
    Record r3 {1200, 44.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);

    try {
        s.getRecord(3);
        FAIL() << "Expected std::out_of_range";
    } catch (const std::out_of_range& e) {
        EXPECT_STREQ(e.what(), "Record index out of range");
    } catch (...) {
        FAIL() << "Expected std::out_of_range";
    }
}