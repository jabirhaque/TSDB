#include <gtest/gtest.h>
#include "../src/Storage.hpp"
#include <fstream>
#include <optional>

TEST(StorageTest, AppendSingleRecord) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    EXPECT_TRUE(s.append(r1));

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto records = s.readAll();
    ASSERT_EQ(records.size(), 1);
}

TEST(StorageTest, AppendMultipleRecords) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 42.5};

    EXPECT_TRUE(s.append(r1));
    EXPECT_TRUE(s.append(r2));

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto records = s.readAll();
    ASSERT_EQ(records.size(), 2);
}

TEST(StorageTest, ReadAll) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};

    s.append(r1);
    s.append(r2);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto records = s.readAll();
    ASSERT_EQ(records.size(), 2);
    EXPECT_EQ(records[0].timestamp, 1000);
    EXPECT_EQ(records[1].value, 43.5);
}

TEST(StorageTest, MonotonicAppend) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {900, 43.0};

    EXPECT_TRUE(s.append(r1));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(s.append(r2));

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto records = s.readAll();
    ASSERT_EQ(records.size(), 1);
}

TEST(StorageTest, RestartPreservesData) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};

    s.append(r1);
    s.append(r2);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    Storage s2(filename);

    auto records = s2.readAll();
    ASSERT_EQ(records.size(), 2);
    EXPECT_EQ(records[0].timestamp, 1000);
    EXPECT_EQ(records[1].value, 43.5);
}

TEST(StorageTest, RestartPreservesLastTimestamp) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};

    s.append(r1);
    s.append(r2);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    Storage s2(filename);

    EXPECT_EQ(s2.getLastTimestamp(), 1100);
}

TEST(StorageTest, RestartPreservesMonotonicity) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};

    s.append(r1);
    s.append(r2);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    Storage s2(filename);

    Record r3 {900, 41.0};
    Record r4 {1200, 42.5};
    Record r5 {1000, 44.0};

    EXPECT_FALSE(s2.append(r3));
    EXPECT_TRUE(s2.append(r4));
    EXPECT_FALSE(s2.append(r5));

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto records = s.readAll();
    auto records2 = s.readAll();
    ASSERT_EQ(records.size(), 3);
    ASSERT_EQ(records.size(), records2.size());
}

TEST(StorageTest, ValidHeaderCreation) {
    const char* filename = "testdb.tsdb";
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
    std::ifstream inFile(filename, std::ios::binary);

    try {
        Storage::validateAndReadHeader(inFile, filename);
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
    std::ifstream inFile(filename, std::ios::binary);

    try {
        Storage::validateAndReadHeader(inFile, filename);
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
    std::ifstream inFile(filename, std::ios::binary);

    try {
        Storage::validateAndReadHeader(inFile, filename);
        FAIL() << "Expected std::runtime_error";
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ(e.what(), "Record size mismatch: testdb.tsdb");
    } catch (...) {
        FAIL() << "Expected std::runtime_error";
    }
}

TEST(StorageTest, GetLastRecord) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};

    s.append(r1);
    s.append(r2);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::optional<Record> lastRecord = s.getLastRecord();

    EXPECT_TRUE(lastRecord.has_value());
    EXPECT_EQ(lastRecord->timestamp, 1100);
    EXPECT_EQ(s.getLastTimestamp(), lastRecord->timestamp);
}

TEST(StorageTest, NoLastRecord) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    std::optional<Record> lastRecord = s.getLastRecord();

    EXPECT_FALSE(lastRecord.has_value());
    EXPECT_EQ(s.getLastTimestamp(), std::numeric_limits<int64_t>::min());
}

TEST(StorageTest, GetRecordByIndex) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};
    Record r3 {1200, 44.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    Record rec = s.getRecord(1);

    EXPECT_EQ(rec.timestamp, 1100);
    EXPECT_EQ(rec.value, 43.5);
}

TEST(StorageTest, GetFirstRecordByIndex) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};
    Record r3 {1200, 44.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    Record rec = s.getRecord(0);

    EXPECT_EQ(rec.timestamp, 1000);
    EXPECT_EQ(rec.value, 42.0);
}

TEST(StorageTest, GetLastRecordByIndex) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};
    Record r3 {1200, 44.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    Record rec = s.getRecord(2);

    EXPECT_EQ(rec.timestamp, 1200);
    EXPECT_EQ(rec.value, 44.0);
}

TEST(StorageTest, GetRecordByOutOfBoundsIndexThrows) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};
    Record r3 {1200, 44.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    try {
        s.getRecord(3);
        FAIL() << "Expected std::out_of_range";
    } catch (const std::out_of_range& e) {
        EXPECT_STREQ(e.what(), "Record index out of range");
    } catch (...) {
        FAIL() << "Expected std::out_of_range";
    }
}

TEST(StorageTest, SparseIndexTest) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);
    ASSERT_EQ(s.getSparseIndexStep(), 4);
    ASSERT_TRUE(s.getSparseIndex().empty());

    Record r1 {1000, 40.0};
    Record r2 {1100, 41.0};
    Record r3 {1200, 42.0};
    Record r4 {1300, 43.0};
    Record r5 {1400, 44.0};
    Record r6 {1500, 45.0};
    Record r7 {1600, 46.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);
    s.append(r4);
    s.append(r5);
    s.append(r6);
    s.append(r7);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    const std::vector<IndexEntry>& sparseIndex = s.getSparseIndex();
    ASSERT_EQ(s.getSparseIndexStep(), 4);
    ASSERT_EQ(sparseIndex.size(), 2);
    ASSERT_EQ(sparseIndex[0].timestamp, 1000);
    ASSERT_EQ(sparseIndex[0].recordIndex, 0);
    ASSERT_EQ(sparseIndex[1].timestamp, 1400);
    ASSERT_EQ(sparseIndex[1].recordIndex, 4);

    Storage s2(filename);
    const std::vector<IndexEntry>& sparseIndex2 = s2.getSparseIndex();
    ASSERT_EQ(s2.getSparseIndexStep(), 4);
    ASSERT_EQ(sparseIndex2.size(), 2);
    ASSERT_EQ(sparseIndex2[0].timestamp, 1000);
    ASSERT_EQ(sparseIndex2[0].recordIndex, 0);
    ASSERT_EQ(sparseIndex2[1].timestamp, 1400);
    ASSERT_EQ(sparseIndex2[1].recordIndex, 4);
}

TEST(StorageTest, ReadRangeInclusiveBothEnds) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 40.0};
    Record r2 {1100, 41.0};
    Record r3 {1200, 42.0};
    Record r4 {1300, 43.0};
    Record r5 {1400, 44.0};
    Record r6 {1500, 45.0};
    Record r7 {1600, 46.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);
    s.append(r4);
    s.append(r5);
    s.append(r6);
    s.append(r7);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::vector<Record> records = s.readRange(1100, 1400);
    ASSERT_EQ(records.size(), 4);
    for (int i=0; i<4; ++i) {
        EXPECT_EQ(records[i].timestamp, 1100 + i*100);
        EXPECT_EQ(records[i].value, 41.0 + i);
    }
}

TEST(StorageTest, ReadRangeInclusiveStartEnd) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 40.0};
    Record r2 {1100, 41.0};
    Record r3 {1200, 42.0};
    Record r4 {1300, 43.0};
    Record r5 {1400, 44.0};
    Record r6 {1500, 45.0};
    Record r7 {1600, 46.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);
    s.append(r4);
    s.append(r5);
    s.append(r6);
    s.append(r7);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::vector<Record> records = s.readRange(1100, 1450);
    ASSERT_EQ(records.size(), 4);
    for (int i=0; i<4; ++i) {
        EXPECT_EQ(records[i].timestamp, 1100 + i*100);
        EXPECT_EQ(records[i].value, 41.0 + i);
    }
}

TEST(StorageTest, ReadRangeInclusiveEndEnd) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 40.0};
    Record r2 {1100, 41.0};
    Record r3 {1200, 42.0};
    Record r4 {1300, 43.0};
    Record r5 {1400, 44.0};
    Record r6 {1500, 45.0};
    Record r7 {1600, 46.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);
    s.append(r4);
    s.append(r5);
    s.append(r6);
    s.append(r7);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::vector<Record> records = s.readRange(1050, 1400);
    ASSERT_EQ(records.size(), 4);
    for (int i=0; i<4; ++i) {
        EXPECT_EQ(records[i].timestamp, 1100 + i*100);
        EXPECT_EQ(records[i].value, 41.0 + i);
    }
}

TEST(StorageTest, ReadRangeExclusiveBothEnds) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 40.0};
    Record r2 {1100, 41.0};
    Record r3 {1200, 42.0};
    Record r4 {1300, 43.0};
    Record r5 {1400, 44.0};
    Record r6 {1500, 45.0};
    Record r7 {1600, 46.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);
    s.append(r4);
    s.append(r5);
    s.append(r6);
    s.append(r7);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::vector<Record> records = s.readRange(1050, 1450);
    ASSERT_EQ(records.size(), 4);
    for (int i=0; i<4; ++i) {
        EXPECT_EQ(records[i].timestamp, 1100 + i*100);
        EXPECT_EQ(records[i].value, 41.0 + i);
    }
}

TEST(StorageTest, ReadRangeOutOfRangeLeft) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 40.0};
    Record r2 {1100, 41.0};
    Record r3 {1200, 42.0};
    Record r4 {1300, 43.0};
    Record r5 {1400, 44.0};
    Record r6 {1500, 45.0};
    Record r7 {1600, 46.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);
    s.append(r4);
    s.append(r5);
    s.append(r6);
    s.append(r7);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::vector<Record> records = s.readRange(800, 900);
    ASSERT_EQ(records.size(), 0);
}

TEST(StorageTest, ReadRangeOutOfRangeRight) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 40.0};
    Record r2 {1100, 41.0};
    Record r3 {1200, 42.0};
    Record r4 {1300, 43.0};
    Record r5 {1400, 44.0};
    Record r6 {1500, 45.0};
    Record r7 {1600, 46.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);
    s.append(r4);
    s.append(r5);
    s.append(r6);
    s.append(r7);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::vector<Record> records = s.readRange(1700, 1800);
    ASSERT_EQ(records.size(), 0);
}

TEST(StorageTest, ReadRangeOutOfRangeWithin) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 40.0};
    Record r2 {1100, 41.0};
    Record r3 {1200, 42.0};
    Record r4 {1300, 43.0};
    Record r5 {1400, 44.0};
    Record r6 {1500, 45.0};
    Record r7 {1600, 46.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);
    s.append(r4);
    s.append(r5);
    s.append(r6);
    s.append(r7);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::vector<Record> records = s.readRange(1325, 1375);
    ASSERT_EQ(records.size(), 0);
}

TEST(StorageTest, ReadRangeLeftOnTheBoundary) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 40.0};
    Record r2 {1100, 41.0};
    Record r3 {1200, 42.0};
    Record r4 {1300, 43.0};
    Record r5 {1400, 44.0};
    Record r6 {1500, 45.0};
    Record r7 {1600, 46.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);
    s.append(r4);
    s.append(r5);
    s.append(r6);
    s.append(r7);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::vector<Record> records = s.readRange(1000, 1400);
    ASSERT_EQ(records.size(), 5);
    for (int i=0; i<5; ++i) {
        EXPECT_EQ(records[i].timestamp, 1000 + i*100);
        EXPECT_EQ(records[i].value, 40.0 + i);
    }
}

TEST(StorageTest, ReadRangeLeftFallsOut) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 40.0}; // here
    Record r2 {1100, 41.0};
    Record r3 {1200, 42.0};
    Record r4 {1300, 43.0};
    Record r5 {1400, 44.0}; // here
    Record r6 {1500, 45.0};
    Record r7 {1600, 46.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);
    s.append(r4);
    s.append(r5);
    s.append(r6);
    s.append(r7);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::vector<Record> records = s.readRange(900, 1400);
    ASSERT_EQ(records.size(), 5);
    for (int i=0; i<5; ++i) {
        EXPECT_EQ(records[i].timestamp, 1000 + i*100);
        EXPECT_EQ(records[i].value, 40.0 + i);
    }
}

TEST(StorageTest, ReadRangeRightOnTheBoundary) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 40.0};
    Record r2 {1100, 41.0};
    Record r3 {1200, 42.0};
    Record r4 {1300, 43.0};
    Record r5 {1400, 44.0};
    Record r6 {1500, 45.0};
    Record r7 {1600, 46.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);
    s.append(r4);
    s.append(r5);
    s.append(r6);
    s.append(r7);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::vector<Record> records = s.readRange(1100, 1600);
    ASSERT_EQ(records.size(), 6);
    for (int i=0; i<6; ++i) {
        EXPECT_EQ(records[i].timestamp, 1100 + i*100);
        EXPECT_EQ(records[i].value, 41.0 + i);
    }
}

TEST(StorageTest, ReadRangeRightFallsOut) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 40.0};
    Record r2 {1100, 41.0};
    Record r3 {1200, 42.0};
    Record r4 {1300, 43.0};
    Record r5 {1400, 44.0};
    Record r6 {1500, 45.0};
    Record r7 {1600, 46.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);
    s.append(r4);
    s.append(r5);
    s.append(r6);
    s.append(r7);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::vector<Record> records = s.readRange(1100, 1700);
    ASSERT_EQ(records.size(), 6);
    for (int i=0; i<6; ++i) {
        EXPECT_EQ(records[i].timestamp, 1100 + i*100);
        EXPECT_EQ(records[i].value, 41.0 + i);
    }
}

TEST(StorageTest, ReadRangeSingleRecord) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 40.0};
    Record r2 {1100, 41.0};
    Record r3 {1200, 42.0};
    Record r4 {1300, 43.0};
    Record r5 {1400, 44.0};
    Record r6 {1500, 45.0};
    Record r7 {1600, 46.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);
    s.append(r4);
    s.append(r5);
    s.append(r6);
    s.append(r7);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::vector<Record> records = s.readRange(1100, 1100);
    ASSERT_EQ(records.size(), 1);
    ASSERT_EQ(records[0].timestamp, 1100);
    ASSERT_EQ(records[0].value, 41.0);
}

TEST(StorageTest, ReadRangeSingleRecordNoResult) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 40.0};
    Record r2 {1100, 41.0};
    Record r3 {1200, 42.0};
    Record r4 {1300, 43.0};
    Record r5 {1400, 44.0};
    Record r6 {1500, 45.0};
    Record r7 {1600, 46.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);
    s.append(r4);
    s.append(r5);
    s.append(r6);
    s.append(r7);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::vector<Record> records = s.readRange(1150, 1150);
    ASSERT_TRUE(records.empty());
}

TEST(StorageTest, ReadRangeEmptyFile) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    std::vector<Record> records = s.readRange(1100, 1400);
    ASSERT_EQ(records.size(), 0);
}

TEST(StorageTest, ReadRangeInvalidRangeThrows) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 40.0};
    Record r2 {1100, 41.0};
    Record r3 {1200, 42.0};
    Record r4 {1300, 43.0};
    Record r5 {1400, 44.0};
    Record r6 {1500, 45.0};
    Record r7 {1600, 46.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);
    s.append(r4);
    s.append(r5);
    s.append(r6);
    s.append(r7);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    try {
        std::vector<Record> records = s.readRange(1200, 1000);
        FAIL() << "Expected std::runtime_error";
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ(e.what(), "Invalid time range");
    } catch (...) {
        FAIL() << "Expected std::runtime_error";
    }
}

TEST(StorageTest, CorrupedRecordThrows) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1100, 43.5};

    s.append(r1);
    s.append(r2);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    {
        std::fstream file(filename, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(file.is_open());

        std::streamoff crcOffset =
            sizeof(TSDBHeader) +
            sizeof(int64_t) +
            sizeof(double);

        file.seekp(crcOffset, std::ios::beg);

        uint32_t badCrc = 0xDEADBEEF;
        file.write(reinterpret_cast<const char*>(&badCrc), sizeof(badCrc));

        file.close();
    }

    try {
        auto records = s.readAll();
        FAIL() << "Expected std::runtime_error";
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ(e.what(), "Data corruption detected in record with timestamp: 1000");
    } catch (...) {
        FAIL() << "Expected std::runtime_error";
    }
}

TEST(StorageTest, RecoveryFromCorruptedRecord) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 40.0};
    Record r2 {1100, 41.0};
    Record r3 {1200, 42.0};
    Record r4 {1300, 43.0};
    Record r5 {1400, 44.0};
    Record r6 {1500, 45.0};
    Record r7 {1600, 46.0};

    s.append(r1);
    s.append(r2);
    s.append(r3);
    s.append(r4);
    s.append(r5);
    s.append(r6);
    s.append(r7);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    Record r {1700, 47.0};

    std::ofstream outFile(filename, std::ios::binary | std::ios::app);
    if (!outFile.is_open()) {
        throw std::runtime_error("Failed to open file for writing");
    }

    outFile.write(reinterpret_cast<const char*>(&r.timestamp), sizeof(r.timestamp));

    outFile.close();

    Storage s2(filename);

    std::vector<Record> records = s2.readAll();
    ASSERT_EQ(records.size(), 7);
    ASSERT_EQ(s2.getRecordCount(), 7);
    ASSERT_EQ(records[6].timestamp, 1600);
}

TEST(StorageTest, MultiThreadingAppend) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    const int producerCount = 4;
    const int recordsPerProducer = 100;

    std::vector<std::thread> producers;

    for (int p = 0; p < producerCount; p++) {
        producers.emplace_back([&, p]() {
            for (int i = 0; i < recordsPerProducer; ++i) {
                Record r;
                r.timestamp = p * 1'000'000 + i;
                r.value = static_cast<double>(i);

                EXPECT_TRUE(s.append(r));
            }
        });
    }
    for (auto& t : producers) {
        t.join();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::vector<Record> records = s.readAll();

    ASSERT_EQ(records.size(),
              producerCount * recordsPerProducer);

    for (int i=1; i<records.size(); i++) {
        EXPECT_LT(records[i-1].timestamp, records[i].timestamp);
    }
}

TEST(StorageTest, MultiThreadingAppendMonotonicEnforcement) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    const int producerCount = 4;
    const int recordsPerProducer = 100;

    std::vector<std::thread> producers;

    for (int p = 0; p < producerCount; p++) {
        producers.emplace_back([&, p]() {
            for (int i = 0; i < recordsPerProducer; ++i) {
                Record r;
                r.timestamp = p * 1'000'000 + i;
                r.value = static_cast<double>(i);

                EXPECT_TRUE(s.append(r));
            }
        });
    }
    for (auto& t : producers) {
        t.join();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    for (int i=0; i<recordsPerProducer; i++) {
        Record r;
        r.timestamp = i + 500'000;
        r.value = -1.0;
        s.append(r);
        EXPECT_FALSE(s.append(r));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::vector<Record> records = s.readAll();

    ASSERT_EQ(records.size(),
              producerCount * recordsPerProducer);

    for (int i=1; i<records.size(); i++) {
        EXPECT_LT(records[i-1].timestamp, records[i].timestamp);
    }
}

TEST(StorageTest, ReadFromTimeStamp) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    EXPECT_TRUE(s.append(r1));

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::optional<Record> actual = s.readFromTime(1000);

    ASSERT_TRUE(actual.has_value());
    EXPECT_EQ(actual->timestamp, 1000);
}

TEST(StorageTest, ReadFromTimeStampNoResult) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    EXPECT_TRUE(s.append(r1));

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::optional<Record> actual = s.readFromTime(1100);

    ASSERT_FALSE(actual.has_value());
}