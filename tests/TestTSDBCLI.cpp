#include <gtest/gtest.h>
#include "../src/Storage.hpp"
#include "../src/TSDBCLI.hpp"

TEST(StorageTest, TestValidReadRangeCommand) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    s.append(r1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateReadRangeCommand("readrange 1000 2000"));

    testing::internal::CaptureStdout();

    cli.handleCommand("readrange 1000 2000");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Timestamp: 1000, Value: 42\n");
}

TEST(StorageTest, TestInvalidReadRangeCommand) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);
    Record r {1000, 42.0};
    s.append(r);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_FALSE(cli.validateReadRangeCommand("readrange 1000"));
    EXPECT_FALSE(cli.validateReadRangeCommand("readrange abc def"));
    EXPECT_FALSE(cli.validateReadRangeCommand("readrange 1000 2000 extra"));

    testing::internal::CaptureStdout();

    cli.handleCommand("readrange 1000");
    cli.handleCommand("readrange abc def");
    cli.handleCommand("readrange 1000 2000 extra");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output,
        "Invalid readrange command. Usage: readrange <start> <end>\n"
        "Invalid readrange command. Usage: readrange <start> <end>\n"
        "Invalid readrange command. Usage: readrange <start> <end>\n"
        );
}

TEST(StorageTest, TestReadRangeEmptyDB) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateReadRangeCommand("readrange 1000 2000"));

    testing::internal::CaptureStdout();
    cli.handleCommand("readrange 1000 2000");
    std::string output = testing::internal::GetCapturedStdout();

    EXPECT_EQ(output, "No record found\n");
}

TEST(StorageTest, TestReadRangeMultipleRecords) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};
    Record r2 {1500, 43.5};
    Record r3 {2000, 44.25};

    s.append(r1);
    s.append(r2);
    s.append(r3);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateReadRangeCommand("readrange 1000 1600"));

    testing::internal::CaptureStdout();
    cli.handleCommand("readrange 1000 1600");
    std::string output = testing::internal::GetCapturedStdout();

    EXPECT_EQ(output,
        "Timestamp: 1000, Value: 42\n"
        "Timestamp: 1500, Value: 43.5\n"
        );
}

TEST(StorageTest, TestValidReadFromCommand) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    s.append(r1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateReadFromCommand("readfrom 1000"));

    testing::internal::CaptureStdout();

    cli.handleCommand("readfrom 1000");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Timestamp: 1000, Value: 42\n");
}

TEST(StorageTest, TestInvalidReadFromCommand) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);
    Record r {1000, 42.0};
    s.append(r);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_FALSE(cli.validateReadFromCommand("readfrom abc"));
    EXPECT_FALSE(cli.validateReadFromCommand("readfrom 1000 extra"));

    testing::internal::CaptureStdout();

    cli.handleCommand("readfrom abc");
    cli.handleCommand("readfrom 1000 extra");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output,
        "Invalid readfrom command. Usage: readfrom <timestamp>\n"
        "Invalid readfrom command. Usage: readfrom <timestamp>\n"
        );
}

TEST(StorageTest, TestValidAppendCommand) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateAppendCommand("append 1000 42.0"));

    testing::internal::CaptureStdout();

    cli.handleCommand("append 1000 42.0");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Record accepted, pending persistence\n");
}

TEST(StorageTest, TestInvalidAppendCommand) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    TSDBCLI cli;

    EXPECT_FALSE(cli.validateAppendCommand("append 1000"));
    EXPECT_FALSE(cli.validateAppendCommand("append abc def"));
    EXPECT_FALSE(cli.validateAppendCommand("append 1000 42.0 extra"));

    testing::internal::CaptureStdout();

    cli.handleCommand("append 1000");
    cli.handleCommand("append abc def");
    cli.handleCommand("append 1000 42.0 extra");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output,
        "Invalid append command. Usage: append <timestamp> <value>\n"
        "Invalid append command. Usage: append <timestamp> <value>\n"
        "Invalid append command. Usage: append <timestamp> <value>\n"
        );
}

TEST(StorageTest, TestMonotonicAppendCommand) {
    const char* filename = "testdb.txt";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    s.append(r1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateAppendCommand("append 900 42.5"));
    EXPECT_TRUE(cli.validateAppendCommand("append 1000 43.0"));

    testing::internal::CaptureStdout();

    cli.handleCommand("append 900 42.5");
    cli.handleCommand("append 1000 43.0");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output,
        "Failed to accept record.\n"
        "Failed to accept record.\n"
        );
}