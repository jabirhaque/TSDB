#include <gtest/gtest.h>
#include <filesystem>
#include "../src/Storage.hpp"
#include "../src/TSDBCLI.hpp"

TEST(StorageTest, ValidCreateCommand) {
    TSDBCLI cli;

    EXPECT_TRUE(cli.validateCreateCommand("create testdb"));

    cli.handleCommand("create testdb");

    EXPECT_TRUE(std::filesystem::exists("testdb.tsdb"));
}

TEST(StorageTest, InvalidCreateCommand) {
    TSDBCLI cli;

    EXPECT_FALSE(cli.validateCreateCommand("create testdb.23 __"));

    testing::internal::CaptureStdout();

    cli.handleCommand("create testdb*23 __");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Invalid create command. Usage: create <database> where <database> contains letters and numbers only\n");
}

TEST(StorageTest, CreateCommandExistingDB) {
    TSDBCLI cli;

    cli.handleCommand("create testdb");

    testing::internal::CaptureStdout();

    cli.handleCommand("create testdb");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Database already exists\n");
}

TEST(StorageTest, CreateCommandReservedDBName) {
    TSDBCLI cli;

    EXPECT_TRUE(cli.validateCreateCommand("create performance"));

    testing::internal::CaptureStdout();

    cli.handleCommand("create performance");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "The database name 'performance' is reserved for performance metric mode. Please choose a different name.\n");
}

TEST(StorageTest, ValidUseCommand) {
    TSDBCLI cli;

    EXPECT_TRUE(cli.validateUseCommand("use testdb"));

    cli.handleCommand("create testdb");

    testing::internal::CaptureStdout();

    cli.handleCommand("use testdb");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "");
}

TEST(StorageTest, InvalidUseCommand) {
    TSDBCLI cli;

    EXPECT_FALSE(cli.validateUseCommand("use testdb.23 __"));

    testing::internal::CaptureStdout();

    cli.handleCommand("use testdb.23 __");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Invalid use command. Usage: use <database> where <database> contains letters and numbers only\n");
}

TEST(StorageTest, UseCommandNonExistingDB) {
    TSDBCLI cli;

    cli.handleCommand("use testdb2");

    testing::internal::CaptureStdout();

    cli.handleCommand("use testdb2");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Database not recognised\n");
}

TEST(StorageTest, TestValidReadRangeCommand) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    s.append(r1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateGeneralRangeCommand("readrange ","readrange 1000 2000"));

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();

    cli.handleCommand("readrange 1000 2000");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Timestamp: 1000, Value: 42\n");
}

TEST(StorageTest, TestInvalidReadRangeCommand) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);
    Record r {1000, 42.0};
    s.append(r);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_FALSE(cli.validateGeneralRangeCommand("readrange ","readrange 1000"));
    EXPECT_FALSE(cli.validateGeneralRangeCommand("readrange ","readrange abc def"));
    EXPECT_FALSE(cli.validateGeneralRangeCommand("readrange ","readrange 1000 2000 extra"));

    cli.handleCommand("use testdb");

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
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateGeneralRangeCommand("readrange ","readrange 1000 2000"));

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();
    cli.handleCommand("readrange 1000 2000");
    std::string output = testing::internal::GetCapturedStdout();

    EXPECT_EQ(output, "No record found\n");
}

TEST(StorageTest, TestReadRangeMultipleRecords) {
    const char* filename = "testdb.tsdb";
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

    EXPECT_TRUE(cli.validateGeneralRangeCommand("readrange ","readrange 1000 1600"));

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();
    cli.handleCommand("readrange 1000 1600");
    std::string output = testing::internal::GetCapturedStdout();

    EXPECT_EQ(output,
        "Timestamp: 1000, Value: 42\n"
        "Timestamp: 1500, Value: 43.5\n"
        );
}

TEST(StorageTest, TestValidReadFromCommand) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    s.append(r1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateReadFromCommand("readfrom 1000"));

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();

    cli.handleCommand("readfrom 1000");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Timestamp: 1000, Value: 42\n");
}

TEST(StorageTest, TestInvalidReadFromCommand) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);
    Record r {1000, 42.0};
    s.append(r);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_FALSE(cli.validateReadFromCommand("readfrom abc"));
    EXPECT_FALSE(cli.validateReadFromCommand("readfrom 1000 extra"));

    cli.handleCommand("use testdb");

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
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateAppendCommand("append 1000 42.0"));

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();

    cli.handleCommand("append 1000 42.0");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Record accepted, pending persistence\n");
}

TEST(StorageTest, TestInvalidAppendCommand) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    TSDBCLI cli;

    EXPECT_FALSE(cli.validateAppendCommand("append 1000"));
    EXPECT_FALSE(cli.validateAppendCommand("append abc def"));
    EXPECT_FALSE(cli.validateAppendCommand("append 1000 42.0 extra"));

    cli.handleCommand("use testdb");

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
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    s.append(r1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateAppendCommand("append 900 42.5"));
    EXPECT_TRUE(cli.validateAppendCommand("append 1000 43.0"));

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();

    cli.handleCommand("append 900 42.5");
    cli.handleCommand("append 1000 43.0");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output,
        "Failed to accept record.\n"
        "Failed to accept record.\n"
        );
}

TEST(StorageTest, TestValidCountCommand) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    s.append(r1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();

    cli.handleCommand("count");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Total records: 1\n");
}

TEST(StorageTest, TestValidCountCommandNoDatabaseSelected) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    s.append(r1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    testing::internal::CaptureStdout();

    cli.handleCommand("count");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "No database selected. Use the 'use <database>' command to select a database.\n");
}

TEST(StorageTest, TestValidCountCommandMultipleRecords) {
    const char* filename = "testdb.tsdb";
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

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();

    cli.handleCommand("count");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Total records: 3\n");
}

TEST(StorageTest, TestValidCountRangeCommand) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    s.append(r1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateGeneralRangeCommand("count ","count 1000 2000"));

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();

    cli.handleCommand("count 1000 2000");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Total records: 1\n");
}

TEST(StorageTest, TestValidCountRangeCommandNoDatabaseSelected) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    s.append(r1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateGeneralRangeCommand("count ","count 1000 2000"));

    testing::internal::CaptureStdout();

    cli.handleCommand("count 1000 2000");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "No database selected. Use the 'use <database>' command to select a database.\n");
}

TEST(StorageTest, TestInvalidCountRangeCommand) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);
    Record r {1000, 42.0};
    s.append(r);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_FALSE(cli.validateGeneralRangeCommand("count ","count 1000"));
    EXPECT_FALSE(cli.validateGeneralRangeCommand("count ","count abc def"));
    EXPECT_FALSE(cli.validateGeneralRangeCommand("count ","count 1000 2000 extra"));

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();

    cli.handleCommand("count 1000");
    cli.handleCommand("count abc def");
    cli.handleCommand("count 1000 2000 extra");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output,
        "Invalid count command. Usage: count <start> <end>\n"
        "Invalid count command. Usage: count <start> <end>\n"
        "Invalid count command. Usage: count <start> <end>\n"
        );
}

TEST(StorageTest, TestCountRangeEmptyDB) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateGeneralRangeCommand("count ","count 1000 2000"));

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();
    cli.handleCommand("count 1000 2000");
    std::string output = testing::internal::GetCapturedStdout();

    EXPECT_EQ(output, "Total records: 0\n");
}

TEST(StorageTest, TestCountRangeMultipleRecords) {
    const char* filename = "testdb.tsdb";
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

    EXPECT_TRUE(cli.validateGeneralRangeCommand("count ","count 1000 1600"));

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();
    cli.handleCommand("count 1000 1600");
    std::string output = testing::internal::GetCapturedStdout();

    EXPECT_EQ(output, "Total records: 2\n");
}

TEST(StorageTest, TestValidFirstCommand) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    s.append(r1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();

    cli.handleCommand("first");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Timestamp: 1000, Value: 42\n");
}

TEST(StorageTest, TestValidFirstCommandNoDatabaseSelected) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    s.append(r1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    testing::internal::CaptureStdout();

    cli.handleCommand("first");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "No database selected. Use the 'use <database>' command to select a database.\n");
}

TEST(StorageTest, TestValidFirstRangeCommand) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    s.append(r1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateGeneralRangeCommand("first ","first 1000 2000"));

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();

    cli.handleCommand("first 1000 2000");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Timestamp: 1000, Value: 42\n");
}

TEST(StorageTest, TestValidFirstRangeCommandNoDatabaseSelected) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    Record r1 {1000, 42.0};

    s.append(r1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateGeneralRangeCommand("first ","first 1000 2000"));

    testing::internal::CaptureStdout();

    cli.handleCommand("first 1000 2000");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "No database selected. Use the 'use <database>' command to select a database.\n");
}

TEST(StorageTest, TestInvalidFirstRangeCommand) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);
    Record r {1000, 42.0};
    s.append(r);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_FALSE(cli.validateGeneralRangeCommand("first ","first 1000"));
    EXPECT_FALSE(cli.validateGeneralRangeCommand("first ","first abc def"));
    EXPECT_FALSE(cli.validateGeneralRangeCommand("first ","first 1000 2000 extra"));

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();

    cli.handleCommand("first 1000");
    cli.handleCommand("first abc def");
    cli.handleCommand("first 1000 2000 extra");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output,
        "Invalid first command. Usage: first <start> <end>\n"
        "Invalid first command. Usage: first <start> <end>\n"
        "Invalid first command. Usage: first <start> <end>\n"
        );
}

TEST(StorageTest, TestFirstRangeEmptyDB) {
    const char* filename = "testdb.tsdb";
    std::remove(filename);

    Storage s(filename);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    TSDBCLI cli;

    EXPECT_TRUE(cli.validateGeneralRangeCommand("first ","first 1000 2000"));

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();
    cli.handleCommand("first 1000 2000");
    std::string output = testing::internal::GetCapturedStdout();

    EXPECT_EQ(output, "No record found\n");
}

TEST(StorageTest, TestFirstRangeMultipleRecords) {
    const char* filename = "testdb.tsdb";
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

    EXPECT_TRUE(cli.validateGeneralRangeCommand("first ","first 1000 1600"));

    cli.handleCommand("use testdb");

    testing::internal::CaptureStdout();
    cli.handleCommand("first 1000 1600");
    std::string output = testing::internal::GetCapturedStdout();

    EXPECT_EQ(output, "Timestamp: 1000, Value: 42\n");
}