#include <gtest/gtest.h>
#include <filesystem>
#include "../src/TSDBCLI.hpp"

TEST(TestCLI, createReservedPerformanceName)
{
    TSDBCLI cli;

    testing::internal::CaptureStdout();

    cli.create("performance");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "The database name 'performance' is reserved for performance metric mode. Please choose a different name.\n");
}

TEST(TestCLI, createInvalidFileName)
{
    TSDBCLI cli;

    testing::internal::CaptureStdout();

    cli.create("some!non$ense_");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "The database name must consist of A-Z, a-z or 0-9 characters only. Please choose a different name.\n");
}

TEST(TestCLI, validCreateCommand) {
    TSDBCLI cli;

    cli.create("test");

    EXPECT_TRUE(std::filesystem::exists("testdb.tsdb"));
}

TEST(TestCLI, createExistingName)
{
    TSDBCLI cli;

    cli.create("test");

    testing::internal::CaptureStdout();

    cli.create("test");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Database already exists\n");
}

TEST(TestCLI, nonexistantUseCommand) {
    TSDBCLI cli;

    testing::internal::CaptureStdout();

    cli.use("random");

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Database doesn't exist\n");
}

TEST(TestCLI, count) {
    TSDBCLI cli;

    cli.use("test");

    cli.append(1, 1.0);
    cli.append(2, 2.0);
    cli.append(3, 3.0);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    testing::internal::CaptureStdout();

    cli.count();

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Total records: 3\n");
}

TEST(TestCLI, sum) {
    TSDBCLI cli;

    cli.use("test");

    testing::internal::CaptureStdout();

    cli.sum();

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Sum: 6\n");
}

TEST(TestCLI, avg) {
    TSDBCLI cli;

    cli.use("test");

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    testing::internal::CaptureStdout();

    cli.avg();

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Average: 2\n");
}

TEST(TestCLI, percentile) {
    TSDBCLI cli;

    cli.use("test");

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    testing::internal::CaptureStdout();

    cli.percentile(1, 3, 49);
    cli.percentile(1, 3, 50);
    cli.percentile(1, 3, 100);

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Timestamp: 1, Value: 1\n" "Timestamp: 2, Value: 2\n" "Timestamp: 3, Value: 3\n");
}

TEST(TestCLI, stddevAndVariance) {
    TSDBCLI cli;

    cli.use("test");

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    testing::internal::CaptureStdout();

    cli.stddev();
    cli.variance();

    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(output, "Standard Deviation: 0.816496\n" "Variance: 0.666667\n");
}