#include "TSDBCLI.hpp"
#include <iostream>
#include <sstream>

TSDBCLI::TSDBCLI(Storage& storage): storage(storage){}

void TSDBCLI::run()
{
    printHelp();
    std::string command;
    while (true) {
        std::cout << "tsdb> ";
        std::getline(std::cin, command);
        if (command == "exit" || command == "quit") {
            break;
        }
        handleCommand(command);
    }
}

void TSDBCLI::printHelp() const
{
    std::cout << "TSDB Command Line Interface\n";
    std::cout << "Commands:\n";
    std::cout << "  help                       - Show this help message\n";
    std::cout << "  stats                      - Show database statistics\n"; //TODO
    std::cout << "  readall                    - Read and display all records\n";
    std::cout << "  readfrom <timestamp>       - Read record from the specified timestamp\n";
    std::cout << "  readrange <start> <end>    - Read records in the specified time range\n";
    std::cout << "  append <timestamp> <value> - Append a new record\n";
    std::cout << "  exit, quit                 - Exit the CLI\n";
}

void TSDBCLI::handleCommand(const std::string& command)
{
    if (command == "help")
    {
        printHelp();
    }
    else if (command == "readall")
    {
        std::vector<Record> records = storage.readAll();
        for (const Record& r : records)
        {
            std::cout << "Timestamp: " << r.timestamp << ", Value: " << r.value << "\n";
        }
    }
    else if (command.rfind("readfrom ", 0) == 0)
    {
        if (!validateReadFromCommand(command))
        {
            std::cout << "Invalid readfrom command. Usage: readfrom <timestamp>\n";
            return;
        }
        std::istringstream iss(command);
        std::string ignore;
        int64_t number1;

        iss >> ignore >> number1;

        std::optional<Record> record = storage.readFromTime(number1);

        if (record.has_value())
        {
            std::cout << "Timestamp: " << record.value().timestamp << ", Value: " << record.value().value << "\n";
        }
        else
        {
            std::cout << "No record found\n";
        }
    }
    else if (command.rfind("readrange ", 0) == 0)
    {
        if (!validateReadRangeCommand(command))
        {
            std::cout << "Invalid readrange command. Usage: readrange <start> <end>\n";
            return;
        }
        std::istringstream iss(command);
        std::string ignore;
        int64_t number1, number2;

        iss >> ignore >> number1 >> number2;

        if (number1 > number2)
        {
            std::cout << "Invalid time range: start time is greater than end time.\n";
            return;
        }

        std::vector<Record> records = storage.readRange(number1, number2);
        if (records.empty())
        {
            std::cout << "No record found\n";
        }
        else
        {
            for (const Record& r : records)
            {
                std::cout << "Timestamp: " << r.timestamp << ", Value: " << r.value << "\n";
            }
        }
    }
    else if (command.rfind("append ", 0) == 0)
    {
        if (!validateAppendCommand(command))
        {
            std::cout << "Invalid append command. Usage: append <timestamp> <value>\n";
            return;
        }
        std::istringstream iss(command);
        std::string ignore;
        int64_t timestamp;;
        double value;

        iss >> ignore >> timestamp >> value;

        bool success = storage.append(Record{timestamp, value});

        if (success) std::cout << "Record accepted, pending persistence\n";
        else std::cout << "Failed to accept record.\n";
    }
    else
    {
        std::cout << "Unknown command: " << command << "\n";
    }
}

bool TSDBCLI::validateReadRangeCommand(const std::string& command)
{
    const std::string prefix = "readrange ";
    if (command.rfind(prefix, 0) != 0) {
        return false;
    }
    std::string remainder = command.substr(prefix.size());
    if (remainder.empty()) {
        return false;
    }

    std::istringstream iss(remainder);

    int64_t number1, number2;
    std::string extra;
    if (!(iss >> number1 >> number2)) {
        return false;
    }
    if (iss >> extra) {
        return false;
    }
    return true;
}

bool TSDBCLI::validateReadFromCommand(const std::string& command)
{
    const std::string prefix = "readfrom ";
    if (command.rfind(prefix, 0) != 0) {
        return false;
    }
    std::string remainder = command.substr(prefix.size());
    if (remainder.empty()) {
        return false;
    }

    std::istringstream iss(remainder);

    int64_t number1;
    std::string extra;
    if (!(iss >> number1)) {
        return false;
    }
    if (iss >> extra) {
        return false;
    }
    return true;
}

bool TSDBCLI::validateAppendCommand(const std::string& command)
{
    const std::string prefix = "append ";
    if (command.rfind(prefix, 0) != 0) {
        return false;
    }
    std::string remainder = command.substr(prefix.size());
    if (remainder.empty()) {
        return false;
    }

    std::istringstream iss(remainder);

    int64_t timestamp;
    double value;

    std::string extra;
    if (!(iss >> timestamp >> value)) {
        return false;
    }
    if (iss >> extra) {
        return false;
    }
    return true;
}