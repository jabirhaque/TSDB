#include "TSDBCLI.hpp"
#include <iostream>

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
    std::cout << "  help                    - Show this help message\n";
    std::cout << "  stats                   - Show database statistics\n";
    std::cout << "  readall                 - Read and display all records\n";
    std::cout << "  readrange <start> <end> - Read records in the specified time range\n";
    std::cout << "  readfrom <timestamp>    - Read record from the specified timestamp\n";
    std::cout << "  exit, quit              - Exit the CLI\n";
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
    else
    {
        std::cout << "Unknown command: " << command << "\n";
    }
}