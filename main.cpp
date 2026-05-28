#include <replxx.hxx>
#include <iostream>
#include <sstream>
#include <vector>
#include <string>

#include "src/TSDBCLI.hpp"

std::vector<std::string> split(std::string const& line) {
    std::istringstream iss(line);
    std::vector<std::string> tokens;
    std::string token;
    while (iss >> token) {
        tokens.push_back(token);
    }
    return tokens;
}

void printHelp()
{
    std::cout << "TSDB Command Line Interface\n";
    std::cout << "===========================\n\n";

    std::cout << "General Commands:\n";
    std::cout << "  help                          Show this help message\n";
    std::cout << "  performance                   Enter performance metric mode\n";
    std::cout << "  exit | quit                   Exit the CLI\n\n";

    std::cout << "Database Commands:\n";
    std::cout << "  create <database>             Create a new database\n";
    std::cout << "  use <database>                Switch to an existing database\n\n";

    std::cout << "Data Ingestion:\n";
    std::cout << "  append <timestamp> <value>    Append a new data point\n\n";

    std::cout << "Read Queries:\n";
    std::cout << "  readall                       Read and display all records\n";
    std::cout << "  readfrom <timestamp>          Read records from a timestamp\n";
    std::cout << "  readrange <start> <end>       Read records in a time range\n\n";

    std::cout << "Aggregate Functions:\n";
    std::cout << "  count <start> <end>           Count records\n";
    std::cout << "  first <start> <end>           First values\n";
    std::cout << "  last <start> <end>            Last value\n";
    std::cout << "  sum <start> <end>             Sum of values\n";
    std::cout << "  min <start> <end>             Minimum value\n";
    std::cout << "  max <start> <end>             Maximum value\n";
    std::cout << "  avg <start> <end>             Average value\n";
    std::cout << "  median <start> <end>          Median value\n";
    std::cout << "  percentile <p> <start> <end>  Pth percentile (0–100)\n";
    std::cout << "  stddev <start> <end>          Standard deviation\n";
    std::cout << "  variance <start> <end>        Variance\n\n";

    std::cout << "  Note: if <start> <end> are omitted, the full time series is used\n\n";
}

int main() {
    replxx::Replxx rx;
    TSDBCLI cli;

    rx.history_load("history.txt");

    while (true) {
        char const* input = rx.input("TSDB> ");

        if (!input) {
            std::cout << "\nExiting...\n";
            break;
        }

        std::string line(input);

        if (line.empty()) {
            continue;
        }

        rx.history_add(line);

        auto tokens = split(line);
        std::string const& command = tokens[0];

        if (command == "help") {
            printHelp();
        }
        else if (command == "performance")
        {
            cli.performance();
        }
        else if (command == "create")
        {
            if (tokens.size() != 2)
            {
                std::cout << "Usage: create <name>\n";
                continue;
            }
            cli.create(tokens[1]);
        }
        else if (command == "use")
        {
            if (tokens.size() != 2)
            {
                std::cout << "Usage: use <name>\n";
                continue;
            }
            cli.use(tokens[1]);
        }
        else if (command == "append")
        {
            if (tokens.size() != 3)
            {
                std::cout << "Usage: append <timestamp> <value>\n";
                continue;
            }
            try
            {
                int64_t timestamp = std::stoll(tokens[1]);
                double value = std::stod(tokens[2]);
                cli.append(timestamp, value);
            }catch (...)
            {
                std::cout << "Error: timestamp must be positive integer, value must be a valid decimal\n";
            }
        }
        else if (command == "readall")
        {
            cli.readall();
        }
        else if (command == "readfrom")
        {
            if (tokens.size() != 2)
            {
                std::cout << "Usage: readfrom <timestamp>\n";
                continue;
            }
            try
            {
                int64_t timestamp = std::stoll(tokens[1]);
                cli.readfrom(timestamp);
            }catch (...)
            {
                std::cout << "Error: timestamp argument must be a positive integer\n";
            }
        }
        else if (command == "readrange")
        {
            if (tokens.size() != 3)
            {
                std::cout << "Usage: readfrom <start> <end>\n";
                continue;
            }
            try
            {
                int64_t start = std::stoll(tokens[1]);
                int64_t end = std::stoll(tokens[2]);
                cli.readrange(start, end);
            }catch (...)
            {
                std::cout << "Error: timestamp argument must be a positive integer\n";
            }
        }
        else if (command == "count")
        {
            if (tokens.size() == 1)
            {
                cli.count();
            }
            else if (tokens.size() == 3)
            {
                try
                {
                    int64_t start = std::stoll(tokens[1]);
                    int64_t end = std::stoll(tokens[2]);
                    cli.count(start, end);
                }catch (...)
                {
                    std::cout << "Error: timestamp argument must be a positive integer\n";
                }
            }
            else
            {
                std::cout << "Usage: count <start> <end>\n";
            }
        }
        else if (command == "first")
        {
            if (tokens.size() == 1)
            {
                cli.first();
            }
            else if (tokens.size() == 3)
            {
                try
                {
                    int64_t start = std::stoll(tokens[1]);
                    int64_t end = std::stoll(tokens[2]);
                    cli.first(start, end);
                }catch (...)
                {
                    std::cout << "Error: timestamp argument must be a positive integer\n";
                }
            }
            else
            {
                std::cout << "Usage: first <start> <end>\n";
            }
        }
        else if (command == "last")
        {
            if (tokens.size() == 1)
            {
                cli.last();
            }
            else if (tokens.size() == 3)
            {
                try
                {
                    int64_t start = std::stoll(tokens[1]);
                    int64_t end = std::stoll(tokens[2]);
                    cli.last(start, end);
                }catch (...)
                {
                    std::cout << "Error: timestamp argument must be a positive integer\n";
                }
            }
            else
            {
                std::cout << "Usage: last <start> <end>\n";
            }
        }
        else if (command == "sum")
        {
            if (tokens.size() == 1)
            {
                cli.sum();
            }
            else if (tokens.size() == 3)
            {
                try
                {
                    int64_t start = std::stoll(tokens[1]);
                    int64_t end = std::stoll(tokens[2]);
                    cli.sum(start, end);
                }catch (...)
                {
                    std::cout << "Error: timestamp argument must be a positive integer\n";
                }
            }
            else
            {
                std::cout << "Usage: sum <start> <end>\n";
            }
        }
        else if (command == "min")
        {
            if (tokens.size() == 1)
            {
                cli.min();
            }
            else if (tokens.size() == 3)
            {
                try
                {
                    int64_t start = std::stoll(tokens[1]);
                    int64_t end = std::stoll(tokens[2]);
                    cli.min(start, end);
                }catch (...)
                {
                    std::cout << "Error: timestamp argument must be a positive integer\n";
                }
            }
            else
            {
                std::cout << "Usage: min <start> <end>\n";
            }
        }
        else if (command == "max")
        {
            if (tokens.size() == 1)
            {
                cli.max();
            }
            else if (tokens.size() == 3)
            {
                try
                {
                    int64_t start = std::stoll(tokens[1]);
                    int64_t end = std::stoll(tokens[2]);
                    cli.max(start, end);
                }catch (...)
                {
                    std::cout << "Error: timestamp argument must be a positive integer\n";
                }
            }
            else
            {
                std::cout << "Usage: max <start> <end>\n";
            }
        }
        else if (command == "avg")
        {
            if (tokens.size() == 1)
            {
                cli.avg();
            }
            else if (tokens.size() == 3)
            {
                try
                {
                    int64_t start = std::stoll(tokens[1]);
                    int64_t end = std::stoll(tokens[2]);
                    cli.avg(start, end);
                }catch (...)
                {
                    std::cout << "Error: timestamp argument must be a positive integer\n";
                }
            }
            else
            {
                std::cout << "Usage: avg <start> <end>\n";
            }
        }
        else if (command == "median")
        {
            if (tokens.size() == 1)
            {
                cli.avg();
            }
            else if (tokens.size() == 3)
            {
                try
                {
                    int64_t start = std::stoll(tokens[1]);
                    int64_t end = std::stoll(tokens[2]);
                    cli.median(start, end);
                }catch (...)
                {
                    std::cout << "Error: timestamp argument must be a positive integer\n";
                }
            }
            else
            {
                std::cout << "Usage: median <start> <end>\n";
            }
        }
        else if (command == "percentile")
        {
            if (tokens.size() == 1)
            {
                cli.percentile();
            }
            else if (tokens.size() == 4)
            {
                try
                {
                    int64_t start = std::stoll(tokens[1]);
                    int64_t end = std::stoll(tokens[2]);
                    float p = std::stof(tokens[3]);
                    cli.percentile(start, end, p);
                }catch (...)
                {
                    std::cout << "Error: timestamp argument must be a positive integer\n";
                }
            }
            else
            {
                std::cout << "Usage: percentile <start> <end> <p>\n";
            }
        }
        else if (command == "stddev")
        {
            if (tokens.size() == 1)
            {
                cli.avg();
            }
            else if (tokens.size() == 3)
            {
                try
                {
                    int64_t start = std::stoll(tokens[1]);
                    int64_t end = std::stoll(tokens[2]);
                    cli.median(start, end);
                }catch (...)
                {
                    std::cout << "Error: timestamp argument must be a positive integer\n";
                }
            }
            else
            {
                std::cout << "Usage: median <start> <end>\n";
            }
        }
        else if (command == "exit" || command == "quit") {
            std::cout << "Goodbye!\n";
            break;
        }
        else {
            std::cout << "Command not recognised: " << command << "\n";
        }
    }

    rx.history_save("history.txt");

    return 0;
}
