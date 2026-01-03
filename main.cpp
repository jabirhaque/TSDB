#include "src/Storage.hpp"
#include "src/TSDBCLI.hpp"
#include <iostream>

int main() {
    Storage s("records.txt");
    s.append(Record{1000, 42.0});
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    TSDBCLI cli(s);
    cli.run();
}