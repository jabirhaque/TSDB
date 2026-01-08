#include "src/Storage.hpp"
#include "src/TSDBCLI.hpp"
#include <iostream>

int main() {
    Storage s("records.txt");
    TSDBCLI cli(s);
    cli.run();
}