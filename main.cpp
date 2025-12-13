#include "Storage.hpp"
#include <iostream>

int main() {
    try {
        Storage store("records.txt");

        store.append({1670861284000, 42.5});
        store.append({1670861285000, 43.7});
        store.append({1670861286000, 44.1});

        auto records = store.readAll();

        std::cout << "Records in file:\n";
        for (const auto& r : records) {
            std::cout << r.timestamp << " -> " << r.value << "\n";
        }

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}