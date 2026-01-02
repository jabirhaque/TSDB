#include "src/Storage.hpp"
#include <iostream>

int main() {
    Storage s("records.txt");

    const int producerCount = 4;
    const int recordsPerProducer = 1000;

    std::vector<std::thread> producers;

    for (int p = 0; p < producerCount; p++) {
        producers.emplace_back([&, p]() {
            for (int i = 0; i < recordsPerProducer; ++i) {
                Record r;
                r.timestamp = p * 1'000'000 + i;
                r.value = static_cast<double>(i);
                s.append(r);
            }
        });
    }
    for (auto& t : producers) {
        t.join();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::vector<Record> records = s.readAll();

    std::cout << "Total records: " << records.size() << std::endl;

    return 0;
}