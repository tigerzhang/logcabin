//
// Created by Zhang Hu on 17-3-13.
//

#include <thread>
#include <vector>
#include <csignal>
#include <iostream>
#include <zconf.h>

#include "Client.pb.h"

bool stop = false;
std::vector<std::thread> threads;

void thread_main(void *data) {
    while (!stop) {
        LogCabin::Protocol::Client::StateMachineQuery::Request request;
        LogCabin::Protocol::Client::StateMachineQuery::Response response;

        if (request.has_key_value()) {
            response.mutable_key_value()->set_error(request.key_value().key());
        }

        usleep(100);
        if (stop) {
            std::cout << (void *)&request.default_instance() << std::endl
                    << (void *)&response.default_instance() << std::endl;
        }
    }
}

void
termination_handler (int signum)
{
    stop = true;
}

int main() {
    if (signal (SIGINT, termination_handler) == SIG_IGN)
        signal (SIGINT, SIG_IGN);

    for (int i=0; i<50; i++) {
        std::cout << "start thread " << i << std::endl;
        threads.emplace_back(thread_main, nullptr);
    }

    for (auto it = threads.begin();
            it != threads.end();
            it++) {
        std::cout << "wait thread " << std::endl;
        it->join();
    }
    return 0;
}