//
// Created by Wilbert Harriman on 1/4/23.
//

#ifndef MAP_REDUCE_LOGGER_H
#define MAP_REDUCE_LOGGER_H

#include <cstdio>
#include <cstdarg>
#include <ctime>
#include <cstring>
#include <string>
#include <iostream>
#include <chrono>
#include <fstream>

class Logger {
public:
    Logger(const std::string& job_name) : file_name_(job_name + "-log.out") {}

    void log(const char* format, ...) {
        va_list args;
        va_start(args, format);
        log(format, args);
        va_end(args);
    }

    void log(const char* format, va_list args) {
        char buffer[1024];
        vsnprintf(buffer, 1024, format, args);
        std::string message = std::to_string(getTimestamp()) + ","+ std::string(buffer);
        std::ofstream logfile;
        logfile.open(file_name_, std::ios_base::app);
        logfile << message << std::endl;
        logfile.close();
    }

private:
    long getTimestamp() {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    }
    std::string file_name_;
};


#endif //MAP_REDUCE_LOGGER_H
