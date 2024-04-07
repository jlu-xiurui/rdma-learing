#ifndef LOGGER_H
#define LOGGER_H

#include <cstdarg>
#include <cstdio>
#include <thread>
#include <chrono>
#include <ctime>
#include <sstream>
#include <cassert>
#include <iostream>

namespace RDMA_ECHO {

class Logger {
  public:
    explicit Logger() = default;
    Logger(const Logger &) = delete;
    Logger &operator=(const Logger &) = delete;
    virtual ~Logger() = default;

    virtual void LogV(const char* format, std::va_list ap) = 0;
};

class FileLogger : public Logger {
  public:
    explicit FileLogger(std::FILE* f, bool show_terminal) 
        : f_(f), show_terminal_(show_terminal) {}
    ~FileLogger() override {
        std::fclose(f_);
    }
    void LogV(const char* format, std::va_list ap) override {
        constexpr const int BufferSize = 512;
        char buffer[BufferSize];

        auto now = std::chrono::system_clock::now();
        std::time_t now_time = std::chrono::system_clock::to_time_t(now);
        struct tm *local_time = ::localtime(&now_time);

        std::ostringstream thread_stream;
        thread_stream << std::this_thread::get_id();
        std::string thread_id = thread_stream.str();
        int buffer_idx = std::snprintf(buffer, BufferSize,
            "thread[%s]: %04d-%02d-%02d %02d:%02d:%02d ",
            thread_id.c_str(),
            local_time->tm_year + 1900,
            local_time->tm_mon + 1,
            local_time->tm_mday,
            local_time->tm_hour,
            local_time->tm_min,
            local_time->tm_sec);

        std::va_list arguments_copy;
        va_copy(arguments_copy, ap);
        buffer_idx +=
            std::vsnprintf(buffer + buffer_idx, BufferSize - buffer_idx,
                            format, arguments_copy);
        va_end(arguments_copy);

        if (buffer_idx >= BufferSize - 1) {
            char* new_buffer = new char[buffer_idx + 2];
            buffer_idx = std::snprintf(buffer, BufferSize,
                "thread[%s]: %04d-%02d-%02d %02d:%02d:%02d ",
                thread_id.c_str(),
                local_time->tm_year + 1900,
                local_time->tm_mon + 1,
                local_time->tm_mday,
                local_time->tm_hour,
                local_time->tm_min,
                local_time->tm_sec);
            std::va_list arguments_copy;
            va_copy(arguments_copy, ap);
            buffer_idx +=
                std::vsnprintf(new_buffer + buffer_idx, BufferSize - buffer_idx,
                                format, arguments_copy);
            va_end(arguments_copy);
            WriteLog(buffer, buffer_idx);
            delete[] new_buffer;
            return;
        }
        WriteLog(buffer, buffer_idx);
    }
  private:
    void WriteLog(char* buffer, int size) {
        if (buffer[size] != '\n') {
            buffer[size] = '\n';
            ++size;
        }
        std::fwrite(buffer, 1, size, f_);
        std::fflush(f_);
        if (show_terminal_) {
            std::fwrite(buffer, 1, size, stdout);
        }
    }
    std::FILE* f_;
    bool show_terminal_;
};

void Log(FileLogger* logger, const char* format, ...);

}



#endif