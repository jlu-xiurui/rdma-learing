
#include "logger.h"

namespace RDMA_ECHO {

void Log(FileLogger* logger, const char* format, ...) {
    if (logger != nullptr) {
        std::va_list ap;
        va_start(ap, format);
        logger->LogV(format, ap);
        va_end(ap);
    }
}

}

