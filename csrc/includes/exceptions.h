#pragma once
#include <exception>
#include <cstdarg>
#include <string>
#include <execinfo.h>
#include <sstream>

class KaraStorageException {
public:
    std::string msg;
    KaraStorageException(const char *format, ...) {
        char buffer[1024];
        va_list aptr;

        va_start(aptr, format);
        vsprintf(buffer, format, aptr);
        va_end(aptr);

        void *array[10];
        size_t size;

        // get void*'s for all entries on the stack
        size = backtrace(array, 10);

        std::ostringstream ss;
        char** strings = backtrace_symbols(array, size);

        ss << buffer << std::endl;

        if (strings != NULL) {
            for (uint32_t i = 0; i < size; ++ i) {
                ss << strings[i] << std::endl;
            }
            free(strings); // required by backtrace_symbols
        }

        this->msg = ss.str();
    }
};
