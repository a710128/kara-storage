#pragma once
#include <exception>
#include <string>

class KaraStorageException {
public:
    std::string msg;
    KaraStorageException(const char *msg) : msg(msg) {}
};
