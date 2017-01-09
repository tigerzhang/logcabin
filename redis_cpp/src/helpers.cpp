#include "../include/helpers.hpp"

std::string concat(char /*delimiter*/, std::string str)
{
    return str;
}

void push_all(std::vector<std::string>& accumulator, std::string value)
{
    accumulator.push_back(value);
}
