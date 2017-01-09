#ifndef REDISCPP_HELPERS_HPP
#define REDISCPP_HELPERS_HPP

#include <string>
#include <memory>
#include <vector>
/*
    Provides some functions used in the RedisWrapper but which make not sense
    putting in the .hpp because they are not directly related to redis.
*/

// we need to concatenate in a string with spaces.
std::string concat(char /*delimiter*/, std::string str);

template<typename ... Args>
std::string concat(char delimiter, std::string first, Args ... rest)
{
    return first + " " + concat(delimiter, rest...);
}

/*
    No make unique in c++11 yet
    http://stackoverflow.com/questions/17902405/how-to-implement-make-unique-function-in-c11
*/
template<typename T, typename ...Args>
std::unique_ptr<T> make_unique( Args&& ...args )
{
    return std::unique_ptr<T>( new T( std::forward<Args>(args)... ) );
}


/*
    Put all the values in a vector.
*/
void push_all(std::vector<std::string>& accumulator, std::string value);

template<typename ... Args>
void push_all(std::vector<std::string>& accumulator, std::string first, Args ... rest)
{
    accumulator.push_back(first);
    push_all(accumulator, rest ...);
}
#endif
