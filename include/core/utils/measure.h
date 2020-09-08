#pragma once

#include <iostream>
#include <chrono>

template<typename ...Ts, typename U>
void measure(const char* /*message*/, U (*function), Ts... args)
{
    auto start = std::chrono::system_clock::now();
    //TODO: delete objects on heap returned by function, might need smart pointers to solve this once and for all
    function(args...);
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> dur = end - start;

    std::cout << dur.count() << ",";
}

template<typename ...Ts, typename U>
void measureEnd(const char* /*message*/, U (*function), Ts... args)
{
    auto start = std::chrono::system_clock::now();
    //TODO: delete objects on heap returned by function, might need smart pointers to solve this once and for all
    function(args...);
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> dur = end - start;

    std::cout << dur.count();
}

